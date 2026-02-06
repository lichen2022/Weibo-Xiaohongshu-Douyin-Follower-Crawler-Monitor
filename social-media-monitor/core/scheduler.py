import schedule
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from core.database import Database
from core.crawlers.weibo_crawler import WeiboCrawler
from core.crawlers.xiaohongshu_crawler import XiaohongshuCrawler
from core.crawlers.douyin_crawler import DouyinCrawler
from config import ScheduleConfig, WeiboConfig, XiaohongshuConfig, DouyinConfig
from utils.logger import Logger


class TaskScheduler:
    """定时任务调度器"""

    def __init__(self, db: Database = None):
        """
        初始化调度器

        Args:
            db: 数据库实例
        """
        self.db = db or Database()
        self.logger = Logger.get_logger("scheduler")
        self.is_running = False
        self.scheduler_thread = None
        self.stop_event = threading.Event()

        self.crawlers = {
            'weibo': WeiboCrawler(),
            'xiaohongshu': XiaohongshuCrawler(),
            'douyin': DouyinCrawler()
        }

        self.platform_configs = {
            'weibo': WeiboConfig,
            'xiaohongshu': XiaohongshuConfig,
            'douyin': DouyinConfig
        }

    def start(self):
        """启动调度器"""
        if self.is_running:
            self.logger.warning("调度器已在运行中")
            return

        self.is_running = True
        self.stop_event.clear()

        tasks = self.db.get_all_tasks()
        for task in tasks:
            if task['is_enabled']:
                self._schedule_task(task)

        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()

        self.logger.info("调度器已启动")

    def stop(self):
        """停止调度器"""
        if not self.is_running:
            self.logger.warning("调度器未运行")
            return

        self.is_running = False
        self.stop_event.set()
        schedule.clear()

        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

        self.logger.info("调度器已停止")

    def _run_scheduler(self):
        """运行调度器主循环"""
        while not self.stop_event.is_set():
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"调度器运行异常: {e}", exc_info=True)

    def _schedule_task(self, task: Dict):
        """
        调度单个任务

        Args:
            task: 任务信息字典
        """
        task_name = task['task_name']
        schedule_time = task['schedule_time']

        try:
            schedule.every().day.at(schedule_time).do(
                self._execute_task, task_id=task['id']
            )
            self.logger.info(f"任务 {task_name} 已调度，执行时间: {schedule_time}")
        except Exception as e:
            self.logger.error(f"调度任务 {task_name} 失败: {e}", exc_info=True)

    def _execute_task(self, task_id: int):
        """
        执行任务

        Args:
            task_id: 任务ID
        """
        task = self.db.get_task_by_name(
            self.db.get_all_tasks()[task_id - 1]['task_name']
            if task_id <= len(self.db.get_all_tasks()) else None
        )

        if not task:
            self.logger.error(f"任务ID {task_id} 不存在")
            return

        if not task['is_enabled']:
            self.logger.info(f"任务 {task['task_name']} 已禁用，跳过执行")
            return

        start_time = datetime.now()
        log_id = self.db.insert_task_log(
            task_id=task_id,
            start_time=start_time,
            status="running"
        )

        self.logger.info(f"开始执行任务: {task['task_name']}")

        try:
            platform_code = task['task_name'].replace('_follower_crawler', '')
            crawler = self.crawlers.get(platform_code)
            config = self.platform_configs.get(platform_code)

            if not crawler or not config:
                raise ValueError(f"不支持的平台: {platform_code}")

            platform = self.db.get_platform_by_code(platform_code)
            if not platform:
                raise ValueError(f"平台不存在: {platform_code}")

            records_count = 0
            success_count = 0
            failed_count = 0

            if platform_code == 'weibo':
                uid_list = config.UID_LIST
                for uid in uid_list:
                    records_count += 1
                    try:
                        user_info = crawler.get_user_info(uid)
                        if user_info:
                            user = self.db.get_user_by_platform_and_id(platform['id'], uid)
                            user_identity = user.get('user_identity', '') if user else ''
                            user_db_id = self.db.insert_user(
                                platform_id=platform['id'],
                                user_id=uid,
                                username=user_info.get('screen_name', ''),
                                user_identity=user_identity
                            )
                            self.db.insert_follower_record(
                                user_id=user_db_id,
                                platform_id=platform['id'],
                                user_identity=user_identity,
                                follower_count=user_info.get('follower_count', 0),
                                record_time=start_time
                            )
                            success_count += 1
                            self.logger.info(f"微博用户 {uid} 数据采集成功，粉丝数: {user_info.get('follower_count', 0)}")
                        else:
                            failed_count += 1
                            self.logger.warning(f"微博用户 {uid} 数据采集失败")
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"微博用户 {uid} 采集异常: {e}", exc_info=True)

            elif platform_code == 'xiaohongshu':
                url_list = config.URL_LIST
                for url in url_list:
                    records_count += 1
                    try:
                        user_info = crawler.get_user_info(url)
                        if user_info:
                            user_id_str = user_info.get('user_id', '')
                            user = self.db.get_user_by_platform_and_id(platform['id'], user_id_str)
                            user_identity = user.get('user_identity', '') if user else ''
                            user_db_id = self.db.insert_user(
                                platform_id=platform['id'],
                                user_id=user_id_str,
                                username=user_info.get('nickname', ''),
                                user_identity=user_identity
                            )
                            self.db.insert_follower_record(
                                user_id=user_db_id,
                                platform_id=platform['id'],
                                user_identity=user_identity,
                                follower_count=user_info.get('follower_count', 0),
                                record_time=start_time
                            )
                            success_count += 1
                            self.logger.info(f"小红书博主 {url} 数据采集成功，粉丝数: {user_info.get('follower_count', 0)}")
                        else:
                            failed_count += 1
                            self.logger.warning(f"小红书博主 {url} 数据采集失败")
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"小红书博主 {url} 采集异常: {e}", exc_info=True)

            elif platform_code == 'douyin':
                sec_user_id_list = config.SEC_USER_ID_LIST
                for sec_user_id in sec_user_id_list:
                    records_count += 1
                    try:
                        user_info = crawler.get_user_info(sec_user_id)
                        if user_info:
                            user = self.db.get_user_by_platform_and_id(platform['id'], sec_user_id)
                            user_identity = user.get('user_identity', '') if user else ''
                            user_db_id = self.db.insert_user(
                                platform_id=platform['id'],
                                user_id=sec_user_id,
                                username=user_info.get('nickname', ''),
                                user_identity=user_identity
                            )
                            self.db.insert_follower_record(
                                user_id=user_db_id,
                                platform_id=platform['id'],
                                user_identity=user_identity,
                                follower_count=user_info.get('follower_count', 0),
                                record_time=start_time
                            )
                            success_count += 1
                            self.logger.info(f"抖音博主 {sec_user_id} 数据采集成功，粉丝数: {user_info.get('follower_count', 0)}")
                        else:
                            failed_count += 1
                            self.logger.warning(f"抖音博主 {sec_user_id} 数据采集失败")
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"抖音博主 {sec_user_id} 采集异常: {e}", exc_info=True)

            end_time = datetime.now()
            status = "success" if failed_count == 0 else "partial_success" if success_count > 0 else "failed"

            self.db.update_task_log(
                log_id=log_id,
                end_time=end_time,
                status=status,
                records_count=records_count,
                success_count=success_count,
                failed_count=failed_count
            )

            self.db.update_task_status(
                task_id=task_id,
                status=status,
                last_run_time=end_time,
                retry_count=0
            )

            self.logger.info(
                f"任务 {task['task_name']} 执行完成，"
                f"总记录: {records_count}, 成功: {success_count}, 失败: {failed_count}"
            )

        except Exception as e:
            end_time = datetime.now()
            error_message = str(e)

            self.db.update_task_log(
                log_id=log_id,
                end_time=end_time,
                status="failed",
                error_message=error_message
            )

            retry_count = task['retry_count'] + 1
            if retry_count <= task['max_retry']:
                self.db.update_task_status(
                    task_id=task_id,
                    status="retrying",
                    last_run_time=end_time,
                    retry_count=retry_count
                )
                self.logger.warning(f"任务 {task['task_name']} 执行失败，将在1分钟后重试 (第{retry_count}次)")
                threading.Timer(60, self._execute_task, args=[task_id]).start()
            else:
                self.db.update_task_status(
                    task_id=task_id,
                    status="failed",
                    last_run_time=end_time,
                    retry_count=retry_count
                )
                self.logger.error(f"任务 {task['task_name']} 执行失败，已达最大重试次数", exc_info=True)

    def run_now(self, task_name: str):
        """
        立即执行指定任务

        Args:
            task_name: 任务名称
        """
        task = self.db.get_task_by_name(task_name)
        if not task:
            self.logger.error(f"任务 {task_name} 不存在")
            return

        self.logger.info(f"手动触发任务: {task_name}")
        self._execute_task(task['id'])

    def update_task_schedule(self, task_name: str, schedule_time: str, is_enabled: bool = None):
        """
        更新任务调度时间

        Args:
            task_name: 任务名称
            schedule_time: 调度时间 (HH:MM)
            is_enabled: 是否启用
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            updates = ["schedule_time = ?", "updated_at = ?"]
            params = [schedule_time, datetime.now()]

            if is_enabled is not None:
                updates.append("is_enabled = ?")
                params.append(1 if is_enabled else 0)

            params.append(task_name)
            cursor.execute(
                f"UPDATE schedule_tasks SET {', '.join(updates)} WHERE task_name = ?",
                params
            )

        if self.is_running:
            schedule.clear()
            tasks = self.db.get_all_tasks()
            for task in tasks:
                if task['is_enabled']:
                    self._schedule_task(task)

        self.logger.info(f"任务 {task_name} 调度时间已更新为 {schedule_time}")

    def get_task_status(self) -> Dict:
        """
        获取调度器状态

        Returns:
            状态信息字典
        """
        tasks = self.db.get_all_tasks()
        task_statuses = []

        for task in tasks:
            logs = self.db.get_task_logs(task['id'], limit=1)
            last_log = logs[0] if logs else None

            task_statuses.append({
                'task_name': task['task_name'],
                'is_enabled': task['is_enabled'],
                'schedule_time': task['schedule_time'],
                'status': task['status'],
                'last_run_time': task['last_run_time'],
                'retry_count': task['retry_count'],
                'last_execution_status': last_log['status'] if last_log else None,
                'last_execution_time': last_log['start_time'] if last_log else None
            })

        return {
            'is_running': self.is_running,
            'tasks': task_statuses
        }

    def execute_all_tasks(self):
        """立即执行所有启用的任务"""
        tasks = self.db.get_all_tasks()
        for task in tasks:
            if task['is_enabled']:
                self.run_now(task['task_name'])
