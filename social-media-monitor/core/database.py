import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Optional, Any
from contextlib import contextmanager
from config import DataConfig
from utils.logger import Logger


class Database:
    """数据库管理类 - 使用SQLite"""

    def __init__(self, db_path: str = None):
        """
        初始化数据库

        Args:
            db_path: 数据库文件路径
        """
        if db_path is None:
            os.makedirs(DataConfig.DATA_DIR, exist_ok=True)
            db_path = os.path.join(DataConfig.DATA_DIR, "social_media_monitor.db")

        self.db_path = db_path
        self.logger = Logger.get_logger("database")
        self._init_database()

    @contextmanager
    def get_connection(self):
        """
        获取数据库连接上下文管理器

        Yields:
            数据库连接对象
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.text_factory = str
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"数据库操作失败: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()

    def _init_database(self):
        """初始化数据库表结构"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS platforms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    code TEXT NOT NULL UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform_id INTEGER NOT NULL,
                    user_id TEXT NOT NULL,
                    username TEXT,
                    user_identity TEXT,
                    avatar TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(platform_id, user_id),
                    FOREIGN KEY (platform_id) REFERENCES platforms(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS follower_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    platform_id INTEGER NOT NULL,
                    user_identity TEXT,
                    follower_count INTEGER NOT NULL,
                    record_time TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'success',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (platform_id) REFERENCES platforms(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schedule_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL UNIQUE,
                    platform_id INTEGER,
                    schedule_time TEXT NOT NULL,
                    is_enabled INTEGER DEFAULT 1,
                    last_run_time TIMESTAMP,
                    next_run_time TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    max_retry INTEGER DEFAULT 3,
                    status TEXT DEFAULT 'idle',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (platform_id) REFERENCES platforms(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS task_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL,
                    records_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES schedule_tasks(id)
                )
            ''')

            self._init_default_platforms(cursor)
            self._init_default_tasks(cursor)

            cursor.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'user_identity' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN user_identity TEXT")
                cursor.execute("UPDATE users SET user_identity = '0' WHERE user_identity IS NULL")
                self.logger.info("已添加 user_identity 字段到 users 表，并填充默认值")

            cursor.execute("PRAGMA table_info(follower_records)")
            follower_columns = [col[1] for col in cursor.fetchall()]

            if 'user_identity' not in follower_columns:
                cursor.execute("ALTER TABLE follower_records ADD COLUMN user_identity TEXT")
                cursor.execute("UPDATE follower_records SET user_identity = '0' WHERE user_identity IS NULL")
                self.logger.info("已添加 user_identity 字段到 follower_records 表，并填充默认值")

            conn.commit()
            self.logger.info("数据库初始化完成")

    def _init_default_platforms(self, cursor):
        """初始化默认平台数据"""
        platforms = [
            ('微博', 'weibo', '微博平台'),
            ('小红书', 'xiaohongshu', '小红书平台'),
            ('抖音', 'douyin', '抖音平台')
        ]

        for name, code, description in platforms:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO platforms (name, code, description) VALUES (?, ?, ?)",
                    (name, code, description)
                )
            except sqlite3.IntegrityError:
                pass

    def _init_default_tasks(self, cursor):
        """初始化默认定时任务"""
        cursor.execute("SELECT id, code FROM platforms")
        platforms = cursor.fetchall()

        for platform in platforms:
            platform_id, code = platform['id'], platform['code']
            task_name = f"{code}_follower_crawler"
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO schedule_tasks (task_name, platform_id, schedule_time) VALUES (?, ?, ?)",
                    (task_name, platform_id, "23:59")
                )
            except sqlite3.IntegrityError:
                pass

    def insert_platform(self, name: str, code: str, description: str = "") -> int:
        """
        插入平台信息

        Args:
            name: 平台名称
            code: 平台代码
            description: 平台描述

        Returns:
            插入的记录ID
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO platforms (name, code, description) VALUES (?, ?, ?)",
                (name, code, description)
            )
            return cursor.lastrowid

    def get_platform_by_code(self, code: str) -> Optional[Dict]:
        """
        根据代码获取平台信息

        Args:
            code: 平台代码

        Returns:
            平台信息字典
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM platforms WHERE code = ?", (code,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_platforms(self) -> List[Dict]:
        """
        获取所有平台信息

        Returns:
            平台信息列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM platforms")
            return [dict(row) for row in cursor.fetchall()]

    def insert_user(self, platform_id: int, user_id: str, username: str = "", 
                   user_identity: str = "", avatar: str = "") -> int:
        """
        插入用户信息

        Args:
            platform_id: 平台ID
            user_id: 用户ID
            username: 用户名
            user_identity: 用户标识（用于跨平台关联同一用户）
            avatar: 头像URL

        Returns:
            插入的记录ID
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """INSERT OR IGNORE INTO users 
                   (platform_id, user_id, username, user_identity, avatar, created_at, updated_at) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (platform_id, user_id, username, user_identity, avatar, datetime.now(), datetime.now())
            )

            if cursor.lastrowid == 0:
                update_fields = []
                update_values = []
                
                if username:
                    update_fields.append("username = ?")
                    update_values.append(username)
                if user_identity:
                    update_fields.append("user_identity = ?")
                    update_values.append(user_identity)
                if avatar:
                    update_fields.append("avatar = ?")
                    update_values.append(avatar)
                
                update_fields.append("updated_at = ?")
                update_values.append(datetime.now())
                update_values.extend([platform_id, user_id])
                
                if update_fields:
                    cursor.execute(
                        f"""UPDATE users 
                           SET {', '.join(update_fields)} 
                           WHERE platform_id = ? AND user_id = ?""",
                        update_values
                    )
                
                cursor.execute(
                    "SELECT id FROM users WHERE platform_id = ? AND user_id = ?",
                    (platform_id, user_id)
                )
                row = cursor.fetchone()
                return row['id'] if row else cursor.lastrowid

            return cursor.lastrowid

    def update_user_identity(self, platform_id: int, user_id: str, user_identity: str) -> bool:
        """
        更新用户标识（只更新 user_identity 字段，不影响其他字段）

        Args:
            platform_id: 平台ID
            user_id: 用户ID
            user_identity: 用户标识

        Returns:
            是否更新成功
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE users 
                   SET user_identity = ?, updated_at = ? 
                   WHERE platform_id = ? AND user_id = ?""",
                (user_identity, datetime.now(), platform_id, user_id)
            )
            return cursor.rowcount > 0

    def get_user_by_platform_and_id(self, platform_id: int, user_id: str) -> Optional[Dict]:
        """
        根据平台ID和用户ID获取用户信息

        Args:
            platform_id: 平台ID
            user_id: 用户ID

        Returns:
            用户信息字典
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM users WHERE platform_id = ? AND user_id = ?",
                (platform_id, user_id)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_users(self, platform_id: int = None) -> List[Dict]:
        """
        获取所有用户信息

        Args:
            platform_id: 平台ID（可选）

        Returns:
            用户信息列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if platform_id:
                cursor.execute("SELECT * FROM users WHERE platform_id = ?", (platform_id,))
            else:
                cursor.execute("SELECT * FROM users")
            return [dict(row) for row in cursor.fetchall()]

    def insert_follower_record(self, user_id: int, platform_id: int, follower_count: int,
                              user_identity: str = "",
                              record_time: datetime = None, status: str = "success",
                              error_message: str = "") -> int:
        """
        插入粉丝量记录

        Args:
            user_id: 用户ID
            platform_id: 平台ID
            follower_count: 粉丝数量
            user_identity: 用户标识
            record_time: 记录时间
            status: 状态
            error_message: 错误信息

        Returns:
            插入的记录ID
        """
        if record_time is None:
            record_time = datetime.now()

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO follower_records 
                   (user_id, platform_id, user_identity, follower_count, record_time, status, error_message) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, platform_id, user_identity, follower_count, record_time, status, error_message)
            )
            return cursor.lastrowid

    def get_follower_records(self, user_id: int = None, platform_id: int = None,
                           platform_ids: List[int] = None,
                           user_identity: str = None,
                           start_time: datetime = None, end_time: datetime = None,
                           limit: int = 100) -> List[Dict]:
        """
        获取粉丝量记录

        Args:
            user_id: 用户ID（可选）
            platform_id: 平台ID（可选）
            platform_ids: 平台ID列表（可选）
            user_identity: 用户标识（可选）
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）
            limit: 返回记录数限制

        Returns:
            粉丝量记录列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            query = "SELECT * FROM follower_records WHERE 1=1"
            params = []

            if user_id:
                query += " AND user_id = ?"
                params.append(user_id)

            if platform_id:
                query += " AND platform_id = ?"
                params.append(platform_id)
            elif platform_ids:
                placeholders = ','.join(['?' for _ in platform_ids])
                query += f" AND platform_id IN ({placeholders})"
                params.extend(platform_ids)

            if user_identity:
                query += " AND user_identity = ?"
                params.append(str(user_identity))

            if start_time:
                query += " AND record_time >= ?"
                params.append(start_time)

            if end_time:
                query += " AND record_time <= ?"
                params.append(end_time)

            query += " ORDER BY record_time DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_latest_follower_count(self, user_id: int) -> Optional[int]:
        """
        获取用户最新的粉丝数量

        Args:
            user_id: 用户ID

        Returns:
            最新粉丝数量
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT follower_count FROM follower_records WHERE user_id = ? ORDER BY record_time DESC LIMIT 1",
                (user_id,)
            )
            row = cursor.fetchone()
            return row['follower_count'] if row else None

    def insert_task_log(self, task_id: int, start_time: datetime, end_time: datetime = None,
                       status: str = "running", records_count: int = 0,
                       success_count: int = 0, failed_count: int = 0,
                       error_message: str = "") -> int:
        """
        插入任务执行日志

        Args:
            task_id: 任务ID
            start_time: 开始时间
            end_time: 结束时间
            status: 状态
            records_count: 记录总数
            success_count: 成功数
            failed_count: 失败数
            error_message: 错误信息

        Returns:
            插入的记录ID
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO task_logs 
                   (task_id, start_time, end_time, status, records_count, success_count, failed_count, error_message) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (task_id, start_time, end_time, status, records_count, success_count, failed_count, error_message)
            )
            return cursor.lastrowid

    def update_task_log(self, log_id: int, end_time: datetime = None, status: str = None,
                       records_count: int = None, success_count: int = None,
                       failed_count: int = None, error_message: str = None):
        """
        更新任务执行日志

        Args:
            log_id: 日志ID
            end_time: 结束时间
            status: 状态
            records_count: 记录总数
            success_count: 成功数
            failed_count: 失败数
            error_message: 错误信息
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            updates = []
            params = []

            if end_time is not None:
                updates.append("end_time = ?")
                params.append(end_time)

            if status is not None:
                updates.append("status = ?")
                params.append(status)

            if records_count is not None:
                updates.append("records_count = ?")
                params.append(records_count)

            if success_count is not None:
                updates.append("success_count = ?")
                params.append(success_count)

            if failed_count is not None:
                updates.append("failed_count = ?")
                params.append(failed_count)

            if error_message is not None:
                updates.append("error_message = ?")
                params.append(error_message)

            if updates:
                params.append(log_id)
                cursor.execute(
                    f"UPDATE task_logs SET {', '.join(updates)} WHERE id = ?",
                    params
                )

    def get_task_logs(self, task_id: int = None, limit: int = 50) -> List[Dict]:
        """
        获取任务执行日志

        Args:
            task_id: 任务ID（可选）
            limit: 返回记录数限制

        Returns:
            任务日志列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if task_id:
                cursor.execute(
                    "SELECT * FROM task_logs WHERE task_id = ? ORDER BY start_time DESC LIMIT ?",
                    (task_id, limit)
                )
            else:
                cursor.execute("SELECT * FROM task_logs ORDER BY start_time DESC LIMIT ?", (limit,))

            return [dict(row) for row in cursor.fetchall()]

    def update_task_status(self, task_id: int, status: str, last_run_time: datetime = None,
                          next_run_time: datetime = None, retry_count: int = None):
        """
        更新任务状态

        Args:
            task_id: 任务ID
            status: 状态
            last_run_time: 最后运行时间
            next_run_time: 下次运行时间
            retry_count: 重试次数
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            updates = ["status = ?", "updated_at = ?"]
            params = [status, datetime.now()]

            if last_run_time is not None:
                updates.append("last_run_time = ?")
                params.append(last_run_time)

            if next_run_time is not None:
                updates.append("next_run_time = ?")
                params.append(next_run_time)

            if retry_count is not None:
                updates.append("retry_count = ?")
                params.append(retry_count)

            params.append(task_id)
            cursor.execute(
                f"UPDATE schedule_tasks SET {', '.join(updates)} WHERE id = ?",
                params
            )

    def get_task_by_name(self, task_name: str) -> Optional[Dict]:
        """
        根据任务名称获取任务信息

        Args:
            task_name: 任务名称

        Returns:
            任务信息字典
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM schedule_tasks WHERE task_name = ?", (task_name,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_tasks(self) -> List[Dict]:
        """
        获取所有任务信息

        Returns:
            任务信息列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM schedule_tasks")
            return [dict(row) for row in cursor.fetchall()]

    def export_to_csv(self, user_id: int = None, platform_id: int = None,
                     start_time: datetime = None, end_time: datetime = None,
                     output_path: str = None) -> str:
        """
        导出数据到CSV文件

        Args:
            user_id: 用户ID（可选）
            platform_id: 平台ID（可选）
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）
            output_path: 输出文件路径（可选）

        Returns:
            导出文件路径
        """
        import csv

        if output_path is None:
            os.makedirs(DataConfig.CSV_DIR, exist_ok=True)
            output_path = os.path.join(
                DataConfig.CSV_DIR,
                f"follower_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )

        records = self.get_follower_records(user_id, platform_id, start_time, end_time, limit=10000)

        if not records:
            self.logger.warning("没有可导出的数据")
            return ""

        with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
            fieldnames = ['id', 'user_id', 'platform_id', 'follower_count',
                         'record_time', 'status', 'error_message', 'created_at']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        self.logger.info(f"数据已导出到: {output_path}")
        return output_path

    def delete_follower_record(self, record_id: int) -> bool:
        """
        删除单条粉丝记录

        Args:
            record_id: 记录ID

        Returns:
            是否删除成功
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM follower_records WHERE id = ?", (record_id,))
            deleted = cursor.rowcount > 0
            if deleted:
                self.logger.info(f"已删除粉丝记录: ID={record_id}")
            return deleted

    def delete_user(self, user_id: int, delete_records: bool = True) -> bool:
        """
        删除用户

        Args:
            user_id: 用户ID
            delete_records: 是否同时删除该用户的粉丝记录

        Returns:
            是否删除成功
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if delete_records:
                cursor.execute("DELETE FROM follower_records WHERE user_id = ?", (user_id,))
                deleted_records = cursor.rowcount
                self.logger.info(f"已删除用户 {user_id} 的 {deleted_records} 条粉丝记录")

            cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
            deleted = cursor.rowcount > 0
            if deleted:
                self.logger.info(f"已删除用户: ID={user_id}")
            return deleted
