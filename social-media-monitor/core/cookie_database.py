import sqlite3
import os
from datetime import datetime
from typing import Optional, Dict
from contextlib import contextmanager
from config import DataConfig
from utils.logger import Logger


def adapt_datetime(dt):
    """将datetime对象适配为SQLite字符串"""
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def convert_datetime(s):
    """将SQLite字符串转换为datetime对象"""
    return datetime.strptime(s.decode('utf-8'), '%Y-%m-%d %H:%M:%S')


sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("TIMESTAMP", convert_datetime)


class CookieDatabase:
    """Cookie数据库管理类"""

    def __init__(self, db_path: str = None):
        """
        初始化Cookie数据库

        Args:
            db_path: 数据库文件路径
        """
        if db_path is None:
            os.makedirs(DataConfig.DATA_DIR, exist_ok=True)
            db_path = os.path.join(DataConfig.DATA_DIR, "cookies.db")

        self.db_path = db_path
        self.logger = Logger.get_logger("cookie_database")
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
            conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
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
                CREATE TABLE IF NOT EXISTS cookies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL UNIQUE,
                    cookie TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime'))
                )
            ''')

            self.logger.info("Cookie数据库初始化完成")

    def save_cookie(self, platform: str, cookie: str) -> bool:
        """
        保存或更新Cookie

        Args:
            platform: 平台名称（weibo/xiaohongshu/douyin）
            cookie: Cookie字符串

        Returns:
            是否成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    INSERT OR REPLACE INTO cookies (platform, cookie, updated_at)
                    VALUES (?, ?, datetime('now', 'localtime'))
                ''', (platform, cookie))

                self.logger.info(f"Cookie已保存: {platform}")
                return True
        except Exception as e:
            self.logger.error(f"保存Cookie失败: {platform}, 错误: {e}", exc_info=True)
            return False

    def get_cookie(self, platform: str) -> Optional[str]:
        """
        获取指定平台的Cookie

        Args:
            platform: 平台名称（weibo/xiaohongshu/douyin）

        Returns:
            Cookie字符串，不存在返回None
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT cookie FROM cookies WHERE platform = ?
                    ORDER BY updated_at DESC LIMIT 1
                ''', (platform,))

                row = cursor.fetchone()
                return row['cookie'] if row else None
        except Exception as e:
            self.logger.error(f"获取Cookie失败: {platform}, 错误: {e}", exc_info=True)
            return None

    def get_all_cookies(self) -> Dict[str, str]:
        """
        获取所有平台的Cookie

        Returns:
            平台到Cookie的映射字典
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT platform, cookie FROM cookies')

                return {row['platform']: row['cookie'] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"获取所有Cookie失败: {e}", exc_info=True)
            return {}

    def delete_cookie(self, platform: str) -> bool:
        """
        删除指定平台的Cookie

        Args:
            platform: 平台名称（weibo/xiaohongshu/douyin）

        Returns:
            是否成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM cookies WHERE platform = ?', (platform,))

                self.logger.info(f"Cookie已删除: {platform}")
                return True
        except Exception as e:
            self.logger.error(f"删除Cookie失败: {platform}, 错误: {e}", exc_info=True)
            return False

    def update_cookie(self, platform: str, cookie: str) -> bool:
        """
        更新指定平台的Cookie

        Args:
            platform: 平台名称（weibo/xiaohongshu/douyin）
            cookie: Cookie字符串

        Returns:
            是否成功
        """
        return self.save_cookie(platform, cookie)
