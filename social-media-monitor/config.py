import os
from dotenv import load_dotenv
from typing import List

load_dotenv()


class Config:
    """配置类"""

    @staticmethod
    def get_env(key: str, default: str = "") -> str:
        """获取环境变量"""
        return os.getenv(key, default)

    @staticmethod
    def get_env_list(key: str, default: List[str] = None) -> List[str]:
        """获取环境变量列表"""
        value = os.getenv(key, "")
        if not value:
            return default or []
        return [item.strip() for item in value.split(",") if item.strip()]

    @staticmethod
    def get_env_bool(key: str, default: bool = False) -> bool:
        """获取环境变量布尔值"""
        value = os.getenv(key, "").lower()
        return value in ["true", "1", "yes"] if value else default

    @staticmethod
    def get_env_int(key: str, default: int = 0) -> int:
        """获取环境变量整数值"""
        try:
            return int(os.getenv(key, default))
        except (ValueError, TypeError):
            return default


class WeiboConfig(Config):
    """微博配置"""

    COOKIE = Config.get_env("WEIBO_COOKIE")
    UID_LIST = Config.get_env_list("WEIBO_UID_LIST")
    DELAY = Config.get_env_int("WEIBO_DELAY", 3)


class XiaohongshuConfig(Config):
    """小红书配置"""

    COOKIE = Config.get_env("XIAOHONGSHU_COOKIE")
    URL_LIST = Config.get_env_list("XIAOHONGSHU_URL_LIST")
    DELAY = Config.get_env_int("XIAOHONGSHU_DELAY", 2)


class DouyinConfig(Config):
    """抖音配置"""

    COOKIE = Config.get_env("DOUYIN_COOKIE")
    SEC_USER_ID_LIST = Config.get_env_list("DOUYIN_SEC_USER_ID_LIST")
    DELAY = Config.get_env_int("DOUYIN_DELAY", 2)


class ScheduleConfig(Config):
    """定时任务配置"""

    TIME = Config.get_env("SCHEDULE_TIME", "23:59")
    ENABLED = Config.get_env_bool("SCHEDULE_ENABLED", False)


class DataConfig(Config):
    """数据存储配置"""

    DATA_DIR = Config.get_env("DATA_DIR", "data")
    CSV_DIR = Config.get_env("CSV_DIR", "data/exports")
    RAW_DATA_DIR = Config.get_env("RAW_DATA_DIR", "data/raw")
    PROCESSED_DATA_DIR = Config.get_env("PROCESSED_DATA_DIR", "data/processed")

    @classmethod
    def ensure_dirs(cls):
        """确保所有数据目录存在"""
        for dir_path in [cls.DATA_DIR, cls.CSV_DIR, cls.RAW_DATA_DIR, cls.PROCESSED_DATA_DIR]:
            os.makedirs(dir_path, exist_ok=True)


class StreamlitConfig(Config):
    """Streamlit配置"""

    TITLE = Config.get_env("STREAMLIT_TITLE", "社交媒体粉丝量监控平台")
    PAGE_ICON = Config.get_env("STREAMLIT_PAGE_ICON", "📊")


class AppConfig:
    """应用配置集合"""

    weibo = WeiboConfig
    xiaohongshu = XiaohongshuConfig
    douyin = DouyinConfig
    schedule = ScheduleConfig
    data = DataConfig
    streamlit = StreamlitConfig

    @classmethod
    def init(cls):
        """初始化配置"""
        cls.data.ensure_dirs()
