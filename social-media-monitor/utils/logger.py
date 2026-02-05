import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from config import DataConfig


class Logger:
    """日志工具类"""

    _loggers = {}

    @classmethod
    def get_logger(cls, name: str = "app", level: int = logging.INFO) -> logging.Logger:
        """
        获取日志记录器

        Args:
            name: 日志记录器名称
            level: 日志级别

        Returns:
            日志记录器实例
        """
        if name in cls._loggers:
            return cls._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            log_dir = os.path.join(DataConfig.DATA_DIR, "logs")
            os.makedirs(log_dir, exist_ok=True)

            log_file = os.path.join(log_dir, f"{name}.log")
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        cls._loggers[name] = logger
        return logger

    @classmethod
    def info(cls, message: str, name: str = "app"):
        """记录信息日志"""
        logger = cls.get_logger(name)
        logger.info(message)

    @classmethod
    def error(cls, message: str, name: str = "app", exc_info: bool = False):
        """记录错误日志"""
        logger = cls.get_logger(name)
        logger.error(message, exc_info=exc_info)

    @classmethod
    def warning(cls, message: str, name: str = "app"):
        """记录警告日志"""
        logger = cls.get_logger(name)
        logger.warning(message)

    @classmethod
    def debug(cls, message: str, name: str = "app"):
        """记录调试日志"""
        logger = cls.get_logger(name)
        logger.debug(message)
