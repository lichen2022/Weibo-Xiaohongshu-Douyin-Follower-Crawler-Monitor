import json
import requests
import time
from typing import Dict, Optional
from config import DouyinConfig
from core.cookie_database import CookieDatabase
from utils.logger import Logger


class DouyinCrawler:
    """抖音博主信息爬虫类"""

    def __init__(self, cookie: str = None, delay: int = None):
        """
        初始化爬虫

        Args:
            cookie: 抖音Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.logger = Logger.get_logger("douyin_crawler")
        self.cookie_db = CookieDatabase()
        self.fixed_cookie = cookie
        self.delay = delay or DouyinConfig.DELAY
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.douyin.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Origin": "https://www.douyin.com"
        }
        self.logger.info(f"抖音爬虫初始化完成，延迟时间: {self.delay}秒")

    def _get_cookie(self) -> str:
        """
        获取当前Cookie（每次都从数据库读取）

        Returns:
            Cookie字符串
        """
        if self.fixed_cookie:
            self.logger.debug("使用固定的Cookie")
            return self.fixed_cookie
        cookie = self.cookie_db.get_cookie('douyin')
        if cookie:
            self.logger.debug("从数据库获取到Cookie")
            return cookie
        else:
            self.logger.warning("数据库中未找到Cookie，使用环境变量中的Cookie")
            return DouyinConfig.COOKIE

    def get_html(self, url: str) -> str:
        """
        发送HTTP请求获取HTML内容

        Args:
            url: 目标URL

        Returns:
            响应文本内容

        Raises:
            requests.RequestException: 请求异常
        """
        self.logger.info(f"开始获取HTML内容，URL: {url}")
        
        try:
            cookie = self._get_cookie()
            headers = self.headers.copy()
            if cookie:
                headers['Cookie'] = cookie
                self.logger.debug(f"已设置Cookie，Cookie长度: {len(cookie)}")
            else:
                self.logger.warning("未设置Cookie，可能导致请求失败")
            
            self.logger.info(f"请求头: {headers}")
            self.logger.info(f"开始发送HTTP请求，超时时间: 30秒")
            
            response = requests.get(
                url,
                headers=headers,
                timeout=30
            )
            
            self.logger.info(f"响应状态码: {response.status_code}")
            self.logger.info(f"响应头: {dict(response.headers)}")
            self.logger.info(f"响应内容长度: {len(response.text)} 字节")
            
            response.raise_for_status()
            time.sleep(self.delay)
            
            self.logger.info(f"成功获取HTML内容，长度: {len(response.text)} 字节")
            return response.text
        except requests.exceptions.Timeout as e:
            self.logger.error(f"请求超时: {e}", exc_info=True)
            self.logger.error(f"超时URL: {url}")
            self.logger.error(f"请求超时时间: 30秒")
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"连接错误: {e}", exc_info=True)
            self.logger.error(f"连接URL: {url}")
            self.logger.error("可能原因: 网络连接问题、DNS解析失败或目标服务器不可达")
            raise
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP错误: {e}", exc_info=True)
            self.logger.error(f"HTTP状态码: {response.status_code}")
            self.logger.error(f"响应内容: {response.text[:500]}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求异常: {e}", exc_info=True)
            self.logger.error(f"请求URL: {url}")
            self.logger.error(f"异常类型: {type(e).__name__}")
            raise

    def get_follower_count(self, sec_user_id: str) -> Optional[int]:
        """
        获取博主粉丝数量

        Args:
            sec_user_id: 用户安全ID

        Returns:
            粉丝数量，失败返回None
        """
        self.logger.info(f"开始获取博主粉丝数量，sec_user_id: {sec_user_id}")
        
        try:
            url = f"https://www.douyin.com/aweme/v1/web/user/profile/other/?sec_user_id={sec_user_id}"

            self.logger.info(f"正在请求API: {url}")
            cookie = self._get_cookie()
            headers = self.headers.copy()
            if cookie:
                headers['Cookie'] = cookie
                self.logger.debug(f"已设置Cookie，Cookie长度: {len(cookie)}")
            else:
                self.logger.warning("未设置Cookie，可能导致请求失败")
            
            self.logger.info(f"请求头: {headers}")
            self.logger.info(f"开始发送HTTP请求，超时时间: 30秒")
            
            response = requests.get(
                url,
                headers=headers,
                timeout=30
            )
            
            self.logger.info(f"响应状态码: {response.status_code}")
            self.logger.info(f"响应头: {dict(response.headers)}")
            self.logger.info(f"响应内容长度: {len(response.text)} 字节")
            
            response.raise_for_status()
            time.sleep(self.delay)

            self.logger.info("开始解析JSON响应")
            data = response.json()
            self.logger.debug(f"响应数据: {json.dumps(data, ensure_ascii=False, indent=2)[:1000]}")

            if data.get('status_code') == 0 and data.get('user'):
                user_info = data['user']
                follower_count = user_info.get('follower_count', 0)
                self.logger.info(f"成功获取粉丝数量: {follower_count:,}")
                return follower_count
            else:
                status_code = data.get('status_code')
                status_msg = data.get('status_msg', '未知错误')
                self.logger.error(f"API返回错误 - status_code: {status_code}, status_msg: {status_msg}")
                self.logger.error(f"完整响应数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
                return None

        except requests.exceptions.Timeout as e:
            self.logger.error(f"请求超时: {e}", exc_info=True)
            self.logger.error(f"超时URL: {url}")
            self.logger.error(f"请求超时时间: 30秒")
            return None
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"连接错误: {e}", exc_info=True)
            self.logger.error(f"连接URL: {url}")
            self.logger.error("可能原因: 网络连接问题、DNS解析失败或目标服务器不可达")
            return None
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP错误: {e}", exc_info=True)
            self.logger.error(f"HTTP状态码: {response.status_code}")
            self.logger.error(f"响应内容: {response.text[:500]}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求异常: {e}", exc_info=True)
            self.logger.error(f"请求URL: {url}")
            self.logger.error(f"异常类型: {type(e).__name__}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}", exc_info=True)
            self.logger.error(f"响应内容前500字符: {response.text[:500]}")
            return None
        except Exception as e:
            self.logger.error(f"获取粉丝数量时发生未知错误: {e}", exc_info=True)
            self.logger.error(f"错误类型: {type(e).__name__}")
            self.logger.error(f"sec_user_id: {sec_user_id}")
            return None

    def get_user_info(self, sec_user_id: str) -> Optional[Dict]:
        """
        获取博主基本信息

        Args:
            sec_user_id: 用户安全ID

        Returns:
            用户信息字典，失败返回None
        """
        self.logger.info(f"开始获取博主用户信息，sec_user_id: {sec_user_id}")
        
        try:
            url = f"https://www.douyin.com/aweme/v1/web/user/profile/other/?sec_user_id={sec_user_id}"

            self.logger.info(f"正在请求API: {url}")
            cookie = self._get_cookie()
            headers = self.headers.copy()
            if cookie:
                headers['Cookie'] = cookie
                self.logger.debug(f"已设置Cookie，Cookie长度: {len(cookie)}")
            else:
                self.logger.warning("未设置Cookie，可能导致请求失败")
            
            self.logger.info(f"请求头: {headers}")
            self.logger.info(f"开始发送HTTP请求，超时时间: 30秒")
            
            response = requests.get(
                url,
                headers=headers,
                timeout=30
            )
            
            self.logger.info(f"响应状态码: {response.status_code}")
            self.logger.info(f"响应头: {dict(response.headers)}")
            self.logger.info(f"响应内容长度: {len(response.text)} 字节")
            
            response.raise_for_status()
            time.sleep(self.delay)

            self.logger.info("开始解析JSON响应")
            data = response.json()
            self.logger.debug(f"响应数据: {json.dumps(data, ensure_ascii=False, indent=2)[:1000]}")

            if data.get('status_code') == 0 and data.get('user'):
                user_info = data['user']
                
                user_data = {
                    'sec_user_id': sec_user_id,
                    'nickname': user_info.get('nickname', ''),
                    'user_id': user_info.get('uid', ''),
                    'follower_count': user_info.get('follower_count', 0),
                    'following_count': user_info.get('following_count', 0),
                    'aweme_count': user_info.get('aweme_count', 0),
                    'verified': user_info.get('custom_verify', '') != '' or user_info.get('enterprise_verify_reason', '') != ''
                }

                self.logger.info(f"成功获取用户信息 - 昵称: {user_data['nickname']}, 粉丝数: {user_data['follower_count']:,}, 关注数: {user_data['following_count']:,}, 作品数: {user_data['aweme_count']:,}, 认证状态: {user_data['verified']}")
                self.logger.debug(f"完整用户数据: {json.dumps(user_data, ensure_ascii=False, indent=2)}")
                return user_data
            else:
                status_code = data.get('status_code')
                status_msg = data.get('status_msg', '未知错误')
                self.logger.error(f"API返回错误 - status_code: {status_code}, status_msg: {status_msg}")
                self.logger.error(f"完整响应数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
                return None

        except requests.exceptions.Timeout as e:
            self.logger.error(f"请求超时: {e}", exc_info=True)
            self.logger.error(f"超时URL: {url}")
            self.logger.error(f"请求超时时间: 30秒")
            return None
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"连接错误: {e}", exc_info=True)
            self.logger.error(f"连接URL: {url}")
            self.logger.error("可能原因: 网络连接问题、DNS解析失败或目标服务器不可达")
            return None
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP错误: {e}", exc_info=True)
            self.logger.error(f"HTTP状态码: {response.status_code}")
            self.logger.error(f"响应内容: {response.text[:500]}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求异常: {e}", exc_info=True)
            self.logger.error(f"请求URL: {url}")
            self.logger.error(f"异常类型: {type(e).__name__}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}", exc_info=True)
            self.logger.error(f"响应内容前500字符: {response.text[:500]}")
            return None
        except Exception as e:
            self.logger.error(f"获取用户信息时发生未知错误: {e}", exc_info=True)
            self.logger.error(f"错误类型: {type(e).__name__}")
            self.logger.error(f"sec_user_id: {sec_user_id}")
            return None
