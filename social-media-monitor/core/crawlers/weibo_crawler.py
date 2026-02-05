import json
import requests
import time
from typing import Dict, Optional
from config import WeiboConfig
from core.cookie_database import CookieDatabase


class WeiboCrawler:
    """微博用户信息爬虫类"""

    def __init__(self, cookie: str = None, delay: int = None):
        """
        初始化爬虫

        Args:
            cookie: 微博Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.cookie_db = CookieDatabase()
        self.fixed_cookie = cookie
        self.delay = delay or WeiboConfig.DELAY
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
            "Referer": "https://weibo.com",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }

    def _get_cookie(self) -> str:
        """
        获取当前Cookie（每次都从数据库读取）

        Returns:
            Cookie字符串
        """
        if self.fixed_cookie:
            return self.fixed_cookie
        cookie = self.cookie_db.get_cookie('weibo')
        return cookie if cookie else WeiboConfig.COOKIE

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
        try:
            response = requests.get(
                url,
                headers=self.headers,
                cookies={"cookie": self._get_cookie()} if self._get_cookie() else {},
                timeout=30
            )
            response.raise_for_status()
            time.sleep(self.delay)
            return response.text
        except requests.RequestException as e:
            print(f"请求失败: {url}, 错误: {e}")
            raise

    def get_follower_count(self, uid: str) -> Optional[int]:
        """
        获取用户粉丝数量

        Args:
            uid: 用户ID

        Returns:
            粉丝数量，失败返回None
        """
        try:
            url = f"https://weibo.com/ajax/profile/info?uid={uid}"
            html = self.get_html(url)
            response = json.loads(html)

            data = response.get('data', {}).get('user', {})

            if not data:
                print(f"用户 {uid} 信息获取失败")
                return None

            follower_count = data.get('followers_count', 0)
            return follower_count

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取粉丝数量时发生错误: {e}")
            return None

    def get_user_info(self, uid: str) -> Optional[Dict]:
        """
        获取用户基本信息

        Args:
            uid: 用户ID

        Returns:
            用户信息字典，失败返回None
        """
        try:
            url = f"https://weibo.com/ajax/profile/info?uid={uid}"
            html = self.get_html(url)
            response = json.loads(html)

            data = response.get('data', {}).get('user', {})

            if not data:
                print(f"用户 {uid} 信息获取失败")
                return None

            user_info = {
                'uid': uid,
                'screen_name': data.get('screen_name', ''),
                'follower_count': data.get('followers_count', 0),
                'friends_count': data.get('friends_count', 0),
                'statuses_count': data.get('statuses_count', 0),
                'verified': data.get('verified', False)
            }

            return user_info

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取用户信息时发生错误: {e}")
            return None
