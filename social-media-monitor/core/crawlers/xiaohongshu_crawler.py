import json
import requests
import re
import time
from typing import Dict, Optional
from config import XiaohongshuConfig
from core.cookie_database import CookieDatabase


class XiaohongshuCrawler:
    """小红书博主信息爬虫类"""

    def __init__(self, cookie: str = None, delay: int = None):
        """
        初始化爬虫

        Args:
            cookie: 小红书Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.cookie_db = CookieDatabase()
        self.fixed_cookie = cookie
        self.delay = delay or XiaohongshuConfig.DELAY
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.xiaohongshu.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Origin": "https://www.xiaohongshu.com"
        }

    def _get_cookie(self) -> str:
        """
        获取当前Cookie（每次都从数据库读取）

        Returns:
            Cookie字符串
        """
        if self.fixed_cookie:
            return self.fixed_cookie
        cookie = self.cookie_db.get_cookie('xiaohongshu')
        return cookie if cookie else XiaohongshuConfig.COOKIE

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

    def get_follower_count(self, profile_url: str) -> Optional[int]:
        """
        获取博主粉丝数量

        Args:
            profile_url: 博主主页URL

        Returns:
            粉丝数量，失败返回None
        """
        try:
            html = self.get_html(profile_url)

            pattern = r'__INITIAL_STATE__\s*=\s*({.+?});'
            match = re.search(pattern, html)

            if match:
                json_str = match.group(1)
                data = json.loads(json_str)

                possible_paths = [
                    data.get('user', {}).get('userPageData', {}).get('user', {}),
                    data.get('user', {}).get('user', {}),
                    data.get('userPageData', {}).get('user', {}),
                    data.get('note', {}).get('noteDetail', {}).get('user', {}),
                ]

                for path in possible_paths:
                    if path and path.get('nickname'):
                        follower_count = path.get('fans', 0)
                        return follower_count

            follower_patterns = [
                r'(\d+(?:\.\d+)?)\s*万?\s*粉丝',
                r'粉丝\s*(\d+(?:\.\d+)?万?)',
                r'(\d+)(?:万+)?粉丝',
                r'fans["\s:]+(\d+)',
                r'粉丝["\s:]+(\d+)',
                r'(\d+)\s*万\s*粉丝',
                r'(\d+)\s*位粉丝',
                r'(\d+)\s*个粉丝',
                r'粉丝数["\s:]+(\d+)',
            ]

            for pattern in follower_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    follower_str = match.group(1)
                    if '万' in follower_str:
                        return int(float(follower_str.replace('万', '')) * 10000)
                    else:
                        return int(follower_str)

            return None

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取粉丝数量时发生错误: {e}")
            return None

    def get_user_info(self, profile_url: str) -> Optional[Dict]:
        """
        获取博主基本信息

        Args:
            profile_url: 博主主页URL

        Returns:
            用户信息字典，失败返回None
        """
        try:
            html = self.get_html(profile_url)

            user_info = {
                'profile_url': profile_url,
                'nickname': '',
                'user_id': '',
                'follower_count': 0,
                'following_count': 0,
                'verified': False
            }

            pattern = r'__INITIAL_STATE__\s*=\s*({.+?});'
            match = re.search(pattern, html)

            if match:
                json_str = match.group(1)
                data = json.loads(json_str)

                possible_paths = [
                    data.get('user', {}).get('userPageData', {}).get('user', {}),
                    data.get('user', {}).get('user', {}),
                    data.get('userPageData', {}).get('user', {}),
                    data.get('note', {}).get('noteDetail', {}).get('user', {}),
                ]

                for path in possible_paths:
                    if path and path.get('nickname'):
                        user_info['nickname'] = path.get('nickname', '')
                        user_info['user_id'] = path.get('user_id', '')
                        user_info['follower_count'] = path.get('fans', 0)
                        user_info['following_count'] = path.get('follows', 0)
                        user_info['verified'] = path.get('officialVerify', {}).get('type', 0) > 0
                        return user_info

            og_title_pattern = r'<meta name="og:title" content="([^"]+)"'
            og_title_match = re.search(og_title_pattern, html)
            if og_title_match:
                og_title = og_title_match.group(1)
                nickname = og_title.split(' - 小红书')[0] if ' - 小红书' in og_title else og_title
                user_info['nickname'] = nickname

            follower_count = self.get_follower_count(profile_url)
            if follower_count is not None:
                user_info['follower_count'] = follower_count

            user_id_pattern = r'user/profile/([a-f0-9]+)'
            user_id_match = re.search(user_id_pattern, profile_url)
            if user_id_match:
                user_info['user_id'] = user_id_match.group(1)

            return user_info

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取用户信息时发生错误: {e}")
            return None
