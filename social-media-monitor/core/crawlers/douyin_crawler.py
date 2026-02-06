import json
import requests
import time
from typing import Dict, Optional
from config import DouyinConfig
from core.cookie_database import CookieDatabase


class DouyinCrawler:
    """抖音博主信息爬虫类"""

    def __init__(self, cookie: str = None, delay: int = None):
        """
        初始化爬虫

        Args:
            cookie: 抖音Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.cookie_db = CookieDatabase()
        self.fixed_cookie = cookie
        self.delay = delay or DouyinConfig.DELAY
        self.max_retries = 3
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.douyin.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Origin": "https://www.douyin.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site"
        }

    def _get_cookie(self) -> str:
        """
        获取当前Cookie（每次都从数据库读取）

        Returns:
            Cookie字符串
        """
        if self.fixed_cookie:
            return self.fixed_cookie
        cookie = self.cookie_db.get_cookie('douyin')
        return cookie if cookie else DouyinConfig.COOKIE

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
            cookie = self._get_cookie()
            headers = self.headers.copy()
            
            response = requests.get(
                url,
                headers=headers,
                cookies={"cookie": cookie} if cookie else {},
                timeout=30
            )
            response.raise_for_status()
            time.sleep(self.delay)
            return response.text
        except requests.RequestException as e:
            print(f"请求失败: {url}, 错误: {e}")
            raise

    def _make_request_with_retry(self, url: str) -> Optional[requests.Response]:
        """
        带重试机制的HTTP请求

        Args:
            url: 目标URL

        Returns:
            响应对象，失败返回None
        """
        cookie = self._get_cookie()
        headers = self.headers.copy()
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    cookies={"cookie": cookie} if cookie else {},
                    timeout=30
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    print(f"请求被拒绝 (403)，可能是Cookie失效或反爬机制触发，尝试 {attempt + 1}/{self.max_retries}")
                    time.sleep(2 * (attempt + 1))
                elif response.status_code == 429:
                    print(f"请求过于频繁 (429)，等待后重试，尝试 {attempt + 1}/{self.max_retries}")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"请求失败，状态码: {response.status_code}，尝试 {attempt + 1}/{self.max_retries}")
                    time.sleep(1)
                    
            except requests.RequestException as e:
                print(f"请求异常: {e}，尝试 {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    time.sleep(2)
        
        return None

    def get_follower_count(self, sec_user_id: str) -> Optional[int]:
        """
        获取博主粉丝数量

        Args:
            sec_user_id: 用户安全ID

        Returns:
            粉丝数量，失败返回None
        """
        try:
            url = f"https://www.douyin.com/aweme/v1/web/user/profile/other/?sec_user_id={sec_user_id}"

            print(f"正在请求API: {url}")
            
            response = self._make_request_with_retry(url)
            
            if response is None:
                print(f"API请求失败，已达到最大重试次数")
                return None
                
            print(f"响应状态码: {response.status_code}")
            time.sleep(self.delay)

            data = response.json()

            if data.get('status_code') == 0 and data.get('user'):
                user_info = data['user']
                follower_count = user_info.get('follower_count', 0)
                return follower_count
            else:
                print(f"API返回错误: {data.get('status_msg', '未知错误')}")
                return None

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取粉丝数量时发生错误: {e}")
            return None

    def get_user_info(self, sec_user_id: str) -> Optional[Dict]:
        """
        获取博主基本信息

        Args:
            sec_user_id: 用户安全ID

        Returns:
            用户信息字典，失败返回None
        """
        try:
            url = f"https://www.douyin.com/aweme/v1/web/user/profile/other/?sec_user_id={sec_user_id}"

            print(f"正在请求API: {url}")
            
            response = self._make_request_with_retry(url)
            
            if response is None:
                print(f"API请求失败，已达到最大重试次数")
                return None
                
            print(f"响应状态码: {response.status_code}")
            time.sleep(self.delay)

            data = response.json()

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

                return user_data
            else:
                print(f"API返回错误: {data.get('status_msg', '未知错误')}")
                return None

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取用户信息时发生错误: {e}")
            return None
