# -*- coding: utf-8 -*-
import json
import csv
import time
import os
import re
import hashlib
import random
import string
from typing import Dict, Optional, List
import requests


class DouyinAPICrawler:
    """抖音博主信息爬虫类（使用API）"""

    def __init__(self, cookie: str = "", delay: int = 2):
        """
        初始化爬虫

        Args:
            cookie: 抖音Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.cookie = cookie
        self.delay = delay
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.douyin.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Origin": "https://www.douyin.com"
        }
        self.cookies = {"cookie": cookie} if cookie else {}

        self.data_fields = [
            'nickname', 'user_id', 'following_count', 'follower_count',
            'aweme_count', 'total_favorited', 'ip_location', 'signature',
            'gender', 'verified', 'verified_reason', 'avatar'
        ]

    def generate_signature(self, url: str) -> str:
        """
        生成X-Bogus签名（简化版本）

        Args:
            url: 请求URL

        Returns:
            签名字符串
        """
        timestamp = str(int(time.time() * 1000))
        random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        
        sign_str = f"{url}{timestamp}{random_str}"
        signature = hashlib.md5(sign_str.encode()).hexdigest()
        
        return f"{signature};{timestamp};{random_str}"

    def get_user_info_by_api(self, sec_user_id: str) -> Optional[Dict]:
        """
        通过API获取用户信息

        Args:
            sec_user_id: 用户安全ID

        Returns:
            用户信息字典，失败返回None
        """
        try:
            url = f"https://www.douyin.com/aweme/v1/web/user/profile/other/?sec_user_id={sec_user_id}"
            
            signature = self.generate_signature(url)
            self.headers["X-Bogus"] = signature
            
            print(f"正在请求API: {url}")
            response = requests.get(
                url,
                headers=self.headers,
                cookies=self.cookies,
                timeout=30
            )
            print(f"响应状态码: {response.status_code}")
            
            response.raise_for_status()
            time.sleep(self.delay)
            
            data = response.json()
            
            if data.get('status_code') == 0 and data.get('user'):
                user_info = data['user']
                
                user_data = {
                    'nickname': user_info.get('nickname', ''),
                    'user_id': user_info.get('uid', ''),
                    'following_count': user_info.get('following_count', 0),
                    'follower_count': user_info.get('follower_count', 0),
                    'aweme_count': user_info.get('aweme_count', 0),
                    'total_favorited': user_info.get('total_favorited', 0),
                    'ip_location': user_info.get('ip_location', ''),
                    'signature': user_info.get('signature', ''),
                    'gender': user_info.get('gender', ''),
                    'verified': user_info.get('custom_verify', '') != '' or user_info.get('enterprise_verify_reason', '') != '',
                    'verified_reason': user_info.get('custom_verify', '') or user_info.get('enterprise_verify_reason', ''),
                    'avatar': user_info.get('avatar_thumb', {}).get('url_list', [''])[0] if user_info.get('avatar_thumb') else ''
                }
                
                print(f"成功获取用户信息 - 昵称: {user_data['nickname']}, 粉丝: {user_data['follower_count']}, 关注: {user_data['following_count']}")
                return user_data
            else:
                print(f"API返回错误: {data.get('status_msg', '未知错误')}")
                return None
                
        except requests.RequestException as e:
            print(f"API请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取用户信息时发生错误: {e}")
            import traceback
            traceback.print_exc()
            return None

    def save_to_csv(self, data: Dict, filename: str = "douyin_users.csv"):
        """
        将数据保存到CSV文件

        Args:
            data: 用户数据字典
            filename: 输出文件名
        """
        try:
            file_exists = os.path.isfile(filename)

            with open(filename, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.data_fields)

                if not file_exists:
                    writer.writeheader()

                writer.writerow(data)

            print(f"数据已保存到 {filename}")

        except IOError as e:
            print(f"文件写入失败: {e}")

    def crawl_user(self, sec_user_id: str, save: bool = True, filename: str = "douyin_users.csv") -> Optional[Dict]:
        """
        爬取单个博主信息

        Args:
            sec_user_id: 用户安全ID
            save: 是否保存到文件
            filename: 保存文件名

        Returns:
            用户数据字典
        """
        print(f"正在爬取博主: {sec_user_id}")
        user_data = self.get_user_info_by_api(sec_user_id)

        if user_data and save:
            self.save_to_csv(user_data, filename)

        return user_data

    def crawl_users(self, sec_user_id_list: List[str], filename: str = "douyin_users.csv") -> List[Dict]:
        """
        批量爬取博主信息

        Args:
            sec_user_id_list: 用户安全ID列表
            filename: 保存文件名

        Returns:
            用户数据列表
        """
        results = []

        for sec_user_id in sec_user_id_list:
            user_data = self.crawl_user(sec_user_id, save=True, filename=filename)
            if user_data:
                results.append(user_data)

        print(f"爬取完成，共获取 {len(results)} 条博主数据")
        return results


def main():
    """主函数"""
    print("=" * 50)
    print("抖音博主信息爬虫（API版）")
    print("=" * 50)
    #替换为你的cookie
    cookie = ""

    crawler = DouyinAPICrawler(cookie=cookie, delay=2)
    #替换为你要爬取的抖音博主sec_user_id列表
    sec_user_id_list = [
        'MS4wLjABAAAAGHyQcEXksWc_qdQ7RLiwMMnrOaqYYC0mB4xvzF2_28E'
    ]
    
    print("使用说明:")
    print("1. 在此列表中添加要爬取的抖音博主sec_user_id")
    print("2. sec_user_id可以从抖音主页URL中获取，格式为: https://www.douyin.com/user/MS4wLjABAAAAGHyQcEXksWc_qdQ7RLiwMMnrOaqYYC0mB4xvzF2_28E")
    print("3. 也可以从分享链接中提取，格式为: https://v.douyin.com/xxxxx")
    print()

    print(f"准备爬取 {len(sec_user_id_list)} 个博主的信息...")
    results = crawler.crawl_users(sec_user_id_list, filename="douyin_users.csv")

    print("\n爬取结果:")
    for idx, user in enumerate(results, 1):
        print(f"{idx}. 昵称: {user['nickname']}")
        print(f"   关注数: {user['following_count']}")
        print(f"   粉丝数: {user['follower_count']}")
        print(f"   获赞数: {user['total_favorited']}")
        print(f"   作品数: {user['aweme_count']}")
        print()

    #print("=" * 50)
    #print("完整JSON数据:")
    #print("=" * 50)
    #print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
