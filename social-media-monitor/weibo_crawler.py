# -*- coding: utf-8 -*-
import json
import requests
import csv
import time
import os
from typing import Dict, List, Optional


class WeiboCrawler:
    """微博用户信息爬虫类"""

    def __init__(self, cookie: str = "", delay: int = 3):
        """
        初始化爬虫

        Args:
            cookie: 微博Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.cookie = cookie
        self.delay = delay
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
            "Referer": "https://weibo.com",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        self.cookies = {"cookie": cookie} if cookie else {}

        self.data_fields = [
            'screen_name', 'description', 'followers_count', 'friends_count',
            'statuses_count', 'location', 'gender', 'verified',
            'verified_reason', 'birthday', 'created_at',
            'sunshine_credit', 'company', 'school'
        ]

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
                cookies=self.cookies,
                timeout=30
            )
            response.raise_for_status()
            time.sleep(self.delay)
            return response.text
        except requests.RequestException as e:
            print(f"请求失败: {url}, 错误: {e}")
            raise

    def extract_user_info(self, uid: str) -> Optional[Dict]:
        """
        提取用户信息

        Args:
            uid: 用户ID

        Returns:
            用户信息字典，失败返回None
        """
        try:
            url1 = f"https://weibo.com/ajax/profile/info?uid={uid}"
            url2 = f"https://weibo.com/ajax/profile/detail?uid={uid}"

            html1 = self.get_html(url1)
            html2 = self.get_html(url2)

            response1 = json.loads(html1)
            response2 = json.loads(html2)

            data1 = response1.get('data', {}).get('user', {})
            data2 = response2.get('data', {})

            if not data1:
                print(f"用户 {uid} 信息获取失败")
                return None

            user_data = {
                'screen_name': data1.get('screen_name', ''),
                'description': data1.get('description', ''),
                'followers_count': data1.get('followers_count', 0),
                'friends_count': data1.get('friends_count', 0),
                'statuses_count': data1.get('statuses_count', 0),
                'location': data1.get('location', ''),
                'gender': data1.get('gender', ''),
                'verified': data1.get('verified', False),
                'verified_reason': data1.get('verified_reason', ''),
                'birthday': data2.get('birthday', ''),
                'created_at': data2.get('created_at', ''),
                'sunshine_credit': data2.get('sunshine_credit', {}).get('level', ''),
                'company': data2.get('career', {}).get('company', ''),
                'school': data2.get('education', {}).get('school', '')
            }

            return user_data

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"提取用户信息时发生错误: {e}")
            return None

    def save_to_csv(self, data: Dict, filename: str = "weibo_users.csv"):
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

    def crawl_user(self, uid: str, save: bool = True, filename: str = "weibo_users.csv") -> Optional[Dict]:
        """
        爬取单个用户信息

        Args:
            uid: 用户ID
            save: 是否保存到文件
            filename: 保存文件名

        Returns:
            用户数据字典
        """
        print(f"正在爬取用户: {uid}")
        user_data = self.extract_user_info(uid)

        if user_data and save:
            self.save_to_csv(user_data, filename)

        return user_data

    def crawl_users(self, uid_list: List[str], filename: str = "weibo_users.csv") -> List[Dict]:
        """
        批量爬取用户信息

        Args:
            uid_list: 用户ID列表
            filename: 保存文件名

        Returns:
            用户数据列表
        """
        results = []

        for uid in uid_list:
            user_data = self.crawl_user(uid, save=True, filename=filename)
            if user_data:
                results.append(user_data)

        print(f"爬取完成，共获取 {len(results)} 条用户数据")
        return results


def main():
    """主函数"""
    print("=" * 50)
    print("微博用户信息爬虫")
    print("=" * 50)
    #替换为你的cookie
    cookie = ""
    crawler = WeiboCrawler(cookie=cookie, delay=3)
    #替换为你要爬取的微博用户ID列表
    uid_list = [
        '1669879400',
        '2803301701',
        '1739928273'
    ]

    print(f"准备爬取 {len(uid_list)} 个用户的信息...")
    results = crawler.crawl_users(uid_list, filename="weibo_users.csv")

    print("\n爬取结果:")
    for idx, user in enumerate(results, 1):
        print(f"{idx}. {user['screen_name']} - 粉丝数: {user['followers_count']}")


if __name__ == '__main__':
    main()
