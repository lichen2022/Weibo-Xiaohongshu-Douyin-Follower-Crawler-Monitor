# -*- coding: utf-8 -*-
import json
import requests
import csv
import time
import os
import re
from typing import Dict, Optional, List


class XiaohongshuCrawler:
    """小红书博主信息爬虫类"""

    def __init__(self, cookie: str = "", delay: int = 2):
        """
        初始化爬虫

        Args:
            cookie: 小红书Cookie，用于身份验证
            delay: 请求间隔时间（秒），防止被反爬
        """
        self.cookie = cookie
        self.delay = delay
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.xiaohongshu.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Origin": "https://www.xiaohongshu.com"
        }
        self.cookies = {"cookie": cookie} if cookie else {}

        self.data_fields = [
            'nickname', 'user_id', 'following_count', 'follower_count',
            'liked_count', 'collected_count', 'ip_location', 'description',
            'gender', 'verified', 'verified_reason', 'note_count', 'avatar'
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

    def extract_user_info(self, profile_url: str) -> Optional[Dict]:
        """
        从小红书博主主页提取用户信息

        Args:
            profile_url: 博主主页URL

        Returns:
            用户信息字典，失败返回None
        """
        try:
            html = self.get_html(profile_url)

            user_data = {
                'nickname': '',
                'user_id': '',
                'following_count': 0,
                'follower_count': 0,
                'liked_count': 0,
                'collected_count': 0,
                'ip_location': '',
                'description': '',
                'gender': '',
                'verified': False,
                'verified_reason': '',
                'note_count': 0,
                'avatar': ''
            }

            pattern = r'__INITIAL_STATE__\s*=\s*({.+?});'
            match = re.search(pattern, html)

            if match:
                json_str = match.group(1)
                data = json.loads(json_str)

                user_info = None

                possible_paths = [
                    data.get('user', {}).get('userPageData', {}).get('user', {}),
                    data.get('user', {}).get('user', {}),
                    data.get('userPageData', {}).get('user', {}),
                    data.get('note', {}).get('noteDetail', {}).get('user', {}),
                ]

                for path in possible_paths:
                    if path and path.get('nickname'):
                        user_info = path
                        break

                if user_info:
                    user_data['nickname'] = user_info.get('nickname', '')
                    user_data['user_id'] = user_info.get('user_id', '')
                    user_data['following_count'] = user_info.get('follows', 0)
                    user_data['follower_count'] = user_info.get('fans', 0)
                    user_data['liked_count'] = user_info.get('likedCount', 0)
                    user_data['collected_count'] = user_info.get('collectedCount', 0)
                    user_data['ip_location'] = user_info.get('ipLocation', '')
                    user_data['description'] = user_info.get('desc', '')
                    user_data['gender'] = user_info.get('gender', '')
                    user_data['verified'] = user_info.get('officialVerify', {}).get('type', 0) > 0
                    user_data['verified_reason'] = user_info.get('officialVerify', {}).get('desc', '')
                    user_data['note_count'] = user_info.get('notes', 0)
                    user_data['avatar'] = user_info.get('avatar', '')

                    return user_data

            og_title_pattern = r'<meta name="og:title" content="([^"]+)"'
            og_title_match = re.search(og_title_pattern, html)
            if og_title_match:
                og_title = og_title_match.group(1)
                nickname = og_title.split(' - 小红书')[0] if ' - 小红书' in og_title else og_title
                user_data['nickname'] = nickname

            following_patterns = [
                r'(\d+)\s*关注',
                r'关注\s*(\d+)',
                r'follows["\s:]+(\d+)',
                r'关注["\s:]+(\d+)',
                r'(\d+)\s*个关注',
                r'关注\s*(\d+)\s*个',
            ]
            for pattern in following_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    user_data['following_count'] = int(match.group(1))
                    break

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
                        user_data['follower_count'] = int(float(follower_str.replace('万', '')) * 10000)
                    else:
                        user_data['follower_count'] = int(follower_str)
                    break

            liked_patterns = [
                r'(\d+(?:\.\d+)?)\s*万?\s*(?:获赞与收藏|获赞|收藏)',
                r'(?:获赞与收藏|获赞|收藏)\s*(\d+(?:\.\d+)?万?)',
                r'(\d+)(?:万+)?获赞与收藏',
                r'likedCount["\s:]+(\d+)',
                r'获赞与收藏["\s:]+(\d+)',
                r'(\d+)\s*万\s*获赞与收藏',
                r'(\d+)\s*个获赞与收藏',
            ]
            for pattern in liked_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    liked_str = match.group(1)
                    if '万' in liked_str:
                        user_data['liked_count'] = int(float(liked_str.replace('万', '')) * 10000)
                    else:
                        user_data['liked_count'] = int(liked_str)
                    break

            ip_patterns = [
                r'IP属地[：:]\s*([^<\s]+)',
                r'IP属地[：:]\s*([^<\u4e00-\u9fa5]+)',
            ]
            for pattern in ip_patterns:
                match = re.search(pattern, html)
                if match:
                    user_data['ip_location'] = match.group(1).strip()
                    break

            desc_pattern = r'<meta name="description" content="([^"]+)"'
            desc_match = re.search(desc_pattern, html)
            if desc_match:
                user_data['description'] = desc_match.group(1)
                desc = user_data['description']
                if '有' in desc and '位粉丝' in desc:
                    fans_match = re.search(r'有(\d+)位粉丝', desc)
                    if fans_match:
                        user_data['follower_count'] = int(fans_match.group(1))

            user_id_pattern = r'user/profile/([a-f0-9]+)'
            user_id_match = re.search(user_id_pattern, profile_url)
            if user_id_match:
                user_data['user_id'] = user_id_match.group(1)

            note_count_patterns = [
                r'笔记\s*(\d+)',
                r'(\d+)\s*笔记',
                r'notes["\s:]+(\d+)',
                r'笔记["\s:]+(\d+)',
                r'(\d+)\s*篇笔记',
                r'笔记数["\s:]+(\d+)',
            ]
            for pattern in note_count_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    user_data['note_count'] = int(match.group(1))
                    break

            return user_data

        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"提取用户信息时发生错误: {e}")
            return None

    def save_to_csv(self, data: Dict, filename: str = "xiaohongshu_users.csv"):
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

    def crawl_user(self, profile_url: str, save: bool = True, filename: str = "xiaohongshu_users.csv") -> Optional[Dict]:
        """
        爬取单个博主信息

        Args:
            profile_url: 博主主页URL
            save: 是否保存到文件
            filename: 保存文件名

        Returns:
            用户数据字典
        """
        print(f"正在爬取博主: {profile_url}")
        user_data = self.extract_user_info(profile_url)

        if user_data and save:
            self.save_to_csv(user_data, filename)

        return user_data

    def crawl_users(self, url_list: List[str], filename: str = "xiaohongshu_users.csv") -> List[Dict]:
        """
        批量爬取博主信息

        Args:
            url_list: 博主主页URL列表
            filename: 保存文件名

        Returns:
            用户数据列表
        """
        results = []

        for url in url_list:
            user_data = self.crawl_user(url, save=True, filename=filename)
            if user_data:
                results.append(user_data)

        print(f"爬取完成，共获取 {len(results)} 条博主数据")
        return results


def main():
    """主函数"""
    print("=" * 50)
    print("小红书博主信息爬虫")
    print("=" * 50)
    #替换为你的cookie
    cookie = ""
    crawler = XiaohongshuCrawler(cookie=cookie, delay=2)
    #替换为你要爬取的小红书博主主页URL：格式为https://www.xiaohongshu.com/user/profile/xxx或https://xhslink.com/m/xxx
    url_list = [
        'https://www.xiaohongshu.com/user/profile/584646cd82ec390d801d2816?xsec_token=YBhXdx4DWypBHqJMJLFPyoOvHSXvLJ_LigBECqBQAqzXU=&xsec_source=app_share&xhsshare=CopyLink&shareRedId=ODk2Q0RLO042NzUyOTgwNjc8OThJRjpK&apptime=1769594018&share_id=9bd8eeecb84746dcba226910d526a4df'
       ,'https://xhslink.com/m/1lvePZ4Ge9k'
    ]

    print(f"准备爬取 {len(url_list)} 个博主的信息...")
    results = crawler.crawl_users(url_list, filename="xiaohongshu_users.csv")

    print("\n爬取结果:")
    for idx, user in enumerate(results, 1):
        print(f"{idx}. 昵称: {user['nickname']}")
        print(f"   关注数: {user['following_count']}")
        print(f"   粉丝数: {user['follower_count']}")
        #print(f"   最新一条帖子获赞: {user['liked_count']}") #数量没过万显示具体数量，如数量过万显示的数字单位为w（如：1表示1w）
        print(f"   IP属地: {user['ip_location']}")
        print()

    #print("=" * 50)
    #print("完整JSON数据:")
    #print("=" * 50)
    #print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
