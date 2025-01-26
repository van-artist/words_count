# type: ignore
import os
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from typing import List, Tuple, Dict, Optional


class WebCrawler:
    def __init__(self, config: Dict):
        """
        初始化爬虫配置
        """
        self.base_urls = config.get("base_urls", [])
        self.max_pages = config.get("max_pages", 30)
        self.timeout = config.get("timeout", 10)
        self.output_dir = config.get("output_dir", "./results")
        self.retry_count = config.get("retry_count", 3)
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self) -> None:
        """
        主控函数：依次对 base_urls 中的每个网站执行爬取
        """
        for url in self.base_urls:
            print(f"开始爬取: {url}")
            # 以域名命名输出文件
            output_file = os.path.join(self.output_dir, f"{urlparse(url).netloc}.txt")

            # 以写模式打开文件，进行边爬边写
            with open(output_file, 'w', encoding='utf-8') as f:
                # 执行 BFS 爬虫逻辑
                pages_crawled = self._bfs_crawl(url, f)
            print(f"爬取完成: {url}, 共爬取 {pages_crawled} 个页面，结果已保存到 {output_file}")

    def _bfs_crawl(self, start_url: str, file_obj) -> int:
        """
        使用广度优先搜索爬取网站，仅限于与 start_url 同域名的链接。
        :param start_url: 起始 URL
        :param file_obj: 已打开的文件对象，用于写入页面内容
        :return: 爬取到的页面数量
        """
        visited = set()
        queue = deque([start_url])
        page_count = 0

        # 获取起始域名（例如 www.jedc.org）
        base_domain = urlparse(start_url).netloc

        while queue and page_count < self.max_pages:
            current_url = queue.popleft()

            if current_url in visited:
                continue
            visited.add(current_url)

            print(f"[INFO] 正在爬取页面：{current_url}")

            # 请求页面
            response = self._fetch_page(current_url)
            if not response:
                continue

            # 解析页面
            soup = BeautifulSoup(response.text, 'html.parser')
            page_title = soup.find('title').get_text(strip=True) if soup.find('title') else current_url

            # 清理脚本与样式等标签
            for tag in soup(["script", "style", "noscript"]):
                tag.extract()
            page_text = soup.get_text("\n", strip=True)

            # 将本页面写入输出文件
            self._write_page(file_obj, page_title, page_text)
            page_count += 1
            print(f"[INFO] 已写入页面：{page_title} ({current_url})")

            # 寻找下一层链接
            a_tags = soup.find_all('a', href=True)
            for a_tag in a_tags:
                href = a_tag['href'].strip()
                full_url = urljoin(current_url, href)
                # 只爬取同域名的链接
                if urlparse(full_url).netloc == base_domain and full_url not in visited:
                    queue.append(full_url)
                    print(f"[INFO] 发现链接：{full_url}，已加入队列")

        print(f"[INFO] 域名 {base_domain} 的爬取结束，共爬取页面数：{page_count}")
        return page_count

    def _fetch_page(self, url: str) -> Optional[requests.Response]:
        """
        尝试多次请求页面并返回 Response，如果多次失败则返回 None
        """
        for attempt in range(self.retry_count):
            try:
                headers = self._get_headers()
                response = requests.get(url, timeout=self.timeout, headers=headers)

                # 如果状态码为 200，则返回成功的响应
                if response.status_code == 200:
                    return response
                # 如果状态码是 403 或 409，可以重试
                elif response.status_code in [403, 409]:
                    print(f"[WARN] 访问受限，状态码 {response.status_code}，重试中 ({attempt + 1}/{self.retry_count})...")
                    time.sleep(random.uniform(2, 5))
                else:
                    print(f"[ERROR] 请求失败: {url}, 状态码: {response.status_code}")
                    return None
            except requests.exceptions.ReadTimeout:
                print(f"[ERROR] 请求超时: {url}，正在重试 ({attempt + 1}/{self.retry_count})...")
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                print(f"[ERROR] 请求出错: {url}, 错误信息: {e}，正在重试 ({attempt + 1}/{self.retry_count})...")
                time.sleep(random.uniform(1, 3))

        # 如果多次尝试都失败，则返回 None
        print(f"[ERROR] 多次重试后仍失败，跳过页面：{url}")
        return None

    def _write_page(self, file_obj, title: str, text: str) -> None:
        """
        将单个页面的标题和正文内容写入文件，保留适当缩进
        """
        file_obj.write(f"{title}:\n")
        text_indented = "    " + text.replace("\n", "\n    ")
        file_obj.write(f"{text_indented}\n\n")

    @staticmethod
    def _get_headers() -> Dict:
        """
        返回随机 User-Agent 请求头
        """
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
        ]
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
