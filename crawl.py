# type: ignore
import os
import time
import random
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from typing import List, Tuple, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


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
        self.max_workers = config.get("max_workers", 5)
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self) -> None:
        """
        主控函数：依次对 base_urls 中的每个网站执行爬取
        """
        for url in self.base_urls:
            print(f"开始爬取: {url}")
            output_file = os.path.join(self.output_dir, f"{urlparse(url).netloc}.txt")
            with open(output_file, 'w', encoding='utf-8') as f:
                pages_crawled = self._bfs_crawl_concurrent(url, f)
            print(f"爬取完成: {url}, 共爬取 {pages_crawled} 个页面，结果已保存到 {output_file}")

    def _bfs_crawl_concurrent(self, start_url: str, file_obj) -> int:
        """
        使用多线程 + 广度优先搜索爬取网站，仅限于与 start_url 同域名的链接。
        """
        visited = set()
        queue = deque([start_url])
        page_count = 0
        base_domain = urlparse(start_url).netloc

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while queue and page_count < self.max_pages:
                # 一次取出不超过 max_workers 个链接
                batch = []
                while (queue 
                       and len(batch) < self.max_workers 
                       and page_count + len(batch) < self.max_pages):
                    url = queue.popleft()
                    if url not in visited:
                        visited.add(url)
                        batch.append(url)

                if not batch:
                    break

                print(f"[INFO] 本轮准备爬取 {len(batch)} 个页面...")

                futures = {executor.submit(self._crawl_single_page, u, base_domain): u
                           for u in batch}

                for future in as_completed(futures):
                    current_url = futures[future]
                    result = future.result()  # (page_title, page_text, new_links) or None

                    if result is None:
                        continue

                    page_title, page_text, new_links = result
                    # 主线程统一写文件，记录标题+URL
                    self._write_page(file_obj, page_title, page_text, current_url)
                    page_count += 1
                    print(f"[INFO] 已写入页面：{page_title} ({current_url})，"
                          f"目前已爬取 {page_count} 个页面")

                    # 将新发现链接加入队列
                    for link in new_links:
                        if link.lower().endswith('.pdf'):
                            print(f"[INFO] 跳过 PDF 文件：{link}")
                            continue
                        if link not in visited:
                            queue.append(link)

        return page_count

    def _crawl_single_page(self, url: str, base_domain: str
                           ) -> Optional[Tuple[str, str, List[str]]]:
        """
        单个页面的爬取与解析逻辑，供线程池调用
        """
        print(f"[THREAD] 正在爬取页面：{url}")
        response = self._fetch_page(url)
        if not response:
            return None

        # 如果 Content-Type 不包含 'text/html'，跳过
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' not in content_type:
            print(f"[INFO] 非HTML页面，跳过：{url} (Content-Type: {content_type})")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        page_title = soup.find('title').get_text(strip=True) if soup.find('title') else url

        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        page_text = soup.get_text("\n", strip=True)

        # 寻找同域名链接
        new_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            full_url = urljoin(url, href)
            if urlparse(full_url).netloc == base_domain:
                new_links.append(full_url)

        return (page_title, page_text, new_links)

    def _fetch_page(self, url: str) -> Optional[requests.Response]:
        """
        尝试多次请求页面并返回 Response，如果多次失败则返回 None
        """
        for attempt in range(self.retry_count):
            try:
                headers = self._get_headers()
                response = requests.get(url, timeout=self.timeout, headers=headers)

                if response.status_code == 200:
                    return response
                elif response.status_code in [403, 409]:
                    print(f"[WARN] 访问受限，状态码 {response.status_code}，重试中 "
                          f"({attempt + 1}/{self.retry_count})...")
                    time.sleep(random.uniform(2, 5))
                else:
                    print(f"[ERROR] 请求失败: {url}, 状态码: {response.status_code}")
                    return None
            except requests.exceptions.ReadTimeout:
                print(f"[ERROR] 请求超时: {url}，正在重试 "
                      f"({attempt + 1}/{self.retry_count})...")
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                print(f"[ERROR] 请求出错: {url}, 错误信息: {e}，正在重试 "
                      f"({attempt + 1}/{self.retry_count})...")
                time.sleep(random.uniform(1, 3))

        print(f"[ERROR] 多次重试后仍失败，跳过页面：{url}")
        return None

    def _write_page(self, file_obj, title: str, text: str, url: str) -> None:
        """
        将单个页面的标题、URL和正文内容写入文件，保留适当缩进
        """
        cleaned_title = self._clean_text(title)
        cleaned_text = self._clean_text(text)
        # 将 "标题 (URL):" 作为标识
        file_obj.write(f"{cleaned_title} ({url}):\n")
        text_indented = "    " + cleaned_text.replace("\n", "\n    ")
        file_obj.write(f"{text_indented}\n\n")

    @staticmethod
    def _clean_text(input_text: str) -> str:
        """
        删除常见的控制字符（ASCII 0~31, 127 等）
        """
        control_chars = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]+')
        return control_chars.sub('', input_text)

    @staticmethod
    def _get_headers() -> Dict:
        """
        返回随机 User-Agent 请求头
        """
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
        ]
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;"
                      "q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

