from typing import Optional, Dict
import asyncio
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp.client_exceptions import ClientError
from fake_useragent import UserAgent

from scraper.interfaces import IDownloader
from scraper.utils.scraper_helpers import random_delay_async


class Downloader(IDownloader):
    def __init__(
        self,
        retry_times: int = 3,
        timeout: int = 10,
        max_connections: int = 10
    ):
        self.retry_times = retry_times
        self.timeout = timeout
        self.user_agent = UserAgent()
        self.max_connections = max_connections
        self._session: Optional[ClientSession] = None

    async def _get_session(self) -> ClientSession:
        """Get or create a session"""
        if self._session is None or self._session.closed:
            connector = TCPConnector(
                limit=self.max_connections,
                ssl=False
            )
            timeout = ClientTimeout(total=self.timeout)
            self._session = ClientSession(
                connector=connector,
                timeout=timeout
            )
        return self._session

    def _get_headers(self) -> Dict[str, str]:
        """Generate a random header"""
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        }

    async def _download_with_retry(self, url: str, retry_count: int = 0) -> str:
        """Implement detail download and retry logic"""
        try:
            session = await self._get_session()
            await random_delay_async()

            async with session.get(url, headers=self._get_headers()) as response:
                response.raise_for_status()
                return await response.text()

        except asyncio.TimeoutError:
            if retry_count < self.retry_times:
                await asyncio.sleep(2 ** retry_count)
                return await self._download_with_retry(url, retry_count + 1)
            raise DownloadException(f"Download timeout: {url}", url)

        except ClientError as e:
            if (retry_count < self.retry_times and 
                hasattr(e, 'status') and 
                e.status in {500, 502, 503, 504}):
                await asyncio.sleep(2 ** retry_count)
                return await self._download_with_retry(url, retry_count + 1)
            raise DownloadException(f"Download failed: {str(e)}", url)

        except Exception as e:
            raise DownloadException(f"Unexpected error: {str(e)}", url)

    async def download_async(self, url: str) -> Optional[str]:
        """Download asynchronously"""
        try:
            return await self._download_with_retry(url)
        except Exception as e:
            raise DownloadException(str(e), url)

    async def close(self):
        """Close the session"""
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        """Enter the async context"""
        await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context"""
        await self.close()


class DownloadException(Exception):
    """Custom exception for download errors"""
    def __init__(self, message: str, url: str):
        self.message = message
        self.url = url
        super().__init__(message)
