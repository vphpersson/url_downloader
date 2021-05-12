from asyncio import Task
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path, PurePath
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlparse
from hashlib import sha256
from collections.abc import Collection

from httpx import AsyncClient, Response, TimeoutException, HTTPStatusError
from httpx._types import URLTypes
from terminal_utils.log_handlers import ProgressStatus
from pyutils.asyncio import limited_gather

from url_downloader.cli import URLDownloaderArgumentParser

LOG = getLogger(__name__)


@dataclass
class DownloadSummary:
    num_downloaded: int = 0
    num_duplicates: int = 0
    num_timeout: int = 0
    num_status_errors: int = 0
    num_unexpected_error: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def num_completed(self) -> int:
        return sum((
            self.num_downloaded,
            self.num_timeout,
            self.num_status_errors,
            self.num_unexpected_error
        ))

    @property
    def elapsed_time(self) -> Optional[timedelta]:
        return self.end_time - self.start_time if self.start_time and self.end_time else None

    def __str__(self) -> str:
        return (
            f'Num downloads: {self.num_downloaded}\n'
            f'Num duplicates: {self.num_duplicates}\n'
            f'Num timeouts: {self.num_timeout}\n'
            f'Num status errors: {self.num_status_errors}\n'
            f'Num unknown errors: {self.num_unexpected_error}'
        )


async def download_urls(
    urls: Collection[URLTypes],
    output_directory: Path,
    http_client: AsyncClient,
    use_hashing: bool = False,
    num_concurrent: int = 5
) -> DownloadSummary:
    """
    Download resources over HTTP and write them to a directory.

    :param urls: The URLs of the resources to be downloaded.
    :param output_directory: The directory where the resources are to be written.
    :param http_client: An HTTP client with which to retrieve the resources.
    :param use_hashing: Whether to name the resources by their hash value when writing them to the output directory.
    :param num_concurrent: The number of concurrent requests for resources.
    :return: A summary describing the status of the download job.
    """

    download_summary = DownloadSummary()

    def result_callback(response_task: Task, url: str) -> None:
        LOG.debug(ProgressStatus(iteration=download_summary.num_completed, total=len(urls)))

        try:
            response: Response = response_task.result()
            response.raise_for_status()
        except TimeoutException:
            LOG.warning(f'Timed out: {url}')
            download_summary.num_timeout += 1
            return
        except HTTPStatusError as e:
            LOG.warning(e.args[0].splitlines()[0])
            download_summary.num_status_errors += 1
            return
        except:
            LOG.exception(f'Unexpected error: {url}')
            download_summary.num_unexpected_error += 1
            return

        download_summary.num_downloaded += 1

        if use_hashing:
            download_path: Path = output_directory / Path(sha256(response.content).hexdigest())
        else:
            download_path: Path = output_directory / PurePath(urlparse(url=url).path).name

        if download_path.exists():
            LOG.warning(f'File already exists at download path: {download_path}')
            download_summary.num_duplicates += 1
        else:
            download_path.write_bytes(response.content)

    download_summary.start_time = datetime.now()

    await limited_gather(
        iteration_coroutine=http_client.get,
        iterable=urls,
        result_callback=result_callback,
        num_concurrent=num_concurrent
    )

    download_summary.end_time = datetime.now()

    return download_summary
