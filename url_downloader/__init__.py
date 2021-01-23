from asyncio import gather as asyncio_gather, Semaphore, Task, Lock
from pathlib import Path, PurePath
from typing import Optional, Callable, MutableSequence
from hashlib import sha256
from urllib.parse import urlparse
from logging import getLogger
from dataclasses import dataclass

from httpx import AsyncClient, TimeoutException, Response, HTTPStatusError
from terminal_utils.log_handlers import ProgressStatus


LOG = getLogger(__name__)


@dataclass
class DownloadSummary:
    num_downloaded: int = 0
    num_duplicates: int = 0
    num_timeout: int = 0
    num_status_errors: int = 0
    num_unexpected_error: int = 0

    @property
    def num_completed(self) -> int:
        return sum((
            self.num_downloaded,
            self.num_timeout,
            self.num_status_errors,
            self.num_unexpected_error
        ))

    def __str__(self) -> str:
        return (
            f'Num downloads: {self.num_downloaded}\n'
            f'Num duplicates: {self.num_duplicates}\n'
            f'Num timeouts: {self.num_timeout}\n'
            f'Num status errors: {self.num_status_errors}\n'
            f'Num unknown errors: {self.num_unexpected_error}'
        )


async def download_urls(
    http_client: AsyncClient,
    urls: MutableSequence[str],
    output_dir: Path,
    num_concurrent: int = 10,
    use_hashing: bool = False,
    hash_function: Optional[Callable[[bytes], str]] = None,
) -> DownloadSummary:
    """
    Download resources given a list of URLs.

    :param http_client: An HTTP client with which to perform the downloads.
    :param urls: The URLs of the resources to download.
    :param output_dir: The path of the directory in which the resource files shall be saved.
    :param num_concurrent: The number of concurrent downloads.
    :param use_hashing: Whether to use the hash a resource's contents as its filename.
    :param hash_function: A callable that generates hashes. Is used only if `use_hashing` is `True`.
    :return: A summary of the status of the downloads.
    """

    download_summary = DownloadSummary()
    hash_function = hash_function or (lambda data: sha256(data).hexdigest())
    num_total_urls = len(urls)

    semaphore = Semaphore(num_concurrent)
    lock = Lock()

    def handle_response(finished_task: Task):
        LOG.debug(ProgressStatus(iteration=download_summary.num_completed, total=num_total_urls))

        try:
            response: Response = finished_task.result()
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

        if use_hashing:
            download_path: Path = output_dir / Path(hash_function(response.content))
        else:
            download_path: Path = output_dir / PurePath(urlparse(url=url).path).name

        if download_path.exists():
            LOG.warning(f'File already exists at download path: {download_path}')
            download_summary.num_duplicates += 1
        else:
            download_path.write_bytes(response.content)

        download_summary.num_downloaded += 1

    def callback(finished_task: Task):
        semaphore.release()
        handle_response(finished_task=finished_task)
        if download_summary.num_completed == num_total_urls:
            lock.release()

    running_tasks: list[Task] = []
    await lock.acquire()
    for url in urls:
        await semaphore.acquire()

        task = Task(http_client.get(url=url))
        task.add_done_callback(callback)
        running_tasks.append(task)

    await lock.acquire()

    return download_summary
