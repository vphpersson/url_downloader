from asyncio import Semaphore, Task, Lock
from typing import Callable, Collection, Any
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
    urls: Collection[str],
    response_callback: Callable[[Response], Any],
    num_concurrent: int = 5,
    raise_for_status: bool = True
) -> DownloadSummary:
    """
    Download resources given a list of URLs.

    :param http_client: An HTTP client with which to perform the downloads.
    :param urls: The URLs of the resources to download.
    :param response_callback: A callback function that receives the response of an HTTP request for a resource.
    :param num_concurrent: The number of concurrent downloads.
    :param raise_for_status: Whether to raise an exception if the response status does not indicate success.
    :return: A summary of the status of the downloads.
    """

    download_summary = DownloadSummary()

    request_limiting_semaphore = Semaphore(num_concurrent)
    all_finished_lock = Lock()

    def handle_response(finished_task: Task) -> None:
        LOG.debug(ProgressStatus(iteration=download_summary.num_completed, total=len(urls)))

        try:
            response: Response = finished_task.result()
            if raise_for_status:
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

        response_callback(response)
        download_summary.num_downloaded += 1

    def task_done_callback(finished_task: Task) -> None:
        request_limiting_semaphore.release()
        handle_response(finished_task=finished_task)
        if download_summary.num_completed == len(urls):
            all_finished_lock.release()

    await all_finished_lock.acquire()
    for url in urls:
        await request_limiting_semaphore.acquire()

        task = Task(http_client.get(url=url))
        task.add_done_callback(task_done_callback)

    await all_finished_lock.acquire()

    return download_summary
