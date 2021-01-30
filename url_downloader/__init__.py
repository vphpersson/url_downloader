from asyncio import Semaphore, Task, Lock
from typing import Callable, Collection, Any

from httpx import AsyncClient


async def download_urls(
    http_client: AsyncClient,
    urls: Collection[str],
    response_callback: Callable[[str, Task], Any],
    num_concurrent: int = 5,
) -> None:
    """
    Download resources given a list of URLs.

    :param http_client: An HTTP client with which to perform the downloads.
    :param urls: The URLs of the resources to download.
    :param response_callback: A callback function that receives the response of an HTTP request for a resource.
    :param num_concurrent: The number of concurrent downloads.
    :return: A summary of the status of the downloads.
    """

    if not urls:
        return

    request_limiting_semaphore = Semaphore(num_concurrent)
    all_finished_lock = Lock()
    num_completed = 0

    def task_done_callback(finished_task: Task) -> None:
        request_limiting_semaphore.release()
        response_callback(finished_task.get_name(), finished_task)
        nonlocal num_completed
        num_completed += 1
        if num_completed == len(urls):
            all_finished_lock.release()

    await all_finished_lock.acquire()
    for url in urls:
        await request_limiting_semaphore.acquire()
        Task(coro=http_client.get(url=url), name=url).add_done_callback(task_done_callback)

    await all_finished_lock.acquire()
