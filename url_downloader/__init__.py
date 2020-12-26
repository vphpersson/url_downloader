from asyncio import gather as asyncio_gather
from pathlib import Path, PurePath
from typing import Optional, Callable, MutableSequence
from hashlib import sha256
from urllib.parse import urlparse
from logging import getLogger
from dataclasses import dataclass

from httpx import AsyncClient, TimeoutException
from terminal_utils.log_handlers import ProgressStatus


LOG = getLogger(__name__)


@dataclass
class DownloadSummary:
    num_downloaded: int = 0
    num_duplicates: int = 0
    num_timeout: int = 0
    num_unexpected_error: int = 0

    def __str__(self) -> str:
        return (
            f'Num downloads: {self.num_downloaded}\n'
            f'Num duplicates: {self.num_duplicates}\n'
            f'Num timeouts: {self.num_timeout}\n'
            f'Num unknown errors: {self.num_unexpected_error}'
        )


async def download_urls(
    client: AsyncClient,
    urls: MutableSequence[str],
    output_dir: Path,
    num_concurrent: int = 5,
    use_hashing: bool = False,
    hash_function: Optional[Callable[[bytes], str]] = None,
) -> DownloadSummary:
    """
    Download resources given a list of URLs.

    :param client: An HTTP client with which to perform the downloads.
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

    async def work() -> None:
        while True:
            LOG.debug(
                ProgressStatus(
                    iteration=sum((
                        download_summary.num_downloaded,
                        download_summary.num_timeout,
                        download_summary.num_unexpected_error
                    )),
                    total=num_total_urls
                )
            )

            try:
                url: str = urls.pop()
            except IndexError:
                break

            try:
                response = await client.get(url=url)
                response.raise_for_status()
            except TimeoutException:
                LOG.warning(f'Timed out: {url}')
                download_summary.num_timeout += 1
                continue
            except:
                LOG.exception(f'Unexpected error: {url}')
                download_summary.num_unexpected_error += 1
                continue

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

    try:
        await asyncio_gather(*(work() for _ in range(min(num_total_urls, num_concurrent))))
    except KeyboardInterrupt:
        pass

    return download_summary
