#!/usr/bin/env python3

from argparse import ArgumentParser, Action, Namespace, FileType, ArgumentTypeError
from pathlib import Path, PurePath
from typing import List, MutableSet, Optional, Dict, Any, Callable, MutableSequence
from hashlib import sha256
from io import TextIOWrapper
from sys import stdin
from asyncio import run as asyncio_run, gather as asyncio_gather, ensure_future as asyncio_ensure_future, TimeoutError
from urllib.parse import urlparse
from logging import getLogger, Handler, CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET, LogRecord
from traceback import format_exc
from dataclasses import dataclass, asdict
from datetime import datetime

from aiohttp import ClientSession, ClientTimeout
from terminal_utils.Progressor import Progressor
from terminal_utils.ColoredOutput import ColoredOutput


LOG = getLogger(__name__)


@dataclass
class DownloadSummary:
    num_downloaded: int = 0
    num_duplicates: int = 0
    num_timeout: int = 0
    num_unknown_error: int = 0

    def __str__(self) -> str:
        return (
            f'Num downloads: {self.num_downloaded}\n'
            f'Num duplicates: {self.num_duplicates}\n'
            f'Num timeouts: {self.num_timeout}\n'
            f'Num unknown errors: {self.num_unknown_error}'
        )


async def download_url(
    urls: MutableSequence[str],
    output_dir: Path,
    num_concurrent: int = 5,
    use_hashing: bool = False,
    hash_function: Optional[Callable[[bytes], str]] = None,
    *,
    client_options: Optional[Dict[str, Any]] = None
) -> DownloadSummary:
    """
    Download resources given a list of URLs.

    :param urls: The URLs of the resources to download.
    :param output_dir: The path of the directory in which the resource files shall be saved.
    :param num_concurrent: The number of concurrent downloads.
    :param use_hashing: Whether to use the hash a resource's contents as its filename.
    :param hash_function: A callable that generates hashes. Is used only if `use_hashing` is `True`.
    :param client_options: Options to be passed to `aiohttp.ClientSession`.
    :return: A summary of the status of the downloads.
    """

    download_summary = DownloadSummary()
    hash_function = hash_function or (lambda data: sha256(data).hexdigest())
    client_options = client_options or {}
    num_total_urls = len(urls)

    async def work() -> None:
        async with ClientSession(**client_options) as session:
            while len(urls) != 0:
                LOG.debug({**dict(num_total_urls=num_total_urls), **asdict(download_summary)})
                url: str = urls.pop()
                if not url:
                    continue

                try:
                    async with session.get(url=url) as response:
                        response_data: bytes = await response.read()
                except TimeoutError:
                    LOG.warning(f'Timed out: {url}')
                    download_summary.num_timeout += 1
                    continue
                except Exception:
                    LOG.error(f'Unknown error ({url}): {format_exc()}')
                    download_summary.num_unknown_error += 1
                    continue

                if use_hashing:
                    download_path: Path = output_dir / Path(hash_function(response_data))
                else:
                    download_path: Path = output_dir / PurePath(urlparse(url=url).path).name

                if download_path.exists():
                    LOG.warning(f'File already exists at download path: {download_path}')
                    download_summary.num_duplicates += 1
                else:
                    download_path.write_bytes(response_data)

                download_summary.num_downloaded += 1

    try:
        await asyncio_gather(*(asyncio_ensure_future(work()) for _ in range(min(num_total_urls, num_concurrent))))
    except KeyboardInterrupt:
        pass

    return download_summary


class ParseUrlsAction(Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        urls: List[Path],
        option_string: Optional[str] = None
    ) -> None:

        all_urls: MutableSet[str] = namespace.all_urls if hasattr(namespace, 'all_urls') else set()
        all_urls.update(urls)

        setattr(namespace, self.dest, urls)
        namespace.all_urls = all_urls


class ParseUrlsFilesAction(Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        urls_files: List[TextIOWrapper],
        option_string: Optional[str] = None
    ) -> None:

        all_urls: MutableSet[str] = namespace.all_urls if hasattr(namespace, 'all_urls') else set()

        all_urls.update(
            url.strip()
            for urls_file in urls_files
            for url in urls_file.read().splitlines()
        )

        setattr(namespace, self.dest, urls_files)
        namespace.all_urls = all_urls


class ParseOutputDirectoryAction(Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        output_directory: Path,
        option_string: Optional[str] = None
    ) -> None:

        if not output_directory.exists():
            raise ArgumentTypeError(f'The output directory "{output_directory}" does not exist.')
        setattr(namespace, self.dest, output_directory)


def get_parser() -> ArgumentParser:
    parser = ArgumentParser()

    parser.add_argument(
        '-u', '--urls',
        help='URLs of resources to be downloaded.',
        dest='urls',
        metavar='URL',
        type=str,
        nargs='+',
        default=[],
        action=ParseUrlsAction
    )

    parser.add_argument(
        '-U', '--urls-files',
        help='File path of files containing rows of URLs of resources to be downloaded.',
        dest='urls_files',
        metavar='URLS_FILE',
        type=FileType(mode='r'),
        nargs='+',
        default=[],
        action=ParseUrlsFilesAction
    )

    parser.add_argument(
        '-x', '--use-hashing',
        help="Let the filenames of downloaded resources' be a sha256 hash of their contents.",
        dest='use_hashing',
        action='store_true'
    )

    parser.add_argument(
        '-n', '--num-concurrent',
        help='The number of concurrent downloads to be performed.',
        dest='num_concurrent',
        type=int,
        default=5
    )

    parser.add_argument(
        '-t', '--timeout',
        help='The total amount of time in seconds to wait for one download to finish before timing out.',
        dest='num_total_timeout_seconds',
        type=int,
        default=60
    )

    parser.add_argument(
        '-w', '--ignore-warnings',
        help='Ignore warning messages.',
        dest='ignore_warnings',
        action='store_true'
    )

    parser.add_argument(
        '-q', '--quiet',
        help='Do not print warning messages, error messages, or the result summary.',
        dest='quiet',
        action='store_true'
    )

    parser.add_argument(
        '-o', '--output-dir',
        help='A path to a directory where downloaded resources are to be saved.',
        dest='output_directory',
        type=Path,
        required=True
    )

    return parser


class DownloadUrlProgressLogHandler(Handler):

    def __init__(self, ignore_warnings: bool = False, level=NOTSET):
        super().__init__(level=level)
        self._progressor = Progressor()
        self._colored_output = ColoredOutput()
        self.ignore_warnings = ignore_warnings

    def emit(self, record: LogRecord):
        if record.levelno in {CRITICAL, ERROR}:
            self._progressor.print_message(message=self._colored_output.print_red(message=record.msg))
        elif record.levelno == WARNING:
            if not self.ignore_warnings:
                self._progressor.print_message(message=self._colored_output.print_yellow(message=record.msg))
        elif record.levelno == INFO:
            self._progressor.print_message(message=record.msg)
        elif record.levelno == DEBUG:
            num_total = record.msg.pop('num_total_urls')
            download_summary = DownloadSummary(**record.msg)
            self._progressor.print_progress(
                iteration=sum((
                    download_summary.num_downloaded,
                    download_summary.num_timeout,
                    download_summary.num_unknown_error
                )),
                total=num_total
            )
        else:
            raise ValueError(f'Unknown log level: levelno={record.levelno}')

    def flush_progress(self):
        self._progressor.print_progress_message('')


def main():
    args = get_parser().parse_args()

    if not args.urls and not args.urls_files:
        args.all_urls = set(stdin.read().splitlines())

    if not args.quiet:
        handler = DownloadUrlProgressLogHandler(ignore_warnings=args.ignore_warnings)
        LOG.addHandler(handler)
        LOG.setLevel(level=DEBUG)

    num_urls = len(args.all_urls)

    start_time: datetime = datetime.now()
    download_summary: DownloadSummary = asyncio_run(
        download_url(
            urls=list(args.all_urls),
            output_dir=args.output_directory,
            use_hashing=args.use_hashing,
            num_concurrent=args.num_concurrent,
            client_options=dict(
                timeout=ClientTimeout(total=args.num_total_timeout_seconds)
            )
        )
    )
    end_time: datetime = datetime.now()

    if not args.quiet:
        handler.flush_progress()
        print(
            f'Elapsed time: {end_time - start_time}\n'
            f'Num URLs: {num_urls}\n'
            f'{download_summary}'
        )


if __name__ == '__main__':
    main()
