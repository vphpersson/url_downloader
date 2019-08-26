#!/usr/bin/env python3

from argparse import ArgumentParser, Action, Namespace, FileType, ArgumentTypeError
from pathlib import Path, PurePath
from typing import List, MutableSet, Optional, Dict, Any, Tuple, Callable
from hashlib import sha256
from io import TextIOWrapper
from sys import stdin, stderr
from asyncio import run as asyncio_run, gather as asyncio_gather, ensure_future as asyncio_ensure_future, TimeoutError
from urllib.parse import urlparse
from logging import getLogger, Formatter, StreamHandler
from traceback import format_exc
from time import time

from aiohttp import ClientSession, ClientTimeout

LOG = getLogger(__name__)


async def download_documents(
    urls: List[str],
    output_dir: Path,
    num_concurrent: int = 5,
    use_hashing: bool = False,
    hash_function: Optional[Callable[[bytes], str]] = None,
    *,
    client_options: Optional[Dict[str, Any]] = None
) -> Tuple[int, int, int, int]:

    num_downloaded = 0
    num_duplicates = 0
    num_timeout = 0
    num_unknown_error = 0
    hash_function = hash_function or (lambda data: sha256(data).hexdigest())

    async def work() -> None:

        nonlocal num_downloaded
        nonlocal num_duplicates
        nonlocal num_timeout
        nonlocal num_unknown_error

        async with ClientSession(**client_options) as session:
            while len(urls) != 0:
                url: str = urls.pop()
                if not url:
                    continue

                try:
                    async with session.get(url=url) as response:
                        response_data: bytes = await response.read()
                except TimeoutError:
                    LOG.warning(f'Timed out: {url}')
                    num_timeout += 1
                    continue
                except Exception:
                    LOG.error(f'Unknown error ({url}): {format_exc()}')
                    num_unknown_error += 1
                    continue
                else:
                    num_downloaded += 1

                if use_hashing:
                    download_path: Path = output_dir / Path(hash_function(response_data))
                else:
                    download_path: Path = output_dir / PurePath(urlparse(url=url).path).name

                if download_path.exists():
                    LOG.warning(f'File already exists at download path: {download_path}')
                    num_duplicates += 1
                else:
                    download_path.write_bytes(response_data)

    try:
        await asyncio_gather(*(asyncio_ensure_future(work()) for _ in range(num_concurrent)))
    except KeyboardInterrupt:
        pass

    return num_downloaded, num_duplicates, num_timeout, num_unknown_error


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
        help='URLs of files to be downloaded.',
        dest='urls',
        metavar='URL',
        type=str,
        nargs='+',
        default=[],
        action=ParseUrlsAction
    )

    parser.add_argument(
        '-U', '--urls-files',
        help='',
        dest='urls_files',
        metavar='URLS_FILE',
        type=FileType(mode='r'),
        nargs='+',
        default=[],
        action=ParseUrlsFilesAction
    )

    parser.add_argument(
        '-x', '--use-hashing',
        help='Let the filename of downloaded documents be their sha256 hash.',
        dest='use_hashing',
        action='store_true'
    )

    parser.add_argument(
        '-n', '--num-concurrent',
        help='File paths of files containing URLs of files to be downloaded.',
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
        '-q', '--quiet',
        help='Do not print warning messages, error messages, or a result summary.',
        dest='quiet',
        action='store_true'
    )

    parser.add_argument(
        '-o', '--output-dir',
        help='A path to which the output should be written.',
        dest='output_directory',
        type=Path,
        required=True
    )

    return parser


def main():
    args = get_parser().parse_args()

    if not args.urls and not args.urls_files:
        args.all_urls = set(stdin.read().splitlines())

    if not args.quiet:
        handler = StreamHandler(stream=stderr)
        handler.setFormatter(Formatter('%(asctime)s [%(levelname)s]  %(message)s'))
        LOG.addHandler(handler)

    start_time = time()
    num_downloaded, num_duplicates, num_timeout, num_unknown_error = asyncio_run(
        download_documents(
            urls=list(args.all_urls),
            output_dir=args.output_dir,
            client_options=dict(
                timeout=ClientTimeout(total=args.num_total_timeout_seconds)
            )
        )
    )
    end_time = time()

    if not args.quiet:
        ...


if __name__ == '__main__':
    main()
