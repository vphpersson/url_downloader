#!/usr/bin/env python3

from asyncio import run as asyncio_run, Task
from dataclasses import dataclass
from logging import getLogger
from argparse import ArgumentParser, Action, Namespace, FileType, ArgumentTypeError
from pathlib import Path, PurePath
from typing import Optional, Iterable, Type
from io import TextIOWrapper
from sys import stdin
from logging import DEBUG
from datetime import datetime
from urllib.parse import urlparse
from hashlib import sha256

from httpx import AsyncClient, Response, TimeoutException, HTTPStatusError
from pyutils.argparse.typed_argument_parser import TypedArgumentParser
from terminal_utils.progressor import Progressor
from terminal_utils.log_handlers import ColoredProgressorLogHandler, ProgressStatus
from pyutils.my_string import text_align_delimiter
from pyutils.asyncio import limited_gather

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


class URLDownloaderArgumentParser(TypedArgumentParser):

    class Namespace:
        urls: list[str]
        urls_files: list[str]
        all_urls: set[str]
        use_hashing: bool
        num_concurrent: int
        num_total_timeout_seconds: int
        ignore_warnings: bool
        quiet: bool
        output_directory: Path

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.add_argument(
            '-u', '--urls',
            help='URLs of resources to be downloaded.',
            dest='urls',
            metavar='URL',
            type=str,
            nargs='+',
            default=[],
            action=self.ParseUrlsAction
        )

        self.add_argument(
            '-U', '--urls-files',
            help='File path of files containing rows of URLs of resources to be downloaded.',
            dest='urls_files',
            metavar='URLS_FILE',
            type=FileType(mode='r'),
            nargs='+',
            default=[],
            action=self.ParseUrlsFilesAction
        )

        self.add_argument(
            '-x', '--use-hashing',
            help="Let the filenames of downloaded resources' be a sha256 hash of their contents.",
            dest='use_hashing',
            action='store_true'
        )

        self.add_argument(
            '-n', '--num-concurrent',
            help='The number of concurrent downloads to be performed.',
            dest='num_concurrent',
            type=int,
            default=5
        )

        self.add_argument(
            '-t', '--timeout',
            help='The total amount of time in seconds to wait for one download to finish before timing out.',
            dest='num_total_timeout_seconds',
            type=int,
            default=60
        )

        self.add_argument(
            '-w', '--ignore-warnings',
            help='Ignore warning messages.',
            dest='ignore_warnings',
            action='store_true'
        )

        self.add_argument(
            '-q', '--quiet',
            help='Do not print warning messages, error messages, or the result summary.',
            dest='quiet',
            action='store_true'
        )

        self.add_argument(
            '-o', '--output-dir',
            help='A path to a directory where downloaded resources are to be saved.',
            dest='output_directory',
            type=Path,
            default=Path('.')
        )

    class ParseUrlsAction(Action):
        def __call__(
            self,
            parser: ArgumentParser,
            namespace: Namespace,
            urls: Iterable[Path],
            option_string: Optional[str] = None
        ) -> None:

            all_urls: set[str] = namespace.all_urls if hasattr(namespace, 'all_urls') else set()
            all_urls.update(urls)

            setattr(namespace, self.dest, urls)
            namespace.all_urls = all_urls

    class ParseUrlsFilesAction(Action):
        def __call__(
            self,
            parser: ArgumentParser,
            namespace: Namespace,
            urls_files: Iterable[TextIOWrapper],
            option_string: Optional[str] = None
        ) -> None:

            all_urls: set[str] = namespace.all_urls if hasattr(namespace, 'all_urls') else set()

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


async def main():
    args: Type[URLDownloaderArgumentParser.Namespace] = URLDownloaderArgumentParser().parse_args()

    try:
        with Progressor() as progressor:
            if not args.urls and not args.urls_files:
                args.all_urls = set(stdin.read().splitlines())

            if args.quiet:
                LOG.disabled = True
            else:
                LOG.addHandler(ColoredProgressorLogHandler(progressor=progressor, print_warnings=not args.ignore_warnings))
                LOG.setLevel(level=DEBUG)

            download_summary = DownloadSummary()

            def result_callback(response_task: Task, url: str) -> None:
                LOG.debug(ProgressStatus(iteration=download_summary.num_completed, total=len(args.all_urls)))

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

                if args.use_hashing:
                    download_path: Path = args.output_directory / Path(sha256(response.content).hexdigest())
                else:
                    download_path: Path = args.output_directory / PurePath(urlparse(url=url).path).name

                if download_path.exists():
                    LOG.warning(f'File already exists at download path: {download_path}')
                    download_summary.num_duplicates += 1
                else:
                    download_path.write_bytes(response.content)

            async with AsyncClient(timeout=float(args.num_total_timeout_seconds)) as http_client:
                start_time: datetime = datetime.now()

                await limited_gather(
                    iteration_coroutine=http_client.get,
                    iterable=args.all_urls,
                    result_callback=result_callback,
                    num_concurrent=args.num_concurrent
                )

                end_time: datetime = datetime.now()
    except KeyboardInterrupt:
        pass
    except:
        LOG.exception('Unexpected error.')
    else:
        if not args.quiet:
            print(
                text_align_delimiter(
                    f'Elapsed time: {end_time - start_time}\n'
                    f'Num URLs: {len(args.all_urls)}\n'
                    f'{download_summary}'
                )
            )

if __name__ == '__main__':
    asyncio_run(main())
