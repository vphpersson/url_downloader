#!/usr/bin/env python3

from asyncio import run as asyncio_run
from argparse import ArgumentParser, Action, Namespace, FileType, ArgumentTypeError
from pathlib import Path
from typing import Optional, Iterable, Type
from io import TextIOWrapper
from sys import stdin
from logging import DEBUG
from datetime import datetime

from httpx import AsyncClient
from pyutils.argparse.typed_argument_parser import TypedArgumentParser
from terminal_utils.progressor import Progressor
from terminal_utils.log_handlers import ColoredProgressorLogHandler
from pyutils.my_string import text_align_delimiter

from url_downloader import download_urls, DownloadSummary, LOG


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
            required=True
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

            num_urls = len(args.all_urls)

            if args.quiet:
                LOG.disabled = True
            else:
                LOG.addHandler(ColoredProgressorLogHandler(progressor=progressor, print_warnings=not args.ignore_warnings))
                LOG.setLevel(level=DEBUG)

            async with AsyncClient(timeout=float(args.num_total_timeout_seconds)) as client:
                start_time: datetime = datetime.now()

                download_summary: DownloadSummary = await download_urls(
                    http_client=client,
                    urls=list(args.all_urls),
                    output_dir=args.output_directory,
                    use_hashing=args.use_hashing,
                    num_concurrent=args.num_concurrent,
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
                    f'Num URLs: {num_urls}\n'
                    f'{download_summary}'
                )
            )

if __name__ == '__main__':
    asyncio_run(main())
