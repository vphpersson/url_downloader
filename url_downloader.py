#!/usr/bin/env python

from asyncio import run as asyncio_run
from collections.abc import Collection
from pathlib import Path
from typing import Type
from sys import stdin
from logging import DEBUG

from httpx import AsyncClient
from httpx._types import URLTypes
from terminal_utils.progressor import Progressor
from terminal_utils.log_handlers import ColoredProgressorLogHandler
from pyutils.my_string import text_align_delimiter

from url_downloader import LOG, download_urls, DownloadSummary
from url_downloader.cli import URLDownloaderArgumentParser


async def url_downloader(
    urls: Collection[URLTypes],
    output_directory: Path,
    use_hashing: bool = False,
    num_total_timeout_seconds: int = 60,
    quiet: bool = False,
    ignore_warnings: bool = False,
    num_concurrent: int = 5
) -> DownloadSummary:
    """
    Download resources over HTTP and write them to a directory.

    :param urls: The URLs of the resources to be downloaded.
    :param output_directory: The directory where the resources are to be written.
    :param use_hashing: Whether to name the resources by their hash value when writing them to the output directory.
    :param num_total_timeout_seconds: A timeout value in seconds to wait for a resource to be retrieved.
    :param quiet: Whether downloading progress should be printed.
    :param ignore_warnings: Whether downloading warning should be printed.
    :param num_concurrent: The number of concurrent requests for resources.
    :return: A summary describing the status of the download job.
    """

    with Progressor() as progressor:
        if quiet:
            LOG.disabled = True
        else:
            LOG.addHandler(
                ColoredProgressorLogHandler(progressor=progressor, print_warnings=not ignore_warnings)
            )
            LOG.setLevel(level=DEBUG)

        async with AsyncClient(timeout=float(num_total_timeout_seconds)) as http_client:
            return await download_urls(
                urls=urls,
                output_directory=output_directory,
                http_client=http_client,
                use_hashing=use_hashing,
                num_concurrent=num_concurrent
            )


async def main():
    args: Type[URLDownloaderArgumentParser.Namespace] = URLDownloaderArgumentParser().parse_args()

    try:
        if not args.urls and not args.urls_files:
            args.all_urls = set(stdin.read().splitlines())

        download_summary = await url_downloader(
            urls=args.all_urls,
            output_directory=args.output_directory,
            use_hashing=args.use_hashing,
            num_total_timeout_seconds=args.num_total_timeout_seconds,
            quiet=args.quiet,
            ignore_warnings=args.ignore_warnings,
            num_concurrent=args.num_concurrent
        )
    except KeyboardInterrupt:
        pass
    except:
        LOG.exception('Unexpected error.')
    else:
        if not args.quiet:
            print(
                text_align_delimiter(
                    f'Elapsed time: {download_summary.elapsed_time}\n'
                    f'Num URLs: {len(args.all_urls)}\n'
                    f'{download_summary}'
                )
            )

if __name__ == '__main__':
    asyncio_run(main())
