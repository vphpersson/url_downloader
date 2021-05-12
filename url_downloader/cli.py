from argparse import ArgumentParser, Action, Namespace, FileType, ArgumentTypeError
from pathlib import Path
from typing import Iterable, Optional
from io import TextIOWrapper

from pyutils.argparse.typed_argument_parser import TypedArgumentParser


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
