from setuptools import setup, find_packages

setup(
    name='url_downloader',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'httpx',
        'terminal_utils @ git+ssh://git@github.com/vphpersson/terminal_utils.git#egg=terminal_utils',
    ]
)
