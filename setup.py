from setuptools import setup, find_packages

setup(
    name='url_downloader',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'httpx',
        'pyutils @ git+ssh://git@github.com/vphpersson/pyutils.git#egg=pyutils',
        'terminal_utils @ git+ssh://git@github.com/vphpersson/terminal_utils.git#egg=terminal_utils'
    ]
)
