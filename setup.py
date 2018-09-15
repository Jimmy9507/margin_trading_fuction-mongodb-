import os
from setuptools import setup, find_packages


def readfile(filename, strip=False):
    with open(filename, mode="rt") as f:
        rv = f.read()
    if strip:
        rv = rv.strip()
    return rv


def __path(filename):
    return os.path.join(os.path.dirname(__file__),
                        filename)


build = 0

if os.path.exists(__path('build.info')):
    build = open(__path('build.info')).read().strip()

version = '0.1.{}'.format(build)

setup(
    name="mongo_datahub",
    version=version,
    url="http://www.ricequant.com",
    author="Ricequant",
    author_email="public@ricequant.com",

    packages=find_packages(exclude=[]),
    package_data={'datahub': ['fetch/mysql/calendar/tradedate.csv']},
    namespace_packages=["datahub"],

    install_requires=readfile("requirements.txt"),

    zip_safe=False,

    entry_points={
        "console_scripts": [
            "datahub = datahub.cmd:run",
            "xueqiu_crawler = datahub.fetch.xueqiu.crawler:cmd_entry",
            "xueqiu_build = datahub.handle.xueqiu.build_csv:cmd_entry",
        ]
    }
)
