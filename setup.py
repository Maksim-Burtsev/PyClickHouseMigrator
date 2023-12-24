from setuptools import find_packages, setup

setup(
    name="clickhouse-migrator",
    version="0.1",
    description="Simple tool for manage ClickHouse migrations.",
    author="Maksim Burtsev",
    author_email="zadrot-lol@list.ru",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.1",
        "clickhouse-driver>=0.2.0",
    ],
    entry_points={
        "console_scripts": ["migrator = clickhouse_migrator.cli:main"],
    },
)
