from setuptools import find_packages, setup

setup(
    name="py-clickhouse-migrator",
    version="0.1",
    description="Simple tool for manage ClickHouse migrations.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Maksim Burtsev",
    author_email="zadrot-lol@list.ru",
    license="MIT",
    classifiers={"Operating System :: OS Independent"},
    packages=find_packages(),
    install_requires=[
        "click>=8.0.1",
        "clickhouse-driver>=0.2.0",
    ],
    entry_points={
        "console_scripts": ["migrator = py_clickhouse_migrator.cli:main"],
    },
)
