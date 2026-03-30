FROM python:3.14-slim

LABEL maintainer="Maksim Burtsev <zadrot-lol@list.ru>"
LABEL description="Python CLI tool for ClickHouse schema migrations"
LABEL org.opencontainers.image.source="https://github.com/Maksim-Burtsev/PyClickHouseMigrator"
LABEL org.opencontainers.image.license="MIT"

RUN pip install --no-cache-dir py-clickhouse-migrator

ENV CLICKHOUSE_MIGRATE_DIR=/migrations

ENTRYPOINT ["migrator"]
