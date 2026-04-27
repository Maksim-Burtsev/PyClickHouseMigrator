FROM python:3.14-slim

LABEL maintainer="Maksim Burtsev <zadrot-lol@list.ru>"
LABEL description="Python CLI tool for ClickHouse schema migrations"
LABEL org.opencontainers.image.source="https://github.com/Maksim-Burtsev/PyClickHouseMigrator"
LABEL org.opencontainers.image.license="MIT"

ARG PACKAGE_VERSION
RUN test -n "$PACKAGE_VERSION" && \
    pip install --no-cache-dir "py-clickhouse-migrator==${PACKAGE_VERSION}"

ENV CLICKHOUSE_MIGRATE_DIR=/migrations

ENTRYPOINT ["migrator"]
