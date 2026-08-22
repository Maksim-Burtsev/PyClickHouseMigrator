FROM python:3.14-slim

LABEL maintainer="Maksim Burtsev <zadrot-lol@list.ru>"
LABEL org.opencontainers.image.title="PyClickHouseMigrator"
LABEL org.opencontainers.image.description="SQL-first ClickHouse schema migration CLI with checksums, rollback, dry-run, advisory locking, and cluster support"
LABEL org.opencontainers.image.source="https://github.com/Maksim-Burtsev/PyClickHouseMigrator"
LABEL org.opencontainers.image.documentation="https://maksim-burtsev.github.io/PyClickHouseMigrator/"
LABEL org.opencontainers.image.licenses="MIT"

ARG PACKAGE_VERSION
RUN test -n "$PACKAGE_VERSION" && \
    pip install --no-cache-dir "py-clickhouse-migrator==${PACKAGE_VERSION}"

ENV CLICKHOUSE_MIGRATE_DIR=/migrations

ENTRYPOINT ["migrator"]
