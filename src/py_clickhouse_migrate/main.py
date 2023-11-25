import datetime as dt
import os
from dataclasses import dataclass

from clickhouse_driver import Client
from template import value as tt_value

SQL = str


@dataclass
class Migration:
    name: str
    up: SQL
    rollback: SQL


class UpMigration(Migration):
    ...


class RollbackMigration(Migration):
    ...


@dataclass
class Settings:
    MIGRATIONS_DIR: str = os.getenv("CLICKHOUSE_MIGRATE_DIR", "./db/migrations")
    DATABASE_URL: str = os.getenv("CLICKHOUSE_MIGRATE_URL")


settings = Settings()


class Migrator(object):
    def __init__(
        self,
        database_url: str = settings.DATABASE_URL,
        migrations_dir: str = settings.MIGRATIONS_DIR,
    ) -> None:
        self.ch_client: Client = Client(database_url)
        self.migrations_dir: str = migrations_dir
        # TODO parse database name from url

    def init(self) -> None:
        # TODO check existence of migrations directory and create if does not exists
        # TODO create database if not exists
        migrator_table: SQL = """
        CREATE TABLE IF NOT EXISTS db_migrations (
            name String,
            up String,
            rollback String,
            dt DateTime64 now()
        )
        Engine MergeTree()
        ORDER BY dt
        """
        self.ch_client.execute(migrator_table)

    def up(self, n: int = None) -> None:
        migrations: list[UpMigration] = self.get_migrations_for_apply(n)
        for migration in migrations:
            self.apply_migration(query=migration.query)
            self.save_applied_migration(
                name=migration.name,
                up=migration.up,
                rollback=migration.rollback,
            )
        self.save_current_schema()

    def rollback(self, n: int = 1) -> None:
        migrations: list[RollbackMigration] = self.get_migrations_for_rollback(n)
        for migration in migrations:
            # TODO open transaction?
            self.apply_migration(
                query=migration.rollback,
            )
            self.delete_migration(
                name=migration.name,
            )
        self.save_current_schema()

    def apply_migration(self, query: str) -> None:
        queries: list[SQL] = query.split(";")
        # TODO transation
        for query in queries:
            self.ch_client.execute(query)

    def get_migrations_for_apply(self, n: int = None) -> list[Migration]:
        pass

    def get_applied_migrations(self) -> list[str]:
        pass

    def get_migrations_for_rollback(self, n: int = 1) -> list[RollbackMigration]:
        pass

    def create_new_migration(self, name: str = "") -> None:
        number: int = self.ch_client.execute("SELECT count() FROM db_migrations LIMIT 1")[0][0]
        filename: str = f"{number}_{str(dt.datetime.now()).replace(' ', '_')}_{name}.py"
        with open(f"{self.migrations_dir}/{filename}", "w") as f:
            f.write(tt_value)

    def save_current_schema(self) -> None:
        # TODO save save create tables for all tables in current database
        pass

    def save_applied_migration(self, name: str, up: SQL, rollback: SQL) -> None:
        self.ch_client.execute(f"INSERT INTO db_migrations VALUES ('{name}', '{up}', '{rollback}')")

    def delete_migration(self, name: str) -> None:
        self.ch_client.execute(f"DELETE FROM db_migrations WHERE name='{name}'")
