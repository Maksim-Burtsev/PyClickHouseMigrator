import datetime as dt
import os
from dataclasses import dataclass
from importlib.machinery import SourceFileLoader

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from dotenv import load_dotenv

load_dotenv()

SQL = str


MIGRATION_TEMPLATE: str = '''
def up() -> str:
    return """
    """


def rollback() -> str:
    return """
    """
'''
DEFATULT_MIGRATIONS_DIR: str = "./db/migrations"


class ClickHouseServerIsNotHealthyError(Exception):
    ...


class MigrationDirectoryNotFoundError(Exception):
    ...


class InvalidMigrationError(Exception):
    ...


class MissingDatabaseUrlError(Exception):
    ...


@dataclass
class Migration:
    name: str
    up: SQL
    rollback: SQL


class Migrator(object):
    def __init__(
        self,
        database_url: str = "",
        migrations_dir: str = "",
    ) -> None:
        self.database_url: str = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise MissingDatabaseUrlError(
                "ClickHouse url didn't passed.\n.env variable 'DATABASE_URL' or param --url is missing."
            )
        self.migrations_dir: str = migrations_dir or os.getenv("CLICKHOUSE_MIGRATE_DIR", DEFATULT_MIGRATIONS_DIR)
        self.ch_client: Client = Client.from_url(database_url)
        self.health_check()
        self.check_migrations_table()

    def check_migrations_table(self) -> None:
        migrator_table: SQL = """
        CREATE TABLE IF NOT EXISTS db_migrations (
            name String,
            up String,
            rollback String,
            dt DateTime64 DEFAULT now()
        )
        Engine MergeTree()
        ORDER BY dt
        """
        self.ch_client.execute(migrator_table)

    def init(self) -> None:
        db_name: str = self.get_db_name()
        if db_name != "default":
            self.ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        self.create_migrations_directory()
        self.save_current_schema()
        print(f"Migrations directory {self.migrations_dir} sucessfully initialized.")

    def health_check(self) -> None:
        try:
            self.ch_client.execute("SELECT 1")
        except Exception as exc:
            raise ClickHouseServerIsNotHealthyError(f"ClickHouse server in not healthy: {exc}.") from exc

    def get_db_name(self) -> str:
        db_name: str = self.database_url.rsplit("/", 1)[-1]
        if "?" in db_name:
            db_name = db_name[: db_name.find("?")]
        return db_name

    def create_migrations_directory(self) -> None:
        if not os.path.exists(self.migrations_dir):
            os.makedirs(self.migrations_dir)

    def up(self, n: int = None) -> None:
        migrations: list[Migration] = self.get_migrations_for_apply(n)
        if not migrations:
            print("There is no migrations for apply.")
        for migration in migrations:
            self.apply_migration(query=migration.up)
            self.save_applied_migration(
                name=migration.name,
                up=migration.up,
                rollback=migration.rollback,
            )
            print(f"{migration.name} applied [✔]")
        self.save_current_schema()

    def rollback(self, number: int = 1) -> None:
        migrations: list[Migration] = self.get_migrations_for_rollback(number=number)
        for migration in migrations:
            # TODO open transaction by with flag (for enabled setting)
            self.apply_migration(
                query=migration.rollback,
            )
            self.delete_migration(
                name=migration.name,
            )
            print(f"{migration.name} rolled back [✔].")
        self.save_current_schema()

    def apply_migration(self, query: SQL) -> None:
        queries: list[SQL] = query.split(";")
        for query in queries:
            query = query.strip("\n ")
            if not query:
                continue
            try:
                self.ch_client.execute(query)
            except ServerException as exc:
                raise InvalidMigrationError(f"Query {query} raise error: {exc}") from exc

    def get_migrations_for_apply(self, number: int = None) -> list[Migration]:
        filenames: list[str] = self.get_unapplied_migration_names()

        if number:
            filenames: list[str] = filenames[:number]

        result = []
        for filename in filenames:
            module = SourceFileLoader(filename, f"{self.migrations_dir}/{filename}").load_module()
            up: str = module.up()
            if not up.strip():
                print(f"Skip empty migration: {filename}")
                continue
            result.append(
                Migration(
                    name=filename,
                    up=up,
                    rollback=module.rollback(),
                )
            )

        return result

    def get_unapplied_migration_names(self) -> list[str]:
        filenames: list[str] = [file for file in os.listdir(self.migrations_dir) if file.endswith(".py")]
        applied_migrations: list[str] = self.get_applied_migrations_names()
        return sorted(list(set(filenames) - set(applied_migrations)))

    def get_applied_migrations_names(self) -> list[str]:
        return [row[0] for row in self.ch_client.execute("SELECT name FROM db_migrations ORDER BY dt")]

    def get_migrations_for_rollback(self, number: int = 1) -> list[Migration]:
        assert number > 0  # TODO move validation in separate method
        return [
            Migration(name=row[0], up=row[1], rollback=row[2])
            for row in self.ch_client.execute(
                f"SELECT name, up, rollback FROM db_migrations ORDER BY dt DESC LIMIT {number}"
            )
        ]
        # TODO assert len(result) == number?

    def get_new_migration_filename(self, name: str = "") -> str:
        filename: str = f"{str(dt.datetime.now().strftime('%Y%m%d%H%S')).replace(' ', '_')}"
        if name:
            filename += f"_{name}"
        filename += ".py"
        return filename

    def create_new_migration(self, name: str = "") -> str:
        filepath: str = f"{self.migrations_dir}/{self.get_new_migration_filename(name)}"
        try:
            with open(filepath, "w") as f:
                f.write(MIGRATION_TEMPLATE)
        except FileNotFoundError:
            raise MigrationDirectoryNotFoundError(
                f"Migration directory {self.migrations_dir} not found.\nMake sure you 'init' it."
            ) from None
        print(f"Migration {filepath} has been created.")

        return filepath

    def save_current_schema(self) -> None:
        tables: list[tuple[str]] = self.ch_client.execute("SHOW TABLES")
        result: str = "---- Database schema ----\n\n"
        for table in tables:
            schema: list[tuple[str]] = self.ch_client.execute(f"SHOW CREATE TABLE {table[0]}")
            result += f"{schema[0][0]};\n\n"
        result = result.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        schema_path: str = f"{self.migrations_dir.rsplit('/', 1)[0]}/schema.sql"
        with open(schema_path, "w") as f:
            f.write(result[:-2])
        print(f"\nWriting schema {schema_path}")

    def save_applied_migration(self, name: str, up: SQL, rollback: SQL) -> None:
        self.ch_client.execute("INSERT INTO db_migrations (name, up, rollback) VALUES", [[name, up, rollback]])

    def delete_migration(self, name: str) -> None:
        self.ch_client.execute(f"DELETE FROM db_migrations WHERE name='{name}'")

    def show_migrations(self) -> None:
        applied_migration_names = list(map(lambda x: f"[✔] {x}", self.get_applied_migrations_names()))[::-1]
        if applied_migration_names:
            applied_migration_names[0] += " (HEAD)"
        unapplied_migration_names = list(map(lambda x: f"[ ] {x}", self.get_unapplied_migration_names()))[::-1]
        print(
            "\n".join(applied_migration_names + unapplied_migration_names)
            + f"\n\nApplied: {len(applied_migration_names)}"
            f"\nPending: {len(unapplied_migration_names)}"
        )
