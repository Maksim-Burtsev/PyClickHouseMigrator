# PyClickHouseMigrate

PyClickHouseMigrate is simple tool for manage your ClickHouse migrations.

Inspired by [dbmate](https://github.com/amacneil/dbmate) and [aerich](https://github.com/tortoise/aerich).


## Install

```sh
➜ pip install py-clickhouse-migrator
```

## Usage

### Init migrations directory.

By default migrator will create and use `./db/migrations`.

```sh
➜ migrator --url=clickhouse://default@127.0.0.1:9000/default init
```

As you can see ClickHouse url passed with `--url` param.

If you want to change migrations path then you can use `--path` parameter.

```sh
➜ migrator --path=./your_path/migrations  --url=clickhouse://default@127.0.0.1:9000/default init
```

After initializitaion make sure you the folders will created.

```sh
➜ tree db

db
├── migrations
└── schema.sql
```

### Create new migration

For creation new migrations you need `new` command.

```sh
➜  migrator --url=...  new first_migration

Migration ./db/migrations/202401080000_first_migration.py has been created.
```

And after this you can find empty migration inside db directory:
```sh
➜ tree db
db
├── migrations
│   └── 202401080000_first_migration.py
└── schema.sql
```


## Apply new migration
To apply new migrations you need `up` command.

Apply all new migrations:
```sh
➜  migrator --url=...  up
```

Apply `N` new migrations (the next after the last applied):
```sh
➜  migrator --url=...  up N
```

## Rollback
To rollback migrations you need `rollback` command.

Rollback last applied migration:
```sh
➜  migrator --url=...  rollback
```

Rollback `N` migrations (the next after the last applied):
```sh
➜  migrator --url=...  rollback N
```

## View all migrations
To see all migrations (including unapplied) you can use `show` command.

```sh
➜  migrator --url=...  show

[✔] 202404081721_drop_some_column.py (HEAD)
...
[✔] 202401261452_new_column.py
[✔] 202401151114_one_more_table.py
[✔] 202401091440_new_table.py
[✔] 202312261318_init.py

Applied: 21
Pending: 0
```

Here you can also see the last applied migration `(HEAD)`.

## Actual schema of database
Actual schema of database is always stored in the `schema.sql` in the folder where all your migrations are located. (by default its `/db`)
```sh
➜ tree db
db
├── migrations
│   └── 202401080000_init.py
└── schema.sql # here you can find CREATE TABLE for any table of current database
```
