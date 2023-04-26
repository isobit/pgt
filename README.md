# pgt

:warning: Currently a work-in-progress.

`pgt` is a collection of tools for PostgreSQL.

## Migrations

Features:

- Watchdog that looks for other backend processes that are blocked waiting for locks taken by migrations, and cancels if too many are blocked for too long. (sorta inspired by autovacuum's back-off strategy)
- Auto transaction wrapping, but can be disabled e.g. for create index concurrently
- Configurable schema version table
- Version table tracks full history of migrations; updated atomically with transaction but also includes mechanism for detecting partially failed up/down for non-txn migrations
- Convenience for dumping schema SQL (--dump flag that calls pg_dump or custom command, e.g. exec'ing pg_dump in a docker container)
- Test mode that automatically creates an isolated test database, runs each migration (up+down+up or just up if irreversible)
- Interactive mode
- (Partially implemented) export reports on locks acquired by migrations (e.g. detect non-concurrent index creat

Usage:

```
USAGE:
    pgt migrate [OPTIONS]

OPTIONS:
    -h, --help                                              show usage help
    -d, --database <VALUE>         PGT_DATABASE             database connection string  (required)
    -s, --source <VALUE>           PGT_MIGRATION_SOURCE     path to migrations directory  (required)
    -t, --target <VALUE>           PGT_MIGRATION_TARGET     version to target  (default: latest)
    --version-table <VALUE>        PGT_VERSION_TABLE        (default: pgt.schema_version)
    -y, --yes
    --test                         PGT_TEST                 enable test mode
    --test-database-name <VALUE>                            (default: pgt_migrate_test_1693935840)
    --retain-test-database
    --dump <VALUE>                 PGT_DUMP                 file path to dump schema to
    --dump-command <VALUE>         PGT_DUMP_COMMAND         command used to dump schema  (default: pg_dump --schema-only '{{.Url}}')
    --max-block-duration <VALUE>   PGT_MAX_BLOCK_DURATION   (default: 10s)
    --max-block-processes <VALUE>  PGT_MAX_BLOCK_PROCESSES  (default: 0)
```
