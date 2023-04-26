Lock timeout, retry, and/or conflicting lock monitor

Analyze locks when running in isolation, save report

Lint for create index concurrently, monitor concurrent index creation

Test with deployed migration version and commit version (public HTTP endpoint to report version?)

Config: preface+epilogue, lock timeouts, etc

Diff dump against live DB

Redo to test down

Checkpoints 

Schema compat version:

- clients can report which version they were built + tested with
- migrations can specify that they are incompatible with clients expecting at
    or below some version
- when running migrations, warn/error if incompatible client was last seen
    recently

https://www.braintreepayments.com/blog/safe-operations-for-high-volume-postgresql/

https://gocardless.com/blog/zero-downtime-postgres-migrations-the-hard-parts/

https://github.com/doctolib/safe-pg-migrations
