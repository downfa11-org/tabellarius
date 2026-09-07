# CDC publish retry design

## Contract

Each committed GTID transaction is serialized and sent to Cursus exactly once
per in-process delivery attempt. Tabellarius then waits for the delivery to
drain. If that wait times out, it waits again for the same producer sequence
instead of calling `Send` again. Retry delays use jittered exponential backoff
bounded at thirty seconds and stop when the source context is cancelled.

A checkpoint is atomically stored only after the delivery returns success.
Explicit non-retryable broker errors remain fatal. A process restart resumes
from the last stored GTID, so an acknowledgement/checkpoint crash window may
produce a duplicate but cannot skip the failed GTID. Downstream consumers must
continue to use the GTID as their idempotency key.

## Dependency

This contract consumes the Cursus SDK delivery-aware `Flush() error` API from
Cursus PR #179. Broker-side replication durability and Tabellarius checkpoint
recovery remain independently reviewable changes.
