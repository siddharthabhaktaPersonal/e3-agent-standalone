# E3 Agent — standalone (Layer-2 KPI edition)

## Purpose

A standalone Linux process implementing an E3 interface for Layer-2 (MAC)
KPIs (PRB usage, TBS, per-LCID bytes, MCS, CQI, HARQ outcomes, BLER, SNR,
BSR, PHR) delivered as JSON over ZMQ - no shared memory, no L1/PHY
telemetry. See `README.md` for the full field list, build/run instructions,
and the project's history (it started as a fork of NVIDIA's L1/SHM
`cuPHY-CP/data_lake`; that surface has since been fully replaced - see
README's "History" section).

## Invariants (never violate)

- **`e3::StreamType` IDs in `e3_agent.hpp` are a wire/ABI contract.** They
  are the stable values used in `E3-SubscriptionRequest.telemetryIdentifierList`
  and `E3-RanFunctionDefinition`. Append-only, never reorder or reuse a bit
  position - a renumber that compiles and passes every local check still
  silently breaks any real consumer. There is no automated wire-format test
  here - verify manually with `e3_manager_sample` against
  `e3_agent_standalone` after any change to this enum or to
  `sendDueIndications()`'s field mapping.
- Keep `data_lake.hpp`'s KPI structs (`PrbStats`, `TbsStats`, `PerLcidBytes`,
  `McsIndexStats`, `WbCqi`, `TbStats`, `SnrStats`, `BsrStats`, `PhrStats`)
  and `e3_agent.cpp`'s `sendDueIndications()` field mapping in sync - each
  `e3::StreamType` bit corresponds to exactly one field/array here.
- Data refresh and notification are on deliberately decoupled cadences:
  `DataLake::pushL2Slot()` (driven by `main.cpp`'s slot clock) only updates
  the current KPI snapshot; `E3Agent`'s own notifier thread independently
  wakes on `NOTIFIER_TICK_INTERVAL` and sends indications to whichever
  subscriptions are due per their own `periodicity_us`. Don't reintroduce a
  direct call from `DataLake` into `E3Agent` to "push" a notification.

## What lives where

- `e3_agent.hpp` / `e3_agent.cpp` - the E3 interface: ZMQ REQ/REP setup,
  PUB/SUB subscribe/indications, `e3::StreamType` L2 KPI wire IDs. No SHM.
- `nvlog.hpp` - drop-in logging shim (NVIDIA's nvlog is internal-only) so
  the interface files don't need it.
- `data_lake.hpp` / `data_lake.cpp` - `DataLake`: one slot's worth of
  per-UE L2 KPIs in plain structs, plus a synthetic slot-clock generator.
- `main.cpp` - standalone E3 Agent process entry point.
- `e3_manager.cpp` - sample E3 Manager reference client (decodes/prints
  every L2 KPI field; also supports `--dump-file` JSONL wire capture).
