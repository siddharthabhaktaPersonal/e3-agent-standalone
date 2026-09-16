# E3 Agent — standalone (Layer-2 KPI edition)

## Purpose

A standalone Linux process implementing an E3 interface for Layer-2 (MAC)
KPIs (PRB usage, TBS, per-LCID bytes, MCS, CQI, HARQ outcomes, BLER, SNR,
BSR, PHR) delivered as JSON over ZMQ - no shared memory, no L1/PHY
telemetry. A real DU pushes its own KPIs via a pure-C API (`e3_l2_kpi.h`);
`e3_agent_standalone`'s built-in generator is only for testing without one.
See `README.md` for the full field list, build/run/DU-integration
instructions, and the project's history (it started as a fork of NVIDIA's
L1/SHM `cuPHY-CP/data_lake`; that surface has since been fully replaced -
see README's "History" section).

## Invariants (never violate)

- **`e3::StreamType` IDs in `e3_agent.hpp` are a wire/ABI contract.** They
  are the stable values used in `E3-SubscriptionRequest.telemetryIdentifierList`
  and `E3-RanFunctionDefinition`. Append-only, never reorder or reuse a bit
  position - a renumber that compiles and passes every local check still
  silently breaks any real consumer. There is no automated wire-format test
  here - verify manually with `e3_manager_sample` against
  `e3_agent_standalone` after any change to this enum or to
  `sendDueIndications()`'s field mapping.
- Keep `e3_l2_kpi.h`'s KPI structs (`PrbStats`, `TbsStats`, `PerLcidBytes`,
  `McsIndexStats`, `WbCqi`, `TbStats`, `SnrStats`, `BsrStats`, `PhrStats`)
  and `e3_agent.cpp`'s `sendDueIndications()` field mapping in sync - each
  `e3::StreamType` bit corresponds to exactly one field/array here.
- **`e3_l2_kpi.h` is a pure C header - keep it that way.** No C++ features
  (no `std::vector`, no default member initializers, no `namespace`, no
  classes), only `#include <stdint.h>` and POD `typedef struct`s, wrapped in
  `extern "C"` for C++ consumers. It's included directly by DU code (see
  `examples/du_integration_example.c`, compiled with a C compiler in
  `CMakeLists.txt` to catch any accidental C++-only construct creeping in).
  `E3_MAX_LCID` / `E3_MAX_DL_HARQ_ROUNDS` / `E3_MAX_UES_PER_CELL` /
  `E3_MAX_CELLS` are `#define`s here, not `constexpr` - a DU built against a
  different value than the agent silently corrupts `E3CellL2Info`'s layout
  across the ABI boundary, so bump the version story (README) if you ever
  need per-build values instead of compile-time constants.
- Data refresh and notification are on deliberately decoupled cadences:
  `DataLake::updateL2Slot()` / `pushL2Slot()` only update the current KPI
  snapshot; `E3Agent`'s own notifier thread independently wakes on
  `NOTIFIER_TICK_INTERVAL` and sends indications to whichever subscriptions
  are due per their own `periodicity_us`. Don't reintroduce a direct call
  from `DataLake` into `E3Agent` to "push" a notification.
- `sendDueIndications()` reads `dataLake->e3_buffer_info` by reference under
  `e3_buffer_mutex`, not by value - `E3L2BufferInfo`'s fixed
  `E3_MAX_CELLS * E3_MAX_UES_PER_CELL` arrays make a by-value copy tens of
  KB, and this runs every `NOTIFIER_TICK_INTERVAL` regardless of whether
  anything is due. Don't reintroduce a local copy there.

## What lives where

- `e3_l2_kpi.h` - pure C: the KPI struct layout and the C API
  (`e3_agent_create`/`e3_agent_update_l2_slot`/`e3_agent_destroy`) a DU
  links against. Single source of truth for the data model - `data_lake.hpp`
  includes it rather than redefining these structs.
- `e3_agent.hpp` / `e3_agent.cpp` - the E3 interface: ZMQ REQ/REP setup,
  PUB/SUB subscribe/indications, `e3::StreamType` L2 KPI wire IDs. No SHM.
- `nvlog.hpp` - drop-in logging shim (NVIDIA's nvlog is internal-only) so
  the interface files don't need it.
- `data_lake.hpp` / `data_lake.cpp` - `DataLake`: one slot's worth of
  per-UE L2 KPIs, a push-model entry point (`updateL2Slot`) real data
  arrives through, and a synthetic slot-clock generator (`pushL2Slot`).
- `e3_c_api.cpp` - implements `e3_l2_kpi.h`'s C API by wrapping `DataLake`
  behind an opaque handle (`reinterpret_cast`, never dereferenced as
  anything but `DataLake*` - the standard opaque-pointer idiom).
- `main.cpp` - standalone E3 Agent process entry point (synthetic data).
- `e3_manager.cpp` - sample E3 Manager reference client (decodes/prints
  every L2 KPI field; also supports `--dump-file` JSONL wire capture).
- `examples/du_integration_example.c` - plain-C sample DU integration via
  the push-model C API; compiled as C (not C++) on purpose.
