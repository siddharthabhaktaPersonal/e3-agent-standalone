# E3 Agent — standalone (forked from cuPHY-CP/data_lake)

## Purpose

This directory started as NVIDIA's `data_lake` (per-slot PHY telemetry capture
into ClickHouse, streamed to dApps over the E3 interface) and has been
forked into a **standalone Linux process**: `e3_agent.hpp`/`.cpp` (the E3
ZMQ + POSIX-SHM interface) are unmodified, but the ClickHouse/cuPHY-coupled
`data_lake` has been replaced with a plain C/C++ in-process data structure plus
a synthetic slot-clock generator, and the CUDA/cuphydriver/nvipc/ClickHouse
build dependencies are gone. See `README.md` for build/run instructions and
`e3_manager.cpp` for a sample E3 Manager (dApp-side) reference client.

## Invariants (never violate)

- **`e3::StreamType` IDs and `SharedMemoryHeader` are a wire/ABI contract.**
  They are what makes this fork interoperable with real out-of-tree E3
  Manager/dApp implementations (e.g. `NVIDIA/aerial-sample-apps` dApps), which
  is the whole point of keeping `e3_agent.hpp`/`.cpp` unmodified. A
  renumber/rename/reorder that compiles and passes every local check still
  silently breaks any real consumer. The append-only / never-reorder / cap-128
  rules that prevent this live in the `e3_agent.hpp` comments at each type
  definition - read and obey them there before editing these types. There is
  no automated wire-format test here (same as upstream) - verify manually with
  `e3_manager_sample` against `e3_agent_standalone` after any change.

## What lives where

- `e3_agent.hpp` / `e3_agent.cpp` - E3 interface, unmodified from upstream.
- `nvlog.hpp` - drop-in logging shim (upstream's nvlog is internal-only) so the
  two files above compile without edits.
- `data_lake.hpp` / `data_lake.cpp` - standalone data store + synthetic feeder.
- `main.cpp` - standalone E3 Agent process entry point.
- `e3_manager.cpp` - sample E3 Manager reference client.
