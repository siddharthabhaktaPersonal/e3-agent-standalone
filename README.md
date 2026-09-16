# E3 Agent — standalone (Layer-2 KPI edition)

A standalone Linux process implementing an E3 interface for **Layer-2 (MAC)
KPIs** - PRB usage, transport block sizes, per-LCID byte counts, MCS, CQI,
HARQ outcomes, BLER, SNR, buffer status, and power headroom - delivered
entirely as JSON over ZMQ. There is **no shared memory channel and no L1/PHY
telemetry** in this version; every stream is small enough to travel inline
in the indication message.

This began as a fork of NVIDIA's `cuPHY-CP/data_lake` (from
[aerial-cuda-accelerated-ran](https://github.com/NVIDIA/aerial-cuda-accelerated-ran)),
which implements an L1 (PHY) E3 interface with IQ samples, PUSCH PDUs, and
H-estimates delivered over POSIX shared memory. That entire L1/SHM surface
has been removed and replaced with an L2 KPI protocol - see "History" below.

## What's here

| File | Contents |
|---|---|
| `e3_agent.hpp` / `e3_agent.cpp` | The E3 interface: ZMQ REQ/REP setup session, PUB/SUB subscribe/indications, `e3::StreamType` wire IDs for the L2 KPI set. No SHM. |
| `nvlog.hpp` | ~60-line logging shim (level-filtered, prints to stderr) standing in for NVIDIA's internal logger. |
| `data_lake.hpp` / `data_lake.cpp` | `DataLake`: a plain C++ struct holding one slot's worth of per-UE L2 KPIs, plus a synthetic slot-clock generator (`pushL2Slot`). |
| `main.cpp` | Standalone process entry point: a slot clock that calls into `DataLake` every slot. |
| `e3_manager.cpp` | Sample E3 Manager (dApp-side reference client) that decodes and prints every L2 KPI field. |

## L2 KPIs

Per UE, per slot:

| Group | Fields |
|---|---|
| PRB usage | `dl_prb`, `ul_prb`, `dl_prb_retx`, `ul_prb_retx` |
| Transport block size | `dl_aggr_tbs`, `ul_aggr_tbs`, `dl_curr_tbs`, `ul_curr_tbs` |
| Per-LCID bytes | `per_lcid_dl_bytes[MAX_LCID]`, `per_lcid_ul_bytes[MAX_LCID]` |
| MCS | `dl_mcs`, `ul_mcs` |
| CQI | `wb_cqi` |
| TB outcome | `dl_harq_rounds[MAX_DL_HARQ_ROUNDS]`, `dl_errors`, `ul_errors`, `dl_bler`, `ul_bler` |
| SNR | `pusch_snr`, `pucch_snr` |
| Buffer status | `total_bsr` |
| Power headroom | `phr` |

Plus topology/timing: `timestamp`, `timestamp_tai`, `sfn`, `slot`, `cell_id`,
`n_cells`, `n_ue` (cell-level UE count), `rnti` (per-UE identity).

`MAX_LCID` (32) and `MAX_DL_HARQ_ROUNDS` (4) are `constexpr` in `e3_agent.hpp`
- change and rebuild if your use case needs different bounds.

**Scoping note:** the MAC-side struct this was specified against
(`per_ue_per_slot_e3_stats_t`) also carries `is_valid` and `ue_index` fields.
Those are internal ring-buffer bookkeeping (a validity flag and an array
slot index into the MAC's own UE table) with no meaning to an external E3
Manager - `rnti` already identifies the UE, and slot timing is carried by
the existing `sfn`/`slot`/`timestamp` fields - so they're intentionally not
part of the wire protocol.

## Build

Dependencies (all standard Linux packages, no CUDA):

```
sudo apt install libzmq3-dev nlohmann-json3-dev libfmt-dev cmake g++
```

Don't bother looking for a `cppzmq`/`libcppzmq-dev` package - most distros
(including Ubuntu 20.04/22.04) don't ship one. `libzmq3-dev` only gives you
the C API (`zmq.h`); the C++ binding (`zmq.hpp`) this project uses is
vendored in `third_party/` (from
[zeromq/cppzmq](https://github.com/zeromq/cppzmq), MIT licensed) and
`CMakeLists.txt` falls back to it automatically when no system `cppzmq`
CMake package is found. Make sure `third_party/zmq.hpp` and
`third_party/zmq_addon.hpp` are present alongside the other sources before
building.

```
mkdir build && cd build
cmake ..
make -j
```

Produces two binaries: `e3_agent_standalone` and `e3_manager_sample`.

## Run

```
./e3_agent_standalone --cells 2 --ues 3 --slot-us 500
```

In another terminal:

```
./e3_manager_sample --streams sfn,slot,cell_id,rnti,dl_prb,ul_prb,dl_mcs,ul_mcs,wb_cqi,dl_bler,ul_bler,pusch_snr,pucch_snr,total_bsr,phr --periodicity-us 100000
```

The manager will:
1. Send `setupRequest` over REQ/REP (port 5555) and print the assigned `dAppIdentifier`.
2. Send `subscriptionRequest` over PUB→SUB (port 5557) for the requested streams.
3. Receive `indicationMessage`s over SUB←PUB (port 5556), printing per-cell/per-UE KPI fields.
4. On exit (Ctrl-C or `--duration-s`), send `subscriptionDelete` then `releaseMessage`.

Run `--help` on either binary for the full flag list (ports, cell/UE counts,
slot timing, which telemetry streams to subscribe to).

Logging defaults to INFO-and-above. Set `NVLOG_SHIM_LEVEL=DBG` or `VERB` in
the environment for more detail (DBG logs a line per slot - noisy at 500 µs
cadence).

### Capturing the E3AP exchange

`e3_manager_sample --dump-file capture.jsonl` writes every message it sends
and receives - `setupRequest`/`setupResponse`, `subscriptionRequest`/
`subscriptionResponse`, `subscriptionDelete`, `releaseMessage`, and every
`indicationMessage` - as one JSON object per line, tagged `"dir": "tx"` or
`"rx"` with a timestamp and sequence number. Since the manager's SUB socket
sees everything the agent publishes (not just messages addressed to this
dApp), it incidentally captures other connected dApps' `rx` traffic too.

```
./e3_manager_sample --streams sfn,slot,rnti,dl_prb,ul_prb --dump-file capture.jsonl --duration-s 10
jq . capture.jsonl   # pretty-print / filter afterward
```

This only covers what the manager process itself sends/receives at the
application layer - it won't show ZMTP framing or traffic between the agent
and *other* dApps' outbound messages. For that, capture at the network layer
instead: `sudo tcpdump -i lo -w e3.pcap 'tcp port 5555 or tcp port 5556 or tcp port 5557'`
and open in Wireshark with the ZeroMQ/ZMTP dissector.

## Protocol summary

| Channel | Direction | Purpose |
|---|---|---|
| REQ/REP, `rep_port` (5555) | Manager → Agent | `setupRequest` / `setupResponse` |
| PUB→SUB, `sub_port` (5557) | Manager → Agent | `subscriptionRequest`, `subscriptionDelete`, `dAppControlAction`, `releaseMessage` |
| SUB←PUB, `pub_port` (5556) | Agent → Manager | `subscriptionResponse`, `indicationMessage`, `releaseMessage` |

There is no shared-memory channel in this version - every KPI travels inline
in `indicationMessage.protocolData`. Full message shapes are in
`e3_agent.cpp`'s `handleSetupRequest` / `handleSubscriptionRequest` /
`notifyDataReady`.

## Not included

This is a protocol/plumbing reference, not a scheduler simulator: KPI values
are uniform random, not physically or scheduler-meaningful. Swap
`DataLake::pushL2Slot` in `data_lake.cpp` for a real MAC stats source to get
meaningful values without touching the E3 interface itself.

## History

An earlier version of this project forked NVIDIA's production `data_lake`
(cuPHY-CP) as an L1 PHY telemetry E3 interface: IQ samples, PUSCH PDUs,
H-estimates, and SRS data delivered over a POSIX shared-memory ping-pong
buffer, with `e3_agent.hpp`/`.cpp` vendored byte-for-byte unmodified from
NVIDIA's source. That entire L1/SHM surface (`e3::StreamType` IDs 1-79,
`SharedMemoryHeader`, `createSharedMemoryBuffers`, the dual PUSCH/SRS
indication paths) has been removed in this version and replaced with the L2
KPI protocol described above - `e3_agent.hpp`/`.cpp` are original code now,
not an unmodified vendor copy. If you need the L1 SHM-based version, look at
an earlier commit or NVIDIA's `cuPHY-CP/data_lake` / `cuPHY-CP/e3agent-standalone`
directly.
