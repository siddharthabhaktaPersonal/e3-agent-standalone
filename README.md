# E3 Agent — standalone

A standalone Linux process that runs NVIDIA's E3 interface (from
`cuPHY-CP/data_lake` in
[aerial-cuda-accelerated-ran](https://github.com/NVIDIA/aerial-cuda-accelerated-ran))
without cuPHY, ClickHouse, nvipc, or any other part of the Aerial SDK. It
exists so an E3 Manager / dApp can be developed and tested against a real
E3AP session — setup, subscribe, and shared-memory-backed indications — on a
plain dev box, no GPU or RAN required. The design mirrors NVIDIA's own
`cuPHY-CP/e3agent-standalone` reference (production E3 Agent + a synthetic
"data lake shim" in place of the real one).

## What changed vs. the production `data_lake`

| File | Status |
|---|---|
| `e3_agent.hpp` / `e3_agent.cpp` | **Unmodified.** This is the E3 interface: ZMQ REQ/REP + PUB/SUB session, POSIX SHM data channel, `e3::StreamType` wire IDs, `SharedMemoryHeader` ABI. |
| `data_lake.hpp` / `data_lake.cpp` | **Rewritten.** No ClickHouse, no cuPHY types. `DataLake` is now a plain C/C++ data structure: two fixed-size ping-pong row buffers per stream (FH IQ, PUSCH PDU, PUSCH H-est, SRS IQ, SRS H-est, SRS RbSNR) written directly into the SHM region `E3Agent` allocates, plus a small synthetic generator (`pushPuschSlot` / `pushSrsSlot`) standing in for the real cuPHY slot callbacks. |
| `nvlog.hpp` | **New.** NVIDIA's internal structured logger isn't public; this is a ~40-line drop-in with the same macro names (`NVLOGC_FMT`, `NVLOGE_FMT`, ...) that just prints to stderr, so `e3_agent.{hpp,cpp}` compile with zero edits. |
| `main.cpp` | **New.** The standalone process: a slot clock (default 500 µs, mu=1) that calls into `DataLake` every slot and on a configurable SRS period. |
| `e3_manager.cpp` | **New.** A sample E3 Manager (dApp-side reference client) — see below. |

The invariant from the original project still applies and is the reason
`e3_agent.hpp`/`.cpp` are byte-for-byte unchanged: **`e3::StreamType` IDs and
`SharedMemoryHeader` are a wire/ABI contract.** Anything speaking this
protocol (a real NVIDIA dApp, this sample manager, your own client) decodes
telemetry using those IDs and that struct layout. Don't renumber, reorder, or
resize them.

## Build

Dependencies (all standard Linux packages, no CUDA):

```
sudo apt install libzmq3-dev nlohmann-json3-dev libfmt-dev cmake g++
```

That's it — don't bother looking for a `cppzmq`/`libcppzmq-dev` package; most
distros (including Ubuntu 20.04/22.04) don't ship one. `libzmq3-dev` only
gives you the C API (`zmq.h`); the C++ binding (`zmq.hpp`) this project uses
is vendored in `third_party/` (from
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
./e3_agent_standalone --cells 2 --ues 3 --slot-us 500 --srs-period 10
```

In another terminal:

```
./e3_manager_sample --streams sfn,slot,cell_id,rnti,rsrp,sinr,mcs_index,iq_samples,pdu_data --periodicity-us 100000
```

The manager will:
1. Send `setupRequest` over REQ/REP (port 5555) and print the assigned `dAppIdentifier`.
2. Open the `/e3_ran_buffers` POSIX shared-memory segment the agent created.
3. Send `subscriptionRequest` over PUB→SUB (port 5557) for the requested streams.
4. Receive `indicationMessage`s over SUB←PUB (port 5556), printing per-cell/per-UE
   fields and previewing the raw samples referenced in SHM (IQ, PDU bytes, H-estimates, SRS blobs).
5. On exit (Ctrl-C or `--duration-s`), send `subscriptionDelete` then `releaseMessage`.

Run `--help` on either binary for the full flag list (ports, cell/UE counts,
slot timing, which telemetry streams to subscribe to).

Logging defaults to INFO-and-above (`e3_agent.cpp` logs a debug line every
single slot, which floods the terminal at 500 µs cadence if left unfiltered).
Set `NVLOG_SHIM_LEVEL=DBG` or `VERB` in the environment for more detail.

### Capturing the E3AP exchange

`e3_manager_sample --dump-file capture.jsonl` writes every message it sends
and receives - `setupRequest`/`setupResponse`, `subscriptionRequest`/
`subscriptionResponse`, `subscriptionDelete`, `releaseMessage`, and every
`indicationMessage` - as one JSON object per line, tagged `"dir": "tx"` or
`"rx"` with a timestamp and sequence number. Since the manager's SUB socket
sees everything the agent publishes (not just messages addressed to this
dApp), it incidentally captures other connected dApps' `rx` traffic too.

```
./e3_manager_sample --streams sfn,slot,cell_id,rnti,rsrp,sinr,mcs_index,iq_samples,pdu_data --periodicity-us 100000 --dump-file capture.jsonl --duration-s 10
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
| POSIX SHM `/e3_ran_buffers` | Agent writes, Manager reads | Bulk sample data (IQ, PDUs, H-estimates, SRS) referenced by offset/index fields in `indicationMessage` |

Full message shapes are in `e3_agent.cpp`'s `handleSetupRequest` /
`handleSubscriptionRequest` / `notifyDataReady` / `notifySrsDataReady`.

## Not included

This is a protocol/plumbing reference, not a RAN simulator: PUSCH/SRS content
is uniform random, not physically meaningful (no real channel, no real MCS
selection, no CRC failures). Swap `DataLake::pushPuschSlot` /
`pushSrsSlot` in `data_lake.cpp` for a trace-replay or a real data source to
get real signal statistics without touching the E3 interface.
