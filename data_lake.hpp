/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Standalone "data lake" for the E3 Agent reference process - Layer-2 (MAC)
 * KPI edition. `DataLake` holds exactly one slot's worth of per-UE L2 KPIs
 * (see e3_l2_kpi.h for the field-level data model - it's a pure C header so
 * a DU can share the exact same struct layout), guarded by a mutex that
 * E3Agent reads under as a friend. There is no shared memory in this
 * version - every field is small enough to travel in the indication's JSON
 * protocolData.
 *
 * DataLake only keeps its snapshot fresh; it does not decide when to
 * notify anyone. E3Agent runs its own notifier thread that independently
 * reads the snapshot and sends an indication to each subscription according
 * to that subscription's own configured periodicity_us - the two cadences
 * (data refresh vs. notification) are decoupled on purpose.
 *
 * Two ways to keep the snapshot fresh:
 *  - updateL2Slot() - the push-model entry point real data arrives through.
 *    This is exactly what e3_agent_update_l2_slot() (e3_l2_kpi.h) calls into
 *    for a DU linking via the C API; C++ callers can call it directly too.
 *  - pushL2Slot() - a synthetic random-data generator for standalone testing
 *    without a real DU (what main.cpp's slot clock drives). Internally it
 *    just builds an E3CellL2Info[] and calls updateL2Slot() - one code path.
 */

#ifndef DATA_LAKE_H
#define DATA_LAKE_H

#include <cstdint>
#include <vector>
#include <mutex>
#include <memory>
#include <random>

#include "e3_l2_kpi.h"
#include "e3_agent.hpp"

// Slot-level L2 buffer info for E3 indications. Fixed-size array of cells
// (no heap allocation) - mirrors exactly what a DU passes to
// e3_agent_update_l2_slot().
struct E3L2BufferInfo {
    uint16_t sfn{};
    uint16_t slot{};
    uint64_t timestamp_ns{};
    uint64_t timestamp_tai_ns{};
    uint16_t n_cells{};
    E3CellL2Info cells[E3_MAX_CELLS]{};
};

// A cell's synthetic-traffic shape for one slot: how many UEs to synthesize.
// Only used by pushL2Slot()'s built-in random generator - not part of the
// DU-facing C API.
struct SlotCellTopology {
    uint16_t cell_id{};
    uint16_t n_ue{1};
};

class DataLake {
public:
    DataLake(
        bool e3AgentEnabled,
        uint16_t e3RepPort,
        uint16_t e3PubPort,
        uint16_t e3SubPort
    );
    ~DataLake();

    // Constructs E3Agent and starts its threads (no SHM to allocate in this
    // version). Returns false (and logs) on failure - typically a port
    // already bound.
    bool start();
    void stop();

    // Push-model entry point: copy externally-supplied L2 KPIs into the
    // current snapshot. Does NOT trigger any notification - E3Agent's own
    // notifier thread reads this snapshot independently. cells/n_cells
    // follow e3_agent_update_l2_slot()'s contract (n_cells above
    // E3_MAX_CELLS is clamped).
    void updateL2Slot(uint16_t sfn, uint16_t slot, const E3CellL2Info* cells, uint16_t n_cells);

    // Synthesize one slot of random L2 KPIs (for standalone testing without
    // a real DU) and store it via updateL2Slot().
    void pushL2Slot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& cells);

    E3Agent* agent() { return e3_agent.get(); }

private:
    friend class E3Agent;

    bool e3AgentEnabled_;
    uint16_t e3RepPort, e3PubPort, e3SubPort;

    E3L2BufferInfo e3_buffer_info;
    std::mutex e3_buffer_mutex;

    std::unique_ptr<E3Agent> e3_agent;

    std::mt19937 rng_{std::random_device{}()};
};

#endif // DATA_LAKE_H
