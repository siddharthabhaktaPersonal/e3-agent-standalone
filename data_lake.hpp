/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Standalone "data lake" for the E3 Agent reference process - Layer-2 (MAC)
 * KPI edition. `DataLake` holds exactly one slot's worth of per-UE L2 KPIs
 * in plain C structs (mirroring prb_stats_t / tbs_stats_t / per_lcid_bytes_t
 * / mcs_index_stats_t / wb_cqi_t / tb_stats_t / snr_stats_t / bsr_stats_t /
 * phr_stats_t), guarded by a mutex that E3Agent reads under as a friend.
 * There is no shared memory in this version - every field here is small
 * enough to travel in the indication's JSON protocolData, so pushL2Slot()
 * just swaps in a freshly built struct and calls notifyDataReady().
 */

#ifndef DATA_LAKE_H
#define DATA_LAKE_H

#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <random>

#include "e3_agent.hpp"

// ---------------------------------------------------------------------
// L2 KPI data model. Field names/types mirror the MAC-side structs this
// was specified against (UIntN/SIntN/SDouble32/tickType_t aliased to
// fixed-width C++ types below). is_valid/ue_index/tick from the source
// per_ue_per_slot_e3_stats_t are MAC-internal ring-buffer bookkeeping
// (a validity flag and an array slot index into the MAC's own UE table)
// and aren't meaningful to an external E3 Manager, so they're intentionally
// not part of the wire model - RNTI already identifies the UE, and slot
// timing is carried by the existing sfn/slot/timestamp fields.
// ---------------------------------------------------------------------

struct PrbStats {
    uint32_t dl_prb{};
    uint32_t ul_prb{};
    uint32_t dl_prb_retx{};
    uint32_t ul_prb_retx{};
};

struct TbsStats {
    uint64_t dl_aggr_tbs{};
    uint64_t ul_aggr_tbs{};
    uint64_t dl_curr_tbs{};
    uint64_t ul_curr_tbs{};
};

struct PerLcidBytes {
    uint64_t dl_lc_bytes[e3::MAX_LCID]{};
    uint64_t ul_lc_bytes[e3::MAX_LCID]{};
};

struct McsIndexStats {
    uint32_t dl_mcs{};
    uint32_t ul_mcs{};
};

struct WbCqi {
    uint16_t cqi{};
};

struct TbStats {
    uint32_t dl_harq_rounds[e3::MAX_DL_HARQ_ROUNDS]{};
    uint32_t dl_errors{};
    uint32_t ul_errors{};
    double dl_bler{};
    double ul_bler{};
};

struct SnrStats {
    int16_t pusch_snr{};
    int16_t pucch_snr{};
};

struct BsrStats {
    uint64_t total_bsr{};
};

struct PhrStats {
    int32_t phr{};
};

// Per-UE L2 KPI snapshot for one slot.
struct E3UeL2Stats {
    uint16_t rnti{};
    PrbStats prb_stats;
    TbsStats tbs_stats;
    PerLcidBytes per_lcid_bytes;
    McsIndexStats mcs_stats;
    WbCqi wb_cqi;
    TbStats tb_stats;
    SnrStats snr_stats;
    BsrStats bsr_stats;
    PhrStats phr_stats;
};

// Per-cell grouping of UE L2 stats for one slot.
struct E3CellL2Info {
    uint16_t cell_id{};
    uint16_t n_ue{};
    std::vector<E3UeL2Stats> ues;
};

// Slot-level L2 buffer info for E3 indications; one E3CellL2Info per active cell.
struct E3L2BufferInfo {
    uint16_t sfn{};
    uint16_t slot{};
    uint64_t timestamp_ns{};
    uint64_t timestamp_tai_ns{};
    uint16_t n_cells{};
    std::vector<E3CellL2Info> cells;
};

// A cell's synthetic-traffic shape for one slot: how many UEs to synthesize.
// The generator (DataLake) fills in the actual KPI values; callers just
// describe topology.
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

    // Synthesize and publish one slot of L2 KPIs, then fire notifyDataReady().
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
