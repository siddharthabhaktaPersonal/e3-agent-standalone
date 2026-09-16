/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * See data_lake.hpp for the design summary.
 */

#include "data_lake.hpp"
#include <algorithm>
#include <cstring>
#include <ctime>

#define TAG_DATALAKE (NVLOG_TAG_BASE_CUPHY_CONTROLLER + 6)

DataLake::DataLake(
    bool e3AgentEnabled,
    uint16_t repPort,
    uint16_t pubPort,
    uint16_t subPort
)
{
    e3AgentEnabled_ = e3AgentEnabled;
    e3RepPort = repPort;
    e3PubPort = pubPort;
    e3SubPort = subPort;
}

DataLake::~DataLake()
{
    stop();
}

bool DataLake::start()
{
    if (!e3AgentEnabled_) {
        NVLOGC_FMT(TAG_DATALAKE, "DataLake running without E3 Agent (disabled)");
        return true;
    }

    e3_agent = std::make_unique<E3Agent>(this, e3RepPort, e3PubPort, e3SubPort);

    if (!e3_agent->init()) {
        NVLOGE_FMT(TAG_DATALAKE, AERIAL_SYSTEM_API_EVENT,
            "Failed to init E3 Agent (check ports {}/{}/{} are free)", e3RepPort, e3PubPort, e3SubPort);
        e3_agent.reset();
        return false;
    }

    NVLOGC_FMT(TAG_DATALAKE, "E3 Agent started - REP:{} PUB:{} SUB:{}", e3RepPort, e3PubPort, e3SubPort);
    return true;
}

void DataLake::stop()
{
    if (e3_agent) {
        e3_agent->shutdown();
    }
}

static uint64_t nowNs()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * UINT64_C(1000000000) + static_cast<uint64_t>(ts.tv_nsec);
}

// Push-model entry point. Writes straight into e3_buffer_info under the
// lock - deliberately no local E3L2BufferInfo staging copy, since that
// struct's fixed E3_MAX_CELLS * E3_MAX_UES_PER_CELL arrays make a by-value
// copy tens of KB; writing element-by-element avoids paying for that on
// every call (this can be called at slot cadence, i.e. hundreds of Hz).
void DataLake::updateL2Slot(uint16_t sfn, uint16_t slot, const E3CellL2Info* cells, uint16_t n_cells)
{
    if (!cells) return;
    if (n_cells > E3_MAX_CELLS) {
        NVLOGW_FMT(TAG_DATALAKE, "updateL2Slot: n_cells {} exceeds E3_MAX_CELLS {}, clamping", n_cells, E3_MAX_CELLS);
        n_cells = E3_MAX_CELLS;
    }

    std::lock_guard<std::mutex> lock(e3_buffer_mutex);
    e3_buffer_info.sfn = sfn;
    e3_buffer_info.slot = slot;
    e3_buffer_info.timestamp_ns = nowNs();
    e3_buffer_info.timestamp_tai_ns = e3_buffer_info.timestamp_ns; // no grandmaster clock in this standalone build
    e3_buffer_info.n_cells = n_cells;
    for (uint16_t c = 0; c < n_cells; ++c) {
        e3_buffer_info.cells[c] = cells[c]; // POD struct copy
    }
}

void DataLake::pushL2Slot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& topo)
{
    if (topo.empty()) return;
    const uint16_t nCells = std::min<uint16_t>(static_cast<uint16_t>(topo.size()), E3_MAX_CELLS);

    std::uniform_int_distribution<uint32_t> prbDist(0, 273);
    std::uniform_int_distribution<uint32_t> prbRetxDist(0, 20);
    std::uniform_int_distribution<uint64_t> tbsDist(0, 1'000'000);
    std::uniform_int_distribution<uint64_t> lcBytesDist(0, 50'000);
    std::uniform_int_distribution<uint32_t> mcsDist(0, 27);
    std::uniform_int_distribution<uint16_t> cqiDist(0, 15);
    std::uniform_int_distribution<uint32_t> harqRoundDist(0, 10);
    std::uniform_int_distribution<uint32_t> errDist(0, 5);
    std::uniform_real_distribution<double> blerDist(0.0, 0.1);
    std::uniform_int_distribution<int16_t> snrDist(-10, 30);
    std::uniform_int_distribution<uint64_t> bsrDist(0, 200'000);
    std::uniform_int_distribution<int32_t> phrDist(-20, 40);
    std::uniform_int_distribution<int> rntiDist(1, 65535);

    static thread_local E3CellL2Info cells[E3_MAX_CELLS];
    std::memset(cells, 0, sizeof(cells));

    for (uint16_t c = 0; c < nCells; ++c) {
        E3CellL2Info& cell = cells[c];
        const SlotCellTopology& t = topo[c];
        cell.cell_id = t.cell_id;
        cell.n_ue = std::min<uint16_t>(t.n_ue, E3_MAX_UES_PER_CELL);

        for (uint16_t u = 0; u < cell.n_ue; ++u) {
            E3UeL2Stats& ue = cell.ues[u];
            ue.rnti = static_cast<uint16_t>(rntiDist(rng_));

            ue.prb_stats.dl_prb = prbDist(rng_);
            ue.prb_stats.ul_prb = prbDist(rng_);
            ue.prb_stats.dl_prb_retx = std::min(prbRetxDist(rng_), ue.prb_stats.dl_prb);
            ue.prb_stats.ul_prb_retx = std::min(prbRetxDist(rng_), ue.prb_stats.ul_prb);

            ue.tbs_stats.dl_curr_tbs = tbsDist(rng_);
            ue.tbs_stats.ul_curr_tbs = tbsDist(rng_);
            ue.tbs_stats.dl_aggr_tbs = ue.tbs_stats.dl_curr_tbs * (sfn * 20u + slot + 1u);
            ue.tbs_stats.ul_aggr_tbs = ue.tbs_stats.ul_curr_tbs * (sfn * 20u + slot + 1u);

            for (uint32_t lc = 0; lc < E3_MAX_LCID; ++lc) {
                ue.per_lcid_bytes.dl_lc_bytes[lc] = lcBytesDist(rng_);
                ue.per_lcid_bytes.ul_lc_bytes[lc] = lcBytesDist(rng_);
            }

            ue.mcs_stats.dl_mcs = mcsDist(rng_);
            ue.mcs_stats.ul_mcs = mcsDist(rng_);

            ue.wb_cqi.cqi = cqiDist(rng_);

            for (uint32_t r = 0; r < E3_MAX_DL_HARQ_ROUNDS; ++r) {
                ue.tb_stats.dl_harq_rounds[r] = harqRoundDist(rng_);
            }
            ue.tb_stats.dl_errors = errDist(rng_);
            ue.tb_stats.ul_errors = errDist(rng_);
            ue.tb_stats.dl_bler = blerDist(rng_);
            ue.tb_stats.ul_bler = blerDist(rng_);

            ue.snr_stats.pusch_snr = snrDist(rng_);
            ue.snr_stats.pucch_snr = snrDist(rng_);

            ue.bsr_stats.total_bsr = bsrDist(rng_);

            ue.phr_stats.phr = phrDist(rng_);
        }
    }

    updateL2Slot(sfn, slot, cells, nCells);
}
