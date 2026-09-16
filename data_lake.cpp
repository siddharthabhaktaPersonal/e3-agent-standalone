/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * See data_lake.hpp for the design summary.
 */

#include "data_lake.hpp"
#include <algorithm>
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
    e3_buffer_info.cells.reserve(64);

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

void DataLake::pushL2Slot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& cells)
{
    if (cells.empty()) return;
    const uint16_t nCells = static_cast<uint16_t>(cells.size());

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

    E3L2BufferInfo local;
    local.sfn = sfn;
    local.slot = slot;
    local.timestamp_ns = nowNs();
    local.timestamp_tai_ns = local.timestamp_ns; // no grandmaster clock in this standalone build
    local.n_cells = nCells;
    local.cells.resize(nCells);

    for (uint16_t c = 0; c < nCells; ++c) {
        E3CellL2Info& cell = local.cells[c];
        const SlotCellTopology& topo = cells[c];
        cell.cell_id = topo.cell_id;
        cell.ues.resize(topo.n_ue);

        for (uint16_t u = 0; u < topo.n_ue; ++u) {
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

            for (uint32_t lc = 0; lc < e3::MAX_LCID; ++lc) {
                ue.per_lcid_bytes.dl_lc_bytes[lc] = lcBytesDist(rng_);
                ue.per_lcid_bytes.ul_lc_bytes[lc] = lcBytesDist(rng_);
            }

            ue.mcs_stats.dl_mcs = mcsDist(rng_);
            ue.mcs_stats.ul_mcs = mcsDist(rng_);

            ue.wb_cqi.cqi = cqiDist(rng_);

            for (uint32_t r = 0; r < e3::MAX_DL_HARQ_ROUNDS; ++r) {
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
        cell.n_ue = static_cast<uint16_t>(cell.ues.size());
    }

    // Just update the snapshot; E3Agent's own notifier thread reads this
    // independently and sends indications per each subscription's
    // configured periodicity - no need to poke it here.
    std::lock_guard<std::mutex> lock(e3_buffer_mutex);
    e3_buffer_info = std::move(local);
}
