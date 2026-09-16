/*
 * SPDX-FileCopyrightText: Portions Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 *
 * See data_lake.hpp for the design summary. This file owns the ping-pong SHM
 * row buffers and synthesizes plausible per-slot telemetry into them; it has
 * no ClickHouse, no cuPHY, and no cuphydriver dependency.
 */

#include "data_lake.hpp"
#include <algorithm>
#include <ctime>

#define TAG_DATALAKE (NVLOG_TAG_BASE_CUPHY_CONTROLLER + 6)

DataLake::DataLake(
    bool e3AgentEnabled,
    uint16_t repPort,
    uint16_t pubPort,
    uint16_t subPort,
    uint32_t fhSamples,
    uint32_t puschPduSize,
    uint32_t hestSamplesPerRow,
    uint32_t srsIqSamplesPerRow,
    uint32_t srsHestBytesPerRow,
    uint32_t srsRbSnrBytesPerRow,
    int rowsFh,
    int rowsPusch,
    int rowsHest,
    int rowsSrsIq,
    int rowsSrs,
    int rowsSrsHest
)
{
    e3AgentEnabled_ = e3AgentEnabled;
    e3RepPort = repPort;
    e3PubPort = pubPort;
    e3SubPort = subPort;

    numFhSamples = fhSamples;
    maxPuschPduSize = puschPduSize;
    maxHestSamplesPerRow = hestSamplesPerRow;
    maxSrsIqSamplesPerRow = srsIqSamplesPerRow;
    maxSrsHestBytesPerRow = srsHestBytesPerRow;
    maxSrsRbSnrBytesPerRow = srsRbSnrBytesPerRow;

    numRowsToInsertFh = rowsFh;
    numRowsToInsertPusch = rowsPusch;
    numRowsToInsertHest = rowsHest;
    numRowsToInsertSrsIq = rowsSrsIq;
    numRowsToInsertSrs = rowsSrs;
    numRowsToInsertSrsHest = rowsSrsHest;
}

DataLake::~DataLake()
{
    stop();
}

void DataLake::allocateBuffers()
{
    fhHeap.assign(2, std::vector<int16_t>(static_cast<size_t>(numFhSamples) * numRowsToInsertFh));
    puschHeap.assign(2, std::vector<uint8_t>(static_cast<size_t>(maxPuschPduSize) * numRowsToInsertPusch));
    hestHeap.assign(2, std::vector<hestDataType>(static_cast<size_t>(maxHestSamplesPerRow) * numRowsToInsertHest));
    srsIqHeap.assign(2, std::vector<int16_t>(static_cast<size_t>(maxSrsIqSamplesPerRow) * numRowsToInsertSrsIq));
    srsRbSnrHeap.assign(2, std::vector<float>((maxSrsRbSnrBytesPerRow / sizeof(float)) * numRowsToInsertSrs));
    srsHestHeap.assign(2, std::vector<int16_t>((maxSrsHestBytesPerRow / sizeof(int16_t)) * numRowsToInsertSrsHest));

    for (int i = 0; i < 2; ++i) {
        fhInfo[i].pDataAlloc = fhHeap[i].data();
        puschInfo[i].pDataAlloc = puschHeap[i].data();
        hestInfo[i].pDataAlloc = hestHeap[i].data();
        srsIqInfo[i].pDataAlloc = srsIqHeap[i].data();
        srsInfo[i].pRbSnrDataAlloc = srsRbSnrHeap[i].data();
        srsHestInfo[i].pDataAlloc = srsHestHeap[i].data();
    }
    NVLOGC_FMT(TAG_DATALAKE, "DataLake buffers heap-allocated (E3 agent disabled)");
}

bool DataLake::start()
{
    if (!e3AgentEnabled_) {
        allocateBuffers();
        return true;
    }

    e3_agent = std::make_unique<E3Agent>(
        this, e3RepPort, e3PubPort, e3SubPort,
        numRowsToInsertFh, numRowsToInsertPusch, numRowsToInsertHest,
        numFhSamples, maxPuschPduSize, maxHestSamplesPerRow,
        numRowsToInsertSrsIq, numRowsToInsertSrs, numRowsToInsertSrsHest,
        maxSrsIqSamplesPerRow, maxSrsHestBytesPerRow, maxSrsRbSnrBytesPerRow
    );

    e3_buffer_info.cells.reserve(64);
    e3_srs_buffer_info.cells.reserve(64);

    fhInfo_t*      pFh          = &fhInfo[0];
    fhInfo_t*      pInsertFh    = &fhInfo[1];
    puschInfo_t*   pPusch       = &puschInfo[0];
    puschInfo_t*   pInsertPusch = &puschInfo[1];
    hestInfo_t*    pHest        = &hestInfo[0];
    hestInfo_t*    pInsertHest  = &hestInfo[1];
    srsIqInfo_t*   pSrsIq       = &srsIqInfo[0];
    srsIqInfo_t*   pInsertSrsIq = &srsIqInfo[1];
    srsInfo_t*     pSrs         = &srsInfo[0];
    srsInfo_t*     pInsertSrs   = &srsInfo[1];
    srsHestInfo_t* pSrsHest       = &srsHestInfo[0];
    srsHestInfo_t* pInsertSrsHest = &srsHestInfo[1];

    if (!e3_agent->createSharedMemoryBuffers(&pFh, &pInsertFh, &pPusch, &pInsertPusch,
            &pHest, &pInsertHest, &pSrsIq, &pInsertSrsIq, &pSrs, &pInsertSrs,
            &pSrsHest, &pInsertSrsHest)) {
        NVLOGE_FMT(TAG_DATALAKE, AERIAL_SYSTEM_API_EVENT, "Failed to create E3 shared memory buffers");
        e3_agent.reset();
        return false;
    }

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

void DataLake::fillRandomI16(int16_t* dst, size_t count, int16_t lo, int16_t hi)
{
    if (!dst || count == 0) return;
    std::uniform_int_distribution<int> dist(lo, hi);
    for (size_t i = 0; i < count; ++i) {
        dst[i] = static_cast<int16_t>(dist(rng_));
    }
}

static uint64_t nowNs()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * UINT64_C(1000000000) + static_cast<uint64_t>(ts.tv_nsec);
}

void DataLake::pushPuschSlot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& cells)
{
    if (cells.empty()) return;
    const uint16_t nCells = static_cast<uint16_t>(cells.size());

    // Ping-pong flip: when the next slot wouldn't fit in the active buffer,
    // switch to the other half and reset that stream's cursor(s). This is
    // the same "current_*_buffer flips, consumer reads the other half"
    // contract the production data_lake and dApps rely on - it just isn't
    // gated on a DB flush completing, since there's no DB here.
    if (fhRowCursor + nCells > static_cast<uint32_t>(numRowsToInsertFh)) {
        curFhBuf ^= 1;
        fhRowCursor = 0;
    }
    if (puschRowCursor + 1 > static_cast<uint32_t>(numRowsToInsertPusch)) {
        curPuschBuf ^= 1;
        puschRowCursor = 0;
    }
    if (hestRowCursor + nCells > static_cast<uint32_t>(numRowsToInsertHest)) {
        curHestBuf ^= 1;
        hestRowCursor = 0;
        hestByteCursor = 0;
    }

    int16_t*      fhBase    = fhInfo[curFhBuf].pDataAlloc;
    uint8_t*      puschBase = puschInfo[curPuschBuf].pDataAlloc;
    hestDataType* hestBase  = hestInfo[curHestBuf].pDataAlloc;

    const uint32_t fhRowStart   = fhRowCursor;
    const uint32_t puschRow     = puschRowCursor;
    const uint32_t hestRowStart = hestRowCursor;
    const size_t   hestByteStart = hestByteCursor;

    uint32_t puschByteCursor = 0; // this slot's PDUs share one PUSCH row
    uint32_t hestPrefixBytes = 0; // running byte offset across this slot's cells

    std::uniform_real_distribution<float> rsrpDist(-110.0f, -70.0f);
    std::uniform_real_distribution<float> sinrDist(-5.0f, 30.0f);
    std::uniform_real_distribution<float> hestDist(-1.0f, 1.0f);
    std::uniform_int_distribution<int>    rntiDist(1, 65535);
    std::uniform_int_distribution<int>    byteDist(0, 255);

    E3BufferInfo local;
    local.sfn = sfn;
    local.slot = slot;
    local.timestamp_ns = nowNs();
    local.timestamp_tai_ns = local.timestamp_ns; // no grandmaster clock in this standalone build
    local.n_cells = nCells;
    local.cells.resize(nCells);

    for (uint16_t c = 0; c < nCells; ++c) {
        E3CellInfo& cell = local.cells[c];
        const SlotCellTopology& topo = cells[c];
        cell.cell_id      = topo.cell_id;
        cell.n_rx_ant      = topo.n_rx_ant;
        cell.n_rx_ant_srs  = topo.n_rx_ant_srs;
        cell.n_bs_ants     = topo.n_bs_ants;

        cell.current_fh_buffer = curFhBuf;
        cell.fh_write_index    = fhRowStart + c;
        if (fhBase) {
            fillRandomI16(fhBase + static_cast<size_t>(fhRowStart + c) * numFhSamples, numFhSamples, -2048, 2047);
        }

        cell.current_pusch_buffer = curPuschBuf;
        cell.pusch_write_index    = puschRow;

        const uint32_t hestSamples = std::min<uint32_t>(maxHestSamplesPerRow, 256u * (1u + (c % 4u)));
        cell.current_hest_buffer  = curHestBuf;
        cell.hest_write_index     = hestRowStart + c;
        cell.hest_row_byte_offset = static_cast<uint32_t>(hestByteStart) + hestPrefixBytes;
        if (hestBase) {
            hestDataType* hestDst = reinterpret_cast<hestDataType*>(
                reinterpret_cast<uint8_t*>(hestBase) + hestByteStart + hestPrefixBytes);
            for (uint32_t k = 0; k < hestSamples; ++k) {
                hestDst[k].re = hestDist(rng_);
                hestDst[k].im = hestDist(rng_);
            }
        }
        hestPrefixBytes += hestSamples * sizeof(hestDataType);

        cell.ues.resize(topo.n_ue);
        for (uint16_t u = 0; u < topo.n_ue; ++u) {
            E3UeMetrics& m = cell.ues[u];
            m.rnti = static_cast<uint16_t>(rntiDist(rng_));
            m.tb_crc_fail = 0;
            m.cb_errors = 0;
            m.rsrp = rsrpDist(rng_);
            m.noise_var = -20.0f;
            m.sinr = sinrDist(rng_);
            m.cb_count = 4;
            m.rssi = -60.0f;
            m.qam_mod_order = 6;
            m.mcs_index = 16;
            m.mcs_table_index = 1;
            m.rb_start = 0;
            m.rb_size = 50;
            m.start_symbol_index = 0;
            m.nr_of_symbols = 14;
            m.n_layers = 1;
            m.target_code_rate = 6660;
            m.new_data_indicator = 1;
            m.layer_offset = 0;
            m.ue_grp_idx = u;
            m.h_offset = 0;
            m.h_size = hestSamples;
            m.n_subcarriers = m.rb_size * 12;
            m.n_dmrs_estimates = 2;
            m.dmrs_symb_pos = 0x0800;
            m.timing_advance = 0.0f;
            m.cfo_hz = 0.0f;
            m.harq_process_id = static_cast<uint8_t>(u % 16);
            m.rv_index = 0;

            const uint32_t pduLen = (puschByteCursor < maxPuschPduSize)
                ? std::min<uint32_t>(maxPuschPduSize - puschByteCursor, 256)
                : 0;
            m.tb_size = pduLen;
            m.pdu_len = pduLen;
            m.pdu_offset = puschByteCursor;
            if (pduLen > 0 && puschBase) {
                uint8_t* dst = puschBase + static_cast<size_t>(puschRow) * maxPuschPduSize + puschByteCursor;
                for (uint32_t b = 0; b < pduLen; ++b) dst[b] = static_cast<uint8_t>(byteDist(rng_));
            }
            puschByteCursor += pduLen;
        }
        cell.n_ue = static_cast<uint16_t>(cell.ues.size());
    }

    fhRowCursor += nCells;
    puschRowCursor += 1;
    hestRowCursor += nCells;
    hestByteCursor += hestPrefixBytes;

    {
        std::lock_guard<std::mutex> lock(e3_buffer_mutex);
        e3_buffer_info = std::move(local);
    }

    if (e3_agent) {
        e3_agent->notifyDataReady();
    }
}

void DataLake::pushSrsSlot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& cells, uint16_t nSrsUePerCell)
{
    if (cells.empty()) return;
    const uint16_t nCells = static_cast<uint16_t>(cells.size());
    const uint32_t totalUes = static_cast<uint32_t>(nCells) * nSrsUePerCell;

    if (srsIqRowCursor + nCells > static_cast<uint32_t>(numRowsToInsertSrsIq)) {
        curSrsIqBuf ^= 1;
        srsIqRowCursor = 0;
    }
    // SRS Hest / RbSNR are per-UE row streams (see data_lake.hpp srsHestInfo_t /
    // srsInfo_t comments): capacity is gated on UE-rows-per-slot, not cells.
    if (srsHestRowCursor + totalUes > static_cast<uint32_t>(numRowsToInsertSrsHest)) {
        curSrsHestBuf ^= 1;
        srsHestRowCursor = 0;
        srsHestByteCursor = 0;
    }
    if (srsRbSnrRowCursor + totalUes > static_cast<uint32_t>(numRowsToInsertSrs)) {
        curSrsRbSnrBuf ^= 1;
        srsRbSnrRowCursor = 0;
        srsRbSnrByteCursor = 0;
    }

    int16_t* srsIqBase   = srsIqInfo[curSrsIqBuf].pDataAlloc;
    int16_t* srsHestBase = srsHestInfo[curSrsHestBuf].pDataAlloc;
    float*   srsRbSnrBase = srsInfo[curSrsRbSnrBuf].pRbSnrDataAlloc;

    const uint32_t srsIqRowStart = srsIqRowCursor;
    uint32_t srsHestRow  = srsHestRowCursor;
    uint32_t srsRbSnrRow = srsRbSnrRowCursor;
    size_t   srsHestByte  = srsHestByteCursor;
    size_t   srsRbSnrByte = srsRbSnrByteCursor;

    std::uniform_real_distribution<float> snrDist(0.0f, 30.0f);
    std::uniform_real_distribution<float> rbDist(0.0f, 30.0f);
    std::uniform_int_distribution<int>    rntiDist(1, 65535);

    E3SrsBufferInfo local;
    local.sfn = sfn;
    local.slot = slot;
    local.timestamp_ns = nowNs();
    local.timestamp_tai_ns = local.timestamp_ns;
    local.n_cells = nCells;
    local.cells.resize(nCells);

    for (uint16_t c = 0; c < nCells; ++c) {
        E3SrsCellInfo& cell = local.cells[c];
        const SlotCellTopology& topo = cells[c];
        cell.cell_id     = topo.cell_id;
        cell.n_rx_ant_srs = topo.n_rx_ant_srs;
        cell.srs_cell_start_sym = 10;
        cell.srs_cell_n_srs_sym = 4;

        cell.current_srs_iq_buffer  = curSrsIqBuf;
        cell.srs_iq_write_index     = srsIqRowStart + c;
        cell.srs_iq_row_byte_offset = static_cast<uint32_t>(static_cast<size_t>(srsIqRowStart + c) * maxSrsIqSamplesPerRow * sizeof(int16_t));
        if (srsIqBase) {
            fillRandomI16(srsIqBase + static_cast<size_t>(srsIqRowStart + c) * maxSrsIqSamplesPerRow, maxSrsIqSamplesPerRow, -2048, 2047);
        }

        cell.current_srs_hest_buffer   = curSrsHestBuf;
        cell.srs_hest_write_index      = srsHestRow;
        cell.current_srs_rb_snr_buffer = curSrsRbSnrBuf;
        cell.srs_rb_snr_write_index    = srsRbSnrRow;

        cell.ues.resize(nSrsUePerCell);
        for (uint16_t u = 0; u < nSrsUePerCell; ++u) {
            E3SrsUeMetrics& m = cell.ues[u];
            m.rnti = static_cast<uint16_t>(rntiDist(rng_));
            m.wideband_snr = snrDist(rng_);
            m.signal_energy = 100.0f;
            m.noise_energy = 1.0f;
            m.toa_us = 0.1f;
            m.hd_ant_flag = 0;
            m.sc_corr_re = 0.5f;
            m.sc_corr_im = 0.1f;
            m.cs_corr_ratio_db = 10.0f;
            m.n_ant_ports = 1;
            m.n_syms = 1;
            m.n_repetitions = 1;
            m.comb_size = 2;
            m.comb_offset = 0;
            m.start_sym = 10;
            m.cyclic_shift = 0;
            m.frequency_position = 0;
            m.frequency_shift = 0;
            m.frequency_hopping = 0;
            m.resource_type = 0;
            m.t_srs = 10;
            m.t_offset = 0;
            m.usage = 1;
            m.n_valid_prg = 25;
            m.prg_size = 4;
            m.n_prb_grps = 25;

            const uint32_t hestBytes = std::min<uint32_t>(maxSrsHestBytesPerRow, 512);
            m.srs_hest_offset = static_cast<uint32_t>(srsHestByte);
            m.srs_hest_size = hestBytes;
            if (srsHestBase && hestBytes > 0) {
                fillRandomI16(srsHestBase + srsHestByte / sizeof(int16_t), hestBytes / sizeof(int16_t), -2048, 2047);
            }
            srsHestByte += hestBytes;
            ++srsHestRow;

            const uint32_t rbSnrBytes = std::min<uint32_t>(maxSrsRbSnrBytesPerRow, static_cast<uint32_t>(m.n_valid_prg * sizeof(float)));
            m.srs_rb_snr_offset = static_cast<uint32_t>(srsRbSnrByte);
            m.srs_rb_snr_size = rbSnrBytes;
            if (srsRbSnrBase && rbSnrBytes > 0) {
                float* dst = reinterpret_cast<float*>(reinterpret_cast<uint8_t*>(srsRbSnrBase) + srsRbSnrByte);
                for (uint32_t k = 0; k < rbSnrBytes / sizeof(float); ++k) dst[k] = rbDist(rng_);
            }
            srsRbSnrByte += rbSnrBytes;
            ++srsRbSnrRow;
        }
        cell.n_srs_ue = static_cast<uint16_t>(cell.ues.size());
    }

    srsIqRowCursor += nCells;
    srsHestRowCursor = srsHestRow;
    srsHestByteCursor = srsHestByte;
    srsRbSnrRowCursor = srsRbSnrRow;
    srsRbSnrByteCursor = srsRbSnrByte;

    {
        std::lock_guard<std::mutex> lock(e3_srs_buffer_mutex);
        e3_srs_buffer_info = std::move(local);
    }

    if (e3_agent) {
        e3_agent->notifySrsDataReady();
    }
}
