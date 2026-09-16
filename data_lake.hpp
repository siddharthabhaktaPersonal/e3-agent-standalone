/*
 * SPDX-FileCopyrightText: Portions Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Standalone "data lake" for the E3 Agent reference process.
 *
 * This replaces the production data_lake (ClickHouse + cuPHY-coupled slot
 * capture) with a plain in-process C/C++ data structure: two fixed-size
 * ping-pong row buffers per telemetry stream, written directly into the
 * POSIX shared-memory region that E3Agent::createSharedMemoryBuffers()
 * allocates. There is no database, no cuPHY dependency, and no link to
 * cuphydriver - this header plus data_lake.cpp compile into a standalone
 * Linux process alongside e3_agent.hpp/.cpp, which are used UNMODIFIED.
 *
 * The E3UeMetrics / E3CellInfo / E3BufferInfo / E3Srs* structs below are the
 * data model E3Agent reads through its `friend class E3Agent` access into
 * DataLake::e3_buffer_info / e3_srs_buffer_info - they are copied verbatim
 * from the production header because they are part of what notifyDataReady()
 * / notifySrsDataReady() serialize onto the wire; changing their field
 * meanings would silently break the E3AP indication schema documented in
 * e3_agent.cpp's handleSetupRequest().
 */

#ifndef DATA_LAKE_H
#define DATA_LAKE_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <random>

#include "e3_agent.hpp"

// ---------------------------------------------------------------------
// E3 indication data model (unchanged from production data_lake.hpp)
// ---------------------------------------------------------------------

// Per-UE PUSCH metrics, one entry per UE per slot across all UE groups.
struct E3UeMetrics {
    uint16_t rnti{};
    uint8_t tb_crc_fail{};
    uint32_t cb_errors{};
    float rsrp{};
    float noise_var{};
    float sinr{};
    uint16_t cb_count{};
    float rssi{};
    uint8_t qam_mod_order{};
    uint8_t mcs_index{};
    uint8_t mcs_table_index{};
    uint16_t rb_start{};
    uint16_t rb_size{};
    uint8_t start_symbol_index{};
    uint8_t nr_of_symbols{};
    uint32_t tb_size{};
    uint32_t pdu_len{};
    uint32_t pdu_offset{};
    uint16_t target_code_rate{};
    uint8_t new_data_indicator{};
    uint8_t n_layers{};
    uint16_t layer_offset{};
    uint16_t ue_grp_idx{};
    uint32_t h_offset{};
    uint32_t h_size{};
    uint16_t n_subcarriers{};
    uint8_t n_dmrs_estimates{};
    uint16_t dmrs_symb_pos{};
    float timing_advance{};
    float cfo_hz{};
    uint8_t harq_process_id{};
    uint8_t rv_index{};
};

// Per-cell buffer info and per-UE metrics for one cell within a slot.
struct E3CellInfo {
    uint16_t cell_id{};
    uint16_t n_rx_ant{};
    uint16_t n_rx_ant_srs{};
    uint8_t  n_bs_ants{};
    uint16_t n_ue{};

    uint8_t  current_fh_buffer{};
    uint32_t fh_write_index{};
    uint8_t  current_pusch_buffer{};
    uint32_t pusch_write_index{};
    uint8_t  current_hest_buffer{};
    uint32_t hest_write_index{};
    uint32_t hest_row_byte_offset{};

    std::vector<E3UeMetrics> ues;
};

// Slot-level buffer info for E3 indications; one E3CellInfo per active cell.
struct E3BufferInfo {
    uint16_t sfn{};
    uint16_t slot{};
    uint64_t timestamp_ns{};
    uint64_t timestamp_tai_ns{};
    uint16_t n_cells{};
    std::vector<E3CellInfo> cells;
};

// Per-UE SRS metrics for E3 indications
struct E3SrsUeMetrics {
    uint16_t rnti{};
    float wideband_snr{};
    float signal_energy{};
    float noise_energy{};
    float toa_us{};
    uint8_t hd_ant_flag{};
    float sc_corr_re{};
    float sc_corr_im{};
    float cs_corr_ratio_db{};
    uint8_t n_ant_ports{};
    uint8_t n_syms{};
    uint8_t n_repetitions{};
    uint8_t comb_size{};
    uint8_t comb_offset{};
    uint8_t start_sym{};
    uint8_t cyclic_shift{};
    uint8_t frequency_position{};
    uint16_t frequency_shift{};
    uint8_t frequency_hopping{};
    uint8_t resource_type{};
    uint16_t t_srs{};
    uint16_t t_offset{};
    uint32_t usage{};
    uint16_t n_valid_prg{};
    uint16_t prg_size{};
    uint16_t n_prb_grps{};
    uint32_t srs_hest_offset{};
    uint32_t srs_hest_size{};
    uint32_t srs_rb_snr_offset{};
    uint32_t srs_rb_snr_size{};
};

// Per-cell SRS buffer info and per-UE metrics for one cell within a slot.
struct E3SrsCellInfo {
    uint16_t cell_id{};
    uint16_t n_rx_ant_srs{};
    uint8_t  srs_cell_start_sym{};
    uint8_t  srs_cell_n_srs_sym{};
    uint16_t n_srs_ue{};

    uint8_t  current_srs_iq_buffer{};
    uint32_t srs_iq_write_index{};
    uint32_t srs_iq_row_byte_offset{};
    uint8_t  current_srs_hest_buffer{};
    uint32_t srs_hest_write_index{};
    uint8_t  current_srs_rb_snr_buffer{};
    uint32_t srs_rb_snr_write_index{};

    std::vector<E3SrsUeMetrics> ues;
};

// Slot-level SRS buffer info for E3 indications; one E3SrsCellInfo per active cell.
struct E3SrsBufferInfo {
    uint16_t sfn{};
    uint16_t slot{};
    uint64_t timestamp_ns{};
    uint64_t timestamp_tai_ns{};
    uint16_t n_cells{};
    std::vector<E3SrsCellInfo> cells;
};

// ---------------------------------------------------------------------
// H-estimate sample type. Production code uses cuFloatComplex (from
// cuPHY); we don't link cuPHY, so a POD equivalent stands in. Same size
// (8 bytes) and layout (re, im as adjacent float32), so the byte counts
// E3Agent computes from sizeof(hestDataType) stay correct on the wire.
// ---------------------------------------------------------------------
struct hestComplex { float re; float im; };
using hestDataType = hestComplex;

// ---------------------------------------------------------------------
// Ping-pong SHM row buffers. E3Agent::createSharedMemoryBuffers() (in the
// unmodified e3_agent.cpp) only ever touches ->pDataAlloc / ->pRbSnrDataAlloc
// on these, so that's all it needs from us.
// ---------------------------------------------------------------------
struct fhInfo_t      { int16_t*      pDataAlloc = nullptr; };
struct puschInfo_t   { uint8_t*      pDataAlloc = nullptr; };
struct hestInfo_t    { hestDataType* pDataAlloc = nullptr; };
struct srsIqInfo_t   { int16_t*      pDataAlloc = nullptr; };
struct srsInfo_t      { float*        pRbSnrDataAlloc = nullptr; };
struct srsHestInfo_t { int16_t*      pDataAlloc = nullptr; };

// A cell's synthetic-traffic shape for one slot: how many antennas it
// reports and how many UEs to synthesize. The generator (DataLake) fills in
// the actual sample values; callers just describe topology.
struct SlotCellTopology {
    uint16_t cell_id{};
    uint16_t n_rx_ant{4};
    uint16_t n_rx_ant_srs{4};
    uint8_t  n_bs_ants{4};
    uint16_t n_ue{1};
};

class DataLake {
public:
    DataLake(
        bool e3AgentEnabled,
        uint16_t e3RepPort,
        uint16_t e3PubPort,
        uint16_t e3SubPort,
        uint32_t numFhSamples          = 8192,   // int16 samples/row (FH IQ, per cell)
        uint32_t maxPuschPduSize       = 16384,  // bytes/row (PUSCH PDU, whole slot)
        uint32_t maxHestSamplesPerRow  = 4096,   // complex samples/row (PUSCH H-est, per cell)
        uint32_t maxSrsIqSamplesPerRow = 4096,   // int16 samples/row (SRS IQ, per cell)
        uint32_t maxSrsHestBytesPerRow = 4096,   // bytes/row (SRS H-est, per UE)
        uint32_t maxSrsRbSnrBytesPerRow = 1100,  // bytes/row (SRS RbSNR, per UE)
        int numRowsToInsertFh      = 16,
        int numRowsToInsertPusch   = 32,
        int numRowsToInsertHest    = 16,
        int numRowsToInsertSrsIq   = 16,
        int numRowsToInsertSrs     = 32,
        int numRowsToInsertSrsHest = 16
    );
    ~DataLake();

    // Allocates SHM, constructs E3Agent, starts its threads. Returns false
    // (and logs) on failure - typically ports already bound or shm_open denied.
    bool start();
    void stop();

    // Synthesize and publish one slot of PUSCH-path telemetry (FH IQ, PUSCH
    // PDUs, H-estimates + per-UE metrics), then fire notifyDataReady().
    void pushPuschSlot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& cells);

    // Synthesize and publish one slot of SRS-path telemetry (SRS IQ, SRS
    // H-estimates, SRS RbSNR + per-UE metrics), then fire notifySrsDataReady().
    void pushSrsSlot(uint16_t sfn, uint16_t slot, const std::vector<SlotCellTopology>& cells, uint16_t nSrsUePerCell);

    E3Agent* agent() { return e3_agent.get(); }

private:
    friend class E3Agent;

    bool e3AgentEnabled_;
    uint16_t e3RepPort, e3PubPort, e3SubPort;

    uint32_t numFhSamples;
    uint32_t maxPuschPduSize;
    uint32_t maxHestSamplesPerRow;
    uint32_t maxSrsIqSamplesPerRow;
    uint32_t maxSrsHestBytesPerRow;
    uint32_t maxSrsRbSnrBytesPerRow;

    int numRowsToInsertFh;
    int numRowsToInsertPusch;
    int numRowsToInsertHest;
    int numRowsToInsertSrsIq;
    int numRowsToInsertSrs;
    int numRowsToInsertSrsHest;

    // Ping-pong pairs. Index 0/1 is what E3CellInfo::current_*_buffer reports.
    fhInfo_t      fhInfo[2];
    puschInfo_t   puschInfo[2];
    hestInfo_t    hestInfo[2];
    srsIqInfo_t   srsIqInfo[2];
    srsInfo_t     srsInfo[2];
    srsHestInfo_t srsHestInfo[2];

    // Regular-mode (E3 disabled) heap fallback owns the same buffers when
    // there's no SHM to point into.
    std::vector<std::vector<int16_t>>      fhHeap;
    std::vector<std::vector<uint8_t>>      puschHeap;
    std::vector<std::vector<hestDataType>> hestHeap;
    std::vector<std::vector<int16_t>>      srsIqHeap;
    std::vector<std::vector<float>>        srsRbSnrHeap;
    std::vector<std::vector<int16_t>>      srsHestHeap;

    // Write cursors, one ping-pong pair each. Row cursor gates on
    // numRowsToInsert*; byte cursor (where present) tracks the running
    // offset for variable-length per-row content within the active buffer.
    uint8_t  curFhBuf = 0;      uint32_t fhRowCursor = 0;
    uint8_t  curPuschBuf = 0;   uint32_t puschRowCursor = 0;
    uint8_t  curHestBuf = 0;    uint32_t hestRowCursor = 0;   size_t hestByteCursor = 0;
    uint8_t  curSrsIqBuf = 0;   uint32_t srsIqRowCursor = 0;
    uint8_t  curSrsHestBuf = 0; uint32_t srsHestRowCursor = 0; size_t srsHestByteCursor = 0;
    uint8_t  curSrsRbSnrBuf = 0; uint32_t srsRbSnrRowCursor = 0; size_t srsRbSnrByteCursor = 0;

    E3BufferInfo e3_buffer_info;
    std::mutex e3_buffer_mutex;
    E3SrsBufferInfo e3_srs_buffer_info;
    std::mutex e3_srs_buffer_mutex;

    std::unique_ptr<E3Agent> e3_agent;

    std::mt19937 rng_{std::random_device{}()};

    void allocateBuffers();
    void fillRandomI16(int16_t* dst, size_t count, int16_t lo, int16_t hi);
};

#endif // DATA_LAKE_H
