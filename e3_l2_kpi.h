/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Pure C header: the Layer-2 KPI data model and the C API a DU (or any
 * other non-C++ scheduler) uses to push per-slot KPIs into the E3 Agent.
 * This file has zero C++ dependency - #include it directly from a DU
 * written in plain C.
 *
 * Push model: once per TTI/slot, the DU fills an array of E3CellL2Info
 * (fixed-size, no heap allocation, no std::vector) and calls
 * e3_agent_update_l2_slot(). Everything downstream - the mutex-guarded
 * snapshot swap, the notifier thread, JSON serialization - is handled
 * internally by the agent; the DU never touches ZMQ or JSON.
 *
 * Sizing note: E3CellL2Info is dominated by PerLcidBytes (2 * MAX_LCID
 * uint64_t = 512 bytes per UE regardless of how many LCIDs are actually
 * active), so a full E3CellL2Info[E3_MAX_CELLS] array is tens of KB. Prefer
 * a static/global buffer over a large on-stack array if your calling
 * thread has a constrained stack.
 */

#ifndef E3_L2_KPI_H
#define E3_L2_KPI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Tunable bounds - rebuild both the agent and every caller if you change
 * these; they determine E3CellL2Info's layout. */
#define E3_MAX_LCID            32u  /* logical channels reported per UE */
#define E3_MAX_DL_HARQ_ROUNDS   4u  /* DL HARQ round-count buckets per UE */
#define E3_MAX_UES_PER_CELL    16u  /* UEs reported per cell per slot */
#define E3_MAX_CELLS            8u  /* cells reported per slot */

typedef struct {
    uint32_t dl_prb;
    uint32_t ul_prb;
    uint32_t dl_prb_retx;
    uint32_t ul_prb_retx;
} PrbStats;

typedef struct {
    uint64_t dl_aggr_tbs;
    uint64_t ul_aggr_tbs;
    uint64_t dl_curr_tbs;
    uint64_t ul_curr_tbs;
} TbsStats;

typedef struct {
    uint64_t dl_lc_bytes[E3_MAX_LCID];
    uint64_t ul_lc_bytes[E3_MAX_LCID];
} PerLcidBytes;

typedef struct {
    uint32_t dl_mcs;
    uint32_t ul_mcs;
} McsIndexStats;

typedef struct {
    uint16_t cqi;
} WbCqi;

typedef struct {
    uint32_t dl_harq_rounds[E3_MAX_DL_HARQ_ROUNDS];
    uint32_t dl_errors;
    uint32_t ul_errors;
    double dl_bler;
    double ul_bler;
} TbStats;

typedef struct {
    int16_t pusch_snr;
    int16_t pucch_snr;
} SnrStats;

typedef struct {
    uint64_t total_bsr;
} BsrStats;

typedef struct {
    int32_t phr;
} PhrStats;

/*
 * Per-UE L2 KPI snapshot for one slot. Field groups mirror prb_stats_t /
 * tbs_stats_t / per_lcid_bytes_t / mcs_index_stats_t / wb_cqi_t /
 * tb_stats_t / snr_stats_t / bsr_stats_t / phr_stats_t.
 *
 * is_valid / ue_index / tick from the DU's per_ue_per_slot_e3_stats_t are
 * intentionally not here: they're MAC-internal ring-buffer bookkeeping (a
 * validity flag and an array slot index into the MAC's own UE table), not
 * meaningful to an external E3 Manager - rnti already identifies the UE,
 * and slot timing is the sfn/slot passed to e3_agent_update_l2_slot().
 */
typedef struct {
    uint16_t rnti;
    PrbStats prb_stats;
    TbsStats tbs_stats;
    PerLcidBytes per_lcid_bytes;
    McsIndexStats mcs_stats;
    WbCqi wb_cqi;
    TbStats tb_stats;
    SnrStats snr_stats;
    BsrStats bsr_stats;
    PhrStats phr_stats;
} E3UeL2Stats;

/*
 * Per-cell grouping of UE L2 stats for one slot. Fixed-size array + valid
 * count - no heap allocation, safe to populate from a DU's real-time path.
 */
typedef struct {
    uint16_t cell_id;
    uint16_t n_ue;                        /* valid entries in ues[], <= E3_MAX_UES_PER_CELL */
    E3UeL2Stats ues[E3_MAX_UES_PER_CELL];
} E3CellL2Info;

/* Opaque handle to the underlying E3 Agent instance (a C++ DataLake under
 * the hood) - a DU never sees its layout, only ever holds the pointer. */
typedef struct E3AgentHandle E3AgentHandle;

/**
 * Creates the E3 Agent: binds the REQ/REP + PUB/SUB ZMQ sockets on the
 * given ports and starts its session/notifier threads. Returns NULL on
 * failure (e.g. a port already in use) - check stderr for the reason.
 */
E3AgentHandle* e3_agent_create(uint16_t rep_port, uint16_t pub_port, uint16_t sub_port);

/**
 * Push model: call this once per TTI/slot with the DU's real per-cell,
 * per-UE L2 KPIs. Copies the data into an internal mutex-guarded snapshot;
 * does not block on any subscriber and does not itself send anything - the
 * agent's own notifier thread sends indications to each subscription per
 * that subscription's configured periodicity, independent of how often
 * this is called.
 *
 * @param handle   Handle from e3_agent_create().
 * @param sfn      Current system frame number.
 * @param slot     Current slot number within the frame.
 * @param cells    Pointer to an array of n_cells populated E3CellL2Info.
 * @param n_cells  Number of valid entries in cells[]; values above
 *                 E3_MAX_CELLS are clamped (excess cells are dropped).
 * @return 0 on success, -1 if handle or cells is NULL.
 */
int e3_agent_update_l2_slot(E3AgentHandle* handle, uint16_t sfn, uint16_t slot,
                            const E3CellL2Info* cells, uint16_t n_cells);

/** Stops the agent's threads and releases it. `handle` is invalid after this call. */
void e3_agent_destroy(E3AgentHandle* handle);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* E3_L2_KPI_H */
