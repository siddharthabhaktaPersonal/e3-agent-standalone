/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Sample DU-side integration, in plain C - proves e3_l2_kpi.h has zero C++
 * dependency (this file is compiled with a C compiler, not C++).
 *
 * Shows the push model: once per TTI, build an array of E3CellL2Info from
 * real scheduler state and call e3_agent_update_l2_slot(). fabricate_cell_stats()
 * below fakes that scheduler state for the demo - a real DU replaces its body
 * with reads from its own MAC/scheduler structures and leaves everything
 * else (the e3_agent_create/update/destroy calls) unchanged.
 *
 * Usage: du_integration_example [rep_port] [pub_port] [sub_port]
 */

#include "e3_l2_kpi.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t g_running = 1;
static void on_signal(int sig) { (void)sig; g_running = 0; }

static void fabricate_cell_stats(E3CellL2Info* cell, uint16_t cell_id, uint16_t n_ue)
{
    uint16_t u, lc;
    memset(cell, 0, sizeof(*cell));
    cell->cell_id = cell_id;
    cell->n_ue = (n_ue > E3_MAX_UES_PER_CELL) ? E3_MAX_UES_PER_CELL : n_ue;

    for (u = 0; u < cell->n_ue; ++u) {
        E3UeL2Stats* ue = &cell->ues[u];
        ue->rnti = (uint16_t)(1000 + u);

        ue->prb_stats.dl_prb = 50 + (u * 7) % 200;
        ue->prb_stats.ul_prb = 30 + (u * 5) % 150;
        ue->prb_stats.dl_prb_retx = ue->prb_stats.dl_prb / 20;
        ue->prb_stats.ul_prb_retx = ue->prb_stats.ul_prb / 20;

        ue->tbs_stats.dl_curr_tbs = 20000 + u * 137;
        ue->tbs_stats.ul_curr_tbs = 10000 + u * 89;
        ue->tbs_stats.dl_aggr_tbs = ue->tbs_stats.dl_curr_tbs;
        ue->tbs_stats.ul_aggr_tbs = ue->tbs_stats.ul_curr_tbs;

        for (lc = 0; lc < E3_MAX_LCID; ++lc) {
            ue->per_lcid_bytes.dl_lc_bytes[lc] = (lc == 0) ? 500 : 0; /* SRB0 only, for demo */
            ue->per_lcid_bytes.ul_lc_bytes[lc] = (lc == 0) ? 200 : 0;
        }

        ue->mcs_stats.dl_mcs = 16;
        ue->mcs_stats.ul_mcs = 12;
        ue->wb_cqi.cqi = 10;

        ue->tb_stats.dl_errors = 0;
        ue->tb_stats.ul_errors = 0;
        ue->tb_stats.dl_bler = 0.01;
        ue->tb_stats.ul_bler = 0.02;

        ue->snr_stats.pusch_snr = 15;
        ue->snr_stats.pucch_snr = 18;

        ue->bsr_stats.total_bsr = 1024;
        ue->phr_stats.phr = 20;
    }
}

int main(int argc, char** argv)
{
    uint16_t rep_port = 5555, pub_port = 5556, sub_port = 5557;
    const uint16_t n_cells = 1, n_ue = 2;
    const unsigned slot_us = 500;
    E3AgentHandle* handle;
    /* static: E3CellL2Info is tens of KB (dominated by per-LCID byte
     * arrays) - prefer static/heap storage over a large on-stack array,
     * especially on a real-time thread with a constrained stack. */
    static E3CellL2Info cells[E3_MAX_CELLS];
    uint16_t sfn = 0, slot = 0, c;

    if (argc > 1) rep_port = (uint16_t)atoi(argv[1]);
    if (argc > 2) pub_port = (uint16_t)atoi(argv[2]);
    if (argc > 3) sub_port = (uint16_t)atoi(argv[3]);

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    handle = e3_agent_create(rep_port, pub_port, sub_port);
    if (!handle) {
        fprintf(stderr, "e3_agent_create failed - check ports %u/%u/%u\n", rep_port, pub_port, sub_port);
        return 1;
    }
    fprintf(stderr, "DU integration example running: REP=%u PUB=%u SUB=%u. Ctrl-C to stop.\n",
            rep_port, pub_port, sub_port);

    while (g_running) {
        struct timespec ts_start, ts_end;
        long elapsed_us;

        clock_gettime(CLOCK_MONOTONIC, &ts_start);

        for (c = 0; c < n_cells; ++c) {
            fabricate_cell_stats(&cells[c], (uint16_t)(c + 1), n_ue);
        }
        e3_agent_update_l2_slot(handle, sfn, slot, cells, n_cells);

        if (++slot >= 20) { slot = 0; ++sfn; }

        clock_gettime(CLOCK_MONOTONIC, &ts_end);
        elapsed_us = (ts_end.tv_sec - ts_start.tv_sec) * 1000000L +
                     (ts_end.tv_nsec - ts_start.tv_nsec) / 1000L;
        if (elapsed_us < (long)slot_us) {
            usleep((useconds_t)(slot_us - elapsed_us));
        }
    }

    fprintf(stderr, "Shutting down...\n");
    e3_agent_destroy(handle);
    return 0;
}
