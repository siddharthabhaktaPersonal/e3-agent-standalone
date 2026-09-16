/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Implements the C API declared in e3_l2_kpi.h by wrapping DataLake/E3Agent.
 * This is the only file that knows E3AgentHandle is actually a DataLake* -
 * callers (C or C++) only ever see the opaque pointer.
 */

#include "e3_l2_kpi.h"
#include "data_lake.hpp"

extern "C" {

E3AgentHandle* e3_agent_create(uint16_t rep_port, uint16_t pub_port, uint16_t sub_port)
{
    auto* dataLake = new DataLake(/*e3AgentEnabled=*/true, rep_port, pub_port, sub_port);
    if (!dataLake->start()) {
        delete dataLake;
        return nullptr;
    }
    return reinterpret_cast<E3AgentHandle*>(dataLake);
}

int e3_agent_update_l2_slot(E3AgentHandle* handle, uint16_t sfn, uint16_t slot,
                            const E3CellL2Info* cells, uint16_t n_cells)
{
    if (!handle || !cells) return -1;
    reinterpret_cast<DataLake*>(handle)->updateL2Slot(sfn, slot, cells, n_cells);
    return 0;
}

void e3_agent_destroy(E3AgentHandle* handle)
{
    if (!handle) return;
    auto* dataLake = reinterpret_cast<DataLake*>(handle);
    dataLake->stop();
    delete dataLake;
}

} // extern "C"
