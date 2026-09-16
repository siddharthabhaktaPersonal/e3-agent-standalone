/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Standalone E3 Agent process.
 *
 * Runs the unmodified production E3 interface (e3_agent.hpp/.cpp) against a
 * synthetic slot-clock feeder instead of a live cuPHY pipeline. No GPU, no
 * fronthaul, no ClickHouse - this is meant to run on a plain Linux box so an
 * E3 Manager / dApp can be developed and tested against a real E3AP session
 * (setup -> subscribe -> indications over SHM) without a RAN.
 *
 * Usage:
 *   e3_agent_standalone [--rep-port P] [--pub-port P] [--sub-port P]
 *                        [--cells N] [--ues N] [--slot-us US]
 *                        [--srs-period N] [--srs-ues N]
 */

#include "data_lake.hpp"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <thread>
#include <atomic>

namespace {
std::atomic<bool> g_running{true};

void onSignal(int)
{
    g_running.store(false);
}

struct Config {
    uint16_t repPort = 5555;
    uint16_t pubPort = 5556;
    uint16_t subPort = 5557;
    uint16_t numCells = 1;
    uint16_t uesPerCell = 2;
    uint32_t slotUs = 500;      // mu=1 numerology: 0.5 ms/slot
    uint32_t srsPeriodSlots = 10;
    uint16_t srsUesPerCell = 2;
};

bool parseArgs(int argc, char** argv, Config& cfg)
{
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&](uint32_t& out) {
            if (i + 1 >= argc) return false;
            out = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
            return true;
        };
        uint32_t v = 0;
        if (arg == "--rep-port" && next(v)) cfg.repPort = static_cast<uint16_t>(v);
        else if (arg == "--pub-port" && next(v)) cfg.pubPort = static_cast<uint16_t>(v);
        else if (arg == "--sub-port" && next(v)) cfg.subPort = static_cast<uint16_t>(v);
        else if (arg == "--cells" && next(v)) cfg.numCells = static_cast<uint16_t>(v);
        else if (arg == "--ues" && next(v)) cfg.uesPerCell = static_cast<uint16_t>(v);
        else if (arg == "--slot-us" && next(v)) cfg.slotUs = v;
        else if (arg == "--srs-period" && next(v)) cfg.srsPeriodSlots = v;
        else if (arg == "--srs-ues" && next(v)) cfg.srsUesPerCell = static_cast<uint16_t>(v);
        else if (arg == "-h" || arg == "--help") return false;
        else {
            fprintf(stderr, "Unknown argument: %s\n", arg.c_str());
            return false;
        }
    }
    return true;
}
} // namespace

int main(int argc, char** argv)
{
    Config cfg;
    if (!parseArgs(argc, argv, cfg)) {
        fprintf(stderr,
            "Usage: %s [--rep-port P] [--pub-port P] [--sub-port P] [--cells N] "
            "[--ues N] [--slot-us US] [--srs-period N] [--srs-ues N]\n", argv[0]);
        return 1;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    DataLake dataLake(/*e3AgentEnabled=*/true, cfg.repPort, cfg.pubPort, cfg.subPort);
    if (!dataLake.start()) {
        fprintf(stderr, "Failed to start E3 Agent - check ports/SHM permissions.\n");
        return 1;
    }

    fprintf(stderr,
        "E3 Agent standalone running: REP=%u PUB=%u SUB=%u cells=%u ues/cell=%u slot=%uus srs_period=%u slots\n",
        cfg.repPort, cfg.pubPort, cfg.subPort, cfg.numCells, cfg.uesPerCell, cfg.slotUs, cfg.srsPeriodSlots);
    fprintf(stderr, "Press Ctrl-C to stop.\n");

    std::vector<SlotCellTopology> cells(cfg.numCells);
    for (uint16_t c = 0; c < cfg.numCells; ++c) {
        cells[c].cell_id = c + 1;
        cells[c].n_rx_ant = 4;
        cells[c].n_rx_ant_srs = 4;
        cells[c].n_bs_ants = 4;
        cells[c].n_ue = cfg.uesPerCell;
    }

    uint16_t sfn = 0;
    uint16_t slot = 0;
    uint64_t slotCounter = 0;

    while (g_running.load()) {
        const auto tickStart = std::chrono::steady_clock::now();

        dataLake.pushPuschSlot(sfn, slot, cells);
        if (cfg.srsPeriodSlots > 0 && (slotCounter % cfg.srsPeriodSlots) == 0) {
            dataLake.pushSrsSlot(sfn, slot, cells, cfg.srsUesPerCell);
        }

        ++slotCounter;
        if (++slot >= 20) {          // mu=1: 20 slots/frame
            slot = 0;
            ++sfn;
        }

        const auto elapsed = std::chrono::steady_clock::now() - tickStart;
        const auto target = std::chrono::microseconds(cfg.slotUs);
        if (elapsed < target) {
            std::this_thread::sleep_for(target - elapsed);
        }
    }

    fprintf(stderr, "Shutting down E3 Agent...\n");
    dataLake.stop();
    return 0;
}
