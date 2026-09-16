/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Sample E3 Manager (dApp-side reference client).
 *
 * Speaks the same E3AP session e3_agent.cpp implements:
 *   1. REQ/REP  setupRequest -> setupResponse                  (agent's rep_port)
 *   2. PUB/SUB  subscriptionRequest/-Delete/dAppControlAction   (agent's sub_port, manager PUBs)
 *   3. SUB/PUB  subscriptionResponse / indicationMessage / releaseMessage (agent's pub_port, manager SUBs)
 *   4. SHM      POSIX shared memory "/e3_ran_buffers" for the bulk sample data
 *      referenced by indications (iq_samples, pdu_data, h_estimates, srs_*).
 *
 * It reuses e3::streamNameToType / e3::E3AP_PROTOCOL_VERSION / SharedMemoryHeader
 * straight from e3_agent.hpp so the wire contract can't drift between agent and
 * manager - only DataLake's synthetic feed is standalone-specific, this file
 * is not.
 */

#include "e3_agent.hpp"

#include <zmq.hpp>
#include <nlohmann/json.hpp>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using json = nlohmann::json;

namespace {
std::atomic<bool> g_running{true};
void onSignal(int) { g_running.store(false); }

std::string joinNames(const std::vector<std::string>& names)
{
    std::ostringstream oss;
    for (size_t i = 0; i < names.size(); ++i) {
        if (i) oss << ",";
        oss << names[i];
    }
    return oss.str();
}

struct Config {
    std::string host = "localhost";
    uint16_t repPort = 5555;
    uint16_t pubPort = 5556;
    uint16_t subPort = 5557;
    std::vector<std::string> streams = {
        "sfn", "slot", "cell_id", "n_ue", "rnti", "rsrp", "sinr",
        "mcs_index", "tb_crc_fail", "iq_samples", "pdu_data"
    };
    uint32_t periodicityUs = 100000;
    uint32_t subscriptionTimeS = 0; // 0 = indefinite (explicit unsubscribe on exit)
    uint32_t durationS = 0;         // 0 = run until Ctrl-C
    std::string dumpFile;           // empty = capture disabled
};

bool parseArgs(int argc, char** argv, Config& cfg)
{
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto nextStr = [&](std::string& out) { if (i + 1 >= argc) return false; out = argv[++i]; return true; };
        auto nextU32 = [&](uint32_t& out) { if (i + 1 >= argc) return false; out = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10)); return true; };
        std::string s; uint32_t v = 0;
        if (arg == "--host" && nextStr(s)) cfg.host = s;
        else if (arg == "--rep-port" && nextU32(v)) cfg.repPort = static_cast<uint16_t>(v);
        else if (arg == "--pub-port" && nextU32(v)) cfg.pubPort = static_cast<uint16_t>(v);
        else if (arg == "--sub-port" && nextU32(v)) cfg.subPort = static_cast<uint16_t>(v);
        else if (arg == "--streams" && nextStr(s)) {
            cfg.streams.clear();
            std::stringstream ss(s);
            std::string tok;
            while (std::getline(ss, tok, ',')) if (!tok.empty()) cfg.streams.push_back(tok);
        }
        else if (arg == "--periodicity-us" && nextU32(v)) cfg.periodicityUs = v;
        else if (arg == "--subscription-time-s" && nextU32(v)) cfg.subscriptionTimeS = v;
        else if (arg == "--duration-s" && nextU32(v)) cfg.durationS = v;
        else if (arg == "--dump-file" && nextStr(s)) cfg.dumpFile = s;
        else if (arg == "-h" || arg == "--help") return false;
        else { fprintf(stderr, "Unknown argument: %s\n", arg.c_str()); return false; }
    }
    return true;
}

// ---- Wire capture ---------------------------------------------------------
//
// Every message this process sends or receives passes through here when
// --dump-file is set, so you get a complete, replayable JSONL log of the
// application-level E3AP exchange - the ZMTP framing and the manager<->agent
// traffic this process isn't a party to (other dApps, if any) aren't
// captured; use tcpdump/Wireshark with the ZeroMQ dissector for that.

struct MessageDumper {
    std::ofstream file;
    uint64_t seq = 0;

    bool open(const std::string& path)
    {
        file.open(path, std::ios::out | std::ios::trunc);
        if (!file) {
            fprintf(stderr, "Warning: could not open dump file '%s'; capture disabled\n", path.c_str());
            return false;
        }
        return true;
    }

    void log(const char* dir, const json& msg)
    {
        if (!file.is_open()) return;
        json entry;
        entry["seq"] = seq++;
        entry["ts_ns"] = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        entry["dir"] = dir; // "tx" = sent by this manager, "rx" = received from the agent
        entry["msg"] = msg;
        file << entry.dump() << "\n";
        file.flush();
    }
};

// ---- Shared-memory view -------------------------------------------------

struct ShmRegions {
    void* base = nullptr;
    size_t size = 0;
    const SharedMemoryHeader* header = nullptr;
    uint8_t* fh[2] = {};
    uint8_t* pusch[2] = {};
    uint8_t* hest[2] = {};
    uint8_t* srsIq[2] = {};
    uint8_t* srsRbSnr[2] = {};
    uint8_t* srsHest[2] = {};

    bool open(const char* name)
    {
        int fd = shm_open(name, O_RDONLY, 0666);
        if (fd == -1) {
            fprintf(stderr, "shm_open(%s) failed: %s\n", name, strerror(errno));
            return false;
        }
        struct stat st{};
        if (fstat(fd, &st) == -1) {
            fprintf(stderr, "fstat failed: %s\n", strerror(errno));
            close(fd);
            return false;
        }
        size = static_cast<size_t>(st.st_size);
        base = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
        if (base == MAP_FAILED) {
            fprintf(stderr, "mmap failed: %s\n", strerror(errno));
            base = nullptr;
            return false;
        }

        header = static_cast<const SharedMemoryHeader*>(base);
        uint8_t* p = static_cast<uint8_t*>(base) + sizeof(SharedMemoryHeader);
        fh[0] = p;                              p += header->fh_buffer_size;
        fh[1] = p;                              p += header->fh_buffer_size;
        pusch[0] = p;                           p += header->pusch_buffer_size;
        pusch[1] = p;                           p += header->pusch_buffer_size;
        hest[0] = p;                            p += header->hest_buffer_size;
        hest[1] = p;                            p += header->hest_buffer_size;
        srsIq[0] = p;                           p += header->srs_iq_buffer_size;
        srsIq[1] = p;                           p += header->srs_iq_buffer_size;
        srsRbSnr[0] = p;                        p += header->srs_rb_snr_buffer_size;
        srsRbSnr[1] = p;                        p += header->srs_rb_snr_buffer_size;
        srsHest[0] = p;                         p += header->srs_hest_buffer_size;
        srsHest[1] = p;                         p += header->srs_hest_buffer_size;
        return true;
    }

    ~ShmRegions()
    {
        if (base) munmap(base, size);
    }
};

// Print a short preview of raw int16 SHM samples (first few values only).
void previewI16(const uint8_t* base, uint64_t byteOffset, uint32_t count, const char* label)
{
    const int16_t* s = reinterpret_cast<const int16_t*>(base + byteOffset);
    printf("      %s @+%llu: [", label, static_cast<unsigned long long>(byteOffset));
    for (uint32_t i = 0; i < std::min<uint32_t>(count, 6); ++i) printf("%d%s", s[i], (i + 1 < std::min<uint32_t>(count, 6)) ? "," : "");
    printf("%s]\n", count > 6 ? ",..." : "");
}

void previewBytes(const uint8_t* base, uint64_t byteOffset, uint32_t count, const char* label)
{
    printf("      %s @+%llu (%u bytes): [", label, static_cast<unsigned long long>(byteOffset), count);
    for (uint32_t i = 0; i < std::min<uint32_t>(count, 8); ++i) printf("%02x", base[byteOffset + i]);
    printf("%s]\n", count > 8 ? "..." : "");
}

// ---- Indication decoding -------------------------------------------------

void decodeIndication(const json& msg, const ShmRegions& shm)
{
    const json& pd = msg.value("protocolData", json::object());
    printf("indication sub=%u sfn=%d slot=%d\n",
           msg.value("subscriptionId", 0u), pd.value("sfn", -1), pd.value("slot", -1));

    if (!pd.contains("cells")) return;
    for (const auto& cell : pd["cells"]) {
        printf("  cell_id=%d n_ue=%d\n", cell.value("cell_id", -1), cell.value("n_ue", -1));

        if (shm.header && cell.contains("iq_samples")) {
            const auto& s = cell["iq_samples"];
            int idx = s.value("fh_buffer_index", 0);
            uint32_t row = s.value("fh_write_index", 0u);
            uint64_t off = static_cast<uint64_t>(row) * shm.header->num_fh_samples * sizeof(int16_t);
            previewI16(shm.fh[idx & 1], off, shm.header->num_fh_samples, "iq_samples");
        }
        if (shm.header && cell.contains("pdu_data")) {
            const auto& s = cell["pdu_data"];
            int idx = s.value("pusch_buffer_index", 0);
            uint32_t row = s.value("pusch_write_index", 0u);
            uint32_t rowStride = shm.header->num_pusch_rows ? shm.header->pusch_buffer_size / shm.header->num_pusch_rows : 0;
            uint64_t off = static_cast<uint64_t>(row) * rowStride;
            previewBytes(shm.pusch[idx & 1], off, std::min<uint32_t>(rowStride, 64), "pdu_data (row)");
        }
        if (shm.header && cell.contains("h_estimates")) {
            const auto& s = cell["h_estimates"];
            int idx = s.value("hest_buffer_index", 0);
            uint64_t off = s.value("hest_row_byte_offset", 0u);
            previewBytes(shm.hest[idx & 1], off, 32, "h_estimates");
        }
        if (shm.header && cell.contains("srs_iq_samples")) {
            const auto& s = cell["srs_iq_samples"];
            int idx = s.value("srs_iq_buffer_index", 0);
            uint64_t off = s.value("srs_iq_row_byte_offset", 0u);
            previewI16(shm.srsIq[idx & 1], off, 8, "srs_iq_samples");
        }

        if (!cell.contains("ues")) continue;
        for (const auto& ue : cell["ues"]) {
            printf("    rnti=%d", ue.value("rnti", -1));
            if (ue.contains("rsrp")) printf(" rsrp=%.1f", ue.value("rsrp", 0.0f));
            if (ue.contains("sinr")) printf(" sinr=%.1f", ue.value("sinr", 0.0f));
            if (ue.contains("mcs_index")) printf(" mcs=%d", ue.value("mcs_index", 0));
            if (ue.contains("tb_crc_fail")) printf(" crc_fail=%d", ue.value("tb_crc_fail", 0));
            if (ue.contains("srs_wideband_snr")) printf(" srs_snr=%.1f", ue.value("srs_wideband_snr", 0.0f));
            printf("\n");

            if (shm.header && ue.contains("srs_hest_offset") && ue.contains("srs_hest_size") && cell.contains("srs_hest")) {
                int idx = cell["srs_hest"].value("srs_hest_buffer_index", 0);
                previewBytes(shm.srsHest[idx & 1], ue.value("srs_hest_offset", 0u), std::min<uint32_t>(ue.value("srs_hest_size", 0u), 16u), "srs_hest");
            }
            if (shm.header && ue.contains("srs_rb_snr_offset") && ue.contains("srs_rb_snr_size") && cell.contains("srs_rb_snr")) {
                int idx = cell["srs_rb_snr"].value("srs_rb_snr_buffer_index", 0);
                previewBytes(shm.srsRbSnr[idx & 1], ue.value("srs_rb_snr_offset", 0u), std::min<uint32_t>(ue.value("srs_rb_snr_size", 0u), 16u), "srs_rb_snr");
            }
        }
    }
}

} // namespace

int main(int argc, char** argv)
{
    Config cfg;
    if (!parseArgs(argc, argv, cfg)) {
        fprintf(stderr,
            "Usage: %s [--host H] [--rep-port P] [--pub-port P] [--sub-port P] "
            "[--streams a,b,c] [--periodicity-us US] [--subscription-time-s S] [--duration-s S] "
            "[--dump-file path.jsonl]\n",
            argv[0]);
        return 1;
    }
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    MessageDumper dumper;
    if (!cfg.dumpFile.empty() && dumper.open(cfg.dumpFile)) {
        printf("Capturing wire traffic to %s\n", cfg.dumpFile.c_str());
    }

    zmq::context_t ctx(1);
    zmq::socket_t req(ctx, ZMQ_REQ);
    zmq::socket_t sub(ctx, ZMQ_SUB);
    zmq::socket_t pub(ctx, ZMQ_PUB);

    req.set(zmq::sockopt::rcvtimeo, 3000);
    req.set(zmq::sockopt::linger, 0);
    req.connect("tcp://" + cfg.host + ":" + std::to_string(cfg.repPort));

    sub.set(zmq::sockopt::subscribe, "");
    sub.set(zmq::sockopt::rcvtimeo, 200);
    sub.connect("tcp://" + cfg.host + ":" + std::to_string(cfg.pubPort));

    pub.connect("tcp://" + cfg.host + ":" + std::to_string(cfg.subPort));
    // Give the PUB socket's TCP connection + the agent's SUB "slow joiner"
    // window a moment before we publish, or the first message can be dropped.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // --- 1. Setup (REQ/REP) ---
    json setupReq;
    setupReq["type"] = "setupRequest";
    setupReq["id"] = 1;
    setupReq["e3apProtocolVersion"] = e3::E3AP_PROTOCOL_VERSION;
    setupReq["dAppName"] = "sample-e3-manager";
    setupReq["dAppVersion"] = "1.0.0";
    setupReq["vendor"] = "reference-client";

    const std::string setupReqStr = setupReq.dump();
    req.send(zmq::buffer(setupReqStr), zmq::send_flags::none);
    dumper.log("tx", setupReq);

    zmq::message_t setupReplyMsg;
    auto recvResult = req.recv(setupReplyMsg);
    if (!recvResult) {
        fprintf(stderr, "No setupResponse from agent (timed out). Is it running on %s:%u?\n", cfg.host.c_str(), cfg.repPort);
        return 1;
    }
    json setupResp = json::parse(setupReplyMsg.to_string());
    dumper.log("rx", setupResp);
    if (setupResp.value("responseCode", "negative") != "positive") {
        fprintf(stderr, "Setup rejected: %s\n", setupResp.value("message", "unknown").c_str());
        return 1;
    }
    const uint32_t dappId = setupResp.value("dAppIdentifier", 0u);
    const uint32_t ranFunctionId = setupResp["ranFunctionList"][0].value("ranFunctionIdentifier", e3::RAN_FUNCTION_ID_NVIDIA_KPM);
    printf("Setup OK: dAppIdentifier=%u ranIdentifier=%s ranFunctionId=%u\n",
           dappId, setupResp.value("ranIdentifier", std::string()).c_str(), ranFunctionId);

    // --- 2. Open the SHM data channel the agent created ---
    ShmRegions shm;
    if (!shm.open("/e3_ran_buffers")) {
        fprintf(stderr, "Warning: could not open SHM data channel; SHM-backed streams won't be previewed.\n");
    }

    // --- 3. Subscribe (manager PUB -> agent SUB), response arrives on manager SUB ---
    std::vector<uint32_t> telemetryIds;
    for (const auto& name : cfg.streams) {
        e3::StreamType st = e3::streamNameToType(name);
        if (st == e3::StreamType::NONE) {
            fprintf(stderr, "Unknown stream name '%s', skipping\n", name.c_str());
            continue;
        }
        __uint128_t val = static_cast<__uint128_t>(st);
        uint32_t pos = 0;
        while (val >>= 1) ++pos;
        telemetryIds.push_back(pos + 1);
    }
    if (telemetryIds.empty()) {
        fprintf(stderr, "No valid streams requested (--streams %s)\n", joinNames(cfg.streams).c_str());
        return 1;
    }

    json subReq;
    subReq["type"] = "subscriptionRequest";
    subReq["id"] = 2;
    subReq["dAppIdentifier"] = dappId;
    subReq["ranFunctionIdentifier"] = ranFunctionId;
    subReq["telemetryIdentifierList"] = telemetryIds;
    subReq["controlIdentifierList"] = json::array();
    subReq["periodicity"] = cfg.periodicityUs;
    subReq["subscriptionTime"] = cfg.subscriptionTimeS;

    const std::string subReqStr = subReq.dump();
    pub.send(zmq::buffer(subReqStr), zmq::send_flags::none);
    dumper.log("tx", subReq);
    printf("Sent subscriptionRequest for streams [%s] (telemetry IDs: ", joinNames(cfg.streams).c_str());
    for (size_t i = 0; i < telemetryIds.size(); ++i) printf("%u%s", telemetryIds[i], (i + 1 < telemetryIds.size()) ? "," : "");
    printf(")\n");

    uint32_t subscriptionId = 0;
    bool subscribed = false;
    const auto subDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

    // --- 4. Main receive loop: subscriptionResponse first, then indications ---
    const auto runDeadline = (cfg.durationS > 0)
        ? std::chrono::steady_clock::now() + std::chrono::seconds(cfg.durationS)
        : std::chrono::steady_clock::time_point::max();

    while (g_running.load() && std::chrono::steady_clock::now() < runDeadline) {
        zmq::message_t msg;
        auto r = sub.recv(msg, zmq::recv_flags::none);
        if (!r) {
            if (!subscribed && std::chrono::steady_clock::now() > subDeadline) {
                fprintf(stderr, "Timed out waiting for subscriptionResponse; retrying subscribe...\n");
                pub.send(zmq::buffer(subReqStr), zmq::send_flags::none);
                dumper.log("tx", subReq);
            }
            continue;
        }

        json m;
        try {
            m = json::parse(msg.to_string());
        } catch (const json::parse_error&) {
            continue;
        }
        dumper.log("rx", m);

        const std::string type = m.value("type", "");
        if (type == "subscriptionResponse" && m.value("dAppIdentifier", 0u) == dappId) {
            if (m.value("responseCode", "negative") == "positive") {
                subscriptionId = m.value("subscriptionId", 0u);
                subscribed = true;
                printf("Subscribed: subscriptionId=%u periodicity=%uus\n", subscriptionId, m.value("periodicity", 0u));
            } else {
                fprintf(stderr, "Subscription rejected: %s\n", m.value("message", "unknown").c_str());
            }
        } else if (type == "indicationMessage" && m.value("dAppIdentifier", 0u) == dappId) {
            decodeIndication(m, shm);
        } else if (type == "releaseMessage" && m.value("dAppIdentifier", 0u) == dappId) {
            printf("Agent released this dApp (idle timeout). Exiting.\n");
            break;
        }
    }

    // --- 5. Clean shutdown: delete subscription, release ---
    if (subscribed && subscriptionId != 0) {
        json delReq;
        delReq["type"] = "subscriptionDelete";
        delReq["id"] = 3;
        delReq["dAppIdentifier"] = dappId;
        delReq["subscriptionId"] = subscriptionId;
        const std::string delStr = delReq.dump();
        pub.send(zmq::buffer(delStr), zmq::send_flags::none);
        dumper.log("tx", delReq);
    }
    json releaseMsg;
    releaseMsg["type"] = "releaseMessage";
    releaseMsg["id"] = 4;
    releaseMsg["dAppIdentifier"] = dappId;
    const std::string releaseStr = releaseMsg.dump();
    pub.send(zmq::buffer(releaseStr), zmq::send_flags::none);
    dumper.log("tx", releaseMsg);

    if (dumper.file.is_open()) {
        printf("Captured %llu messages to %s\n", static_cast<unsigned long long>(dumper.seq), cfg.dumpFile.c_str());
    }
    printf("Manager exiting.\n");
    return 0;
}
