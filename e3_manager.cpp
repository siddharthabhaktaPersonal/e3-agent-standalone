/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Sample E3 Manager (dApp-side reference client) - Layer-2 (MAC) KPI edition.
 *
 * Speaks the same E3AP session e3_agent.cpp implements:
 *   1. REQ/REP  setupRequest -> setupResponse                  (agent's rep_port)
 *   2. PUB/SUB  subscriptionRequest/-Delete/dAppControlAction   (agent's sub_port, manager PUBs)
 *   3. SUB/PUB  subscriptionResponse / indicationMessage / releaseMessage (agent's pub_port, manager SUBs)
 *
 * There is no shared-memory channel in this version - every L2 KPI stream
 * travels inline in the indication's JSON protocolData, so this file has no
 * SHM handling at all (unlike the earlier L1 edition of this project).
 *
 * It reuses e3::streamNameToType / e3::E3AP_PROTOCOL_VERSION straight from
 * e3_agent.hpp so the wire contract can't drift between agent and manager.
 */

#include "e3_agent.hpp"

#include <zmq.hpp>
#include <nlohmann/json.hpp>

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
        "sfn", "slot", "cell_id", "n_ue", "rnti",
        "dl_prb", "ul_prb", "dl_mcs", "ul_mcs", "wb_cqi",
        "dl_bler", "ul_bler", "pusch_snr", "pucch_snr", "total_bsr", "phr"
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

// ---- Indication decoding -------------------------------------------------

void decodeIndication(const json& msg)
{
    const json& pd = msg.value("protocolData", json::object());
    printf("indication sub=%u sfn=%d slot=%d\n",
           msg.value("subscriptionId", 0u), pd.value("sfn", -1), pd.value("slot", -1));

    if (!pd.contains("cells")) return;
    for (const auto& cell : pd["cells"]) {
        printf("  cell_id=%d n_ue=%d\n", cell.value("cell_id", -1), cell.value("n_ue", -1));

        if (!cell.contains("ues")) continue;
        for (const auto& ue : cell["ues"]) {
            printf("    rnti=%d", ue.value("rnti", -1));
            if (ue.contains("dl_prb") || ue.contains("ul_prb"))
                printf(" prb(dl/ul)=%d/%d", ue.value("dl_prb", 0), ue.value("ul_prb", 0));
            if (ue.contains("dl_prb_retx") || ue.contains("ul_prb_retx"))
                printf(" prb_retx(dl/ul)=%d/%d", ue.value("dl_prb_retx", 0), ue.value("ul_prb_retx", 0));
            if (ue.contains("dl_curr_tbs") || ue.contains("ul_curr_tbs"))
                printf(" curr_tbs(dl/ul)=%llu/%llu",
                       static_cast<unsigned long long>(ue.value("dl_curr_tbs", 0ull)),
                       static_cast<unsigned long long>(ue.value("ul_curr_tbs", 0ull)));
            if (ue.contains("dl_aggr_tbs") || ue.contains("ul_aggr_tbs"))
                printf(" aggr_tbs(dl/ul)=%llu/%llu",
                       static_cast<unsigned long long>(ue.value("dl_aggr_tbs", 0ull)),
                       static_cast<unsigned long long>(ue.value("ul_aggr_tbs", 0ull)));
            if (ue.contains("dl_mcs") || ue.contains("ul_mcs"))
                printf(" mcs(dl/ul)=%d/%d", ue.value("dl_mcs", 0), ue.value("ul_mcs", 0));
            if (ue.contains("wb_cqi")) printf(" cqi=%d", ue.value("wb_cqi", 0));
            if (ue.contains("dl_errors") || ue.contains("ul_errors"))
                printf(" errors(dl/ul)=%d/%d", ue.value("dl_errors", 0), ue.value("ul_errors", 0));
            if (ue.contains("dl_bler") || ue.contains("ul_bler"))
                printf(" bler(dl/ul)=%.3f/%.3f", ue.value("dl_bler", 0.0), ue.value("ul_bler", 0.0));
            if (ue.contains("pusch_snr") || ue.contains("pucch_snr"))
                printf(" snr(pusch/pucch)=%d/%d", ue.value("pusch_snr", 0), ue.value("pucch_snr", 0));
            if (ue.contains("total_bsr")) printf(" bsr=%llu", static_cast<unsigned long long>(ue.value("total_bsr", 0ull)));
            if (ue.contains("phr")) printf(" phr=%d", ue.value("phr", 0));
            printf("\n");

            if (ue.contains("dl_harq_rounds")) {
                printf("      dl_harq_rounds=[");
                const auto& rounds = ue["dl_harq_rounds"];
                for (size_t i = 0; i < rounds.size(); ++i) printf("%s%d", i ? "," : "", rounds[i].get<int>());
                printf("]\n");
            }
            if (ue.contains("per_lcid_dl_bytes") || ue.contains("per_lcid_ul_bytes")) {
                auto printNonzero = [](const char* label, const json& arr) {
                    printf("      %s=[", label);
                    bool first = true;
                    for (size_t i = 0; i < arr.size(); ++i) {
                        uint64_t v = arr[i].get<uint64_t>();
                        if (v == 0) continue;
                        printf("%slcid%zu:%llu", first ? "" : ",", i, static_cast<unsigned long long>(v));
                        first = false;
                    }
                    printf("]\n");
                };
                if (ue.contains("per_lcid_dl_bytes")) printNonzero("per_lcid_dl_bytes", ue["per_lcid_dl_bytes"]);
                if (ue.contains("per_lcid_ul_bytes")) printNonzero("per_lcid_ul_bytes", ue["per_lcid_ul_bytes"]);
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

    // --- 2. Subscribe (manager PUB -> agent SUB), response arrives on manager SUB ---
    std::vector<uint32_t> telemetryIds;
    for (const auto& name : cfg.streams) {
        e3::StreamType st = e3::streamNameToType(name);
        if (st == e3::StreamType::NONE) {
            fprintf(stderr, "Unknown stream name '%s', skipping\n", name.c_str());
            continue;
        }
        uint64_t val = static_cast<uint64_t>(st);
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

    // --- 3. Main receive loop: subscriptionResponse first, then indications ---
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
            decodeIndication(m);
        } else if (type == "releaseMessage" && m.value("dAppIdentifier", 0u) == dappId) {
            printf("Agent released this dApp (idle timeout). Exiting.\n");
            break;
        }
    }

    // --- 4. Clean shutdown: delete subscription, release ---
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
