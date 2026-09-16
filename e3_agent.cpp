/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * E3 interface implementation - Layer-2 (MAC) KPI edition. See e3_agent.hpp
 * for what changed vs. the original L1/SHM version this was forked from.
 */

#include "e3_agent.hpp"
#include "data_lake.hpp"
#include "fmt/format.h"
#include <cstring>

// Constructor
E3Agent::E3Agent(
    DataLake* dl,
    const uint16_t repPort,
    const uint16_t pubPort,
    const uint16_t subPort
) :
    dataLake(dl),
    e3RepPort(repPort),
    e3PubPort(pubPort),
    e3SubPort(subPort),
    zmq_context(1),
    e3_rep_socket(zmq_context, ZMQ_REP),
    e3_pub_socket(zmq_context, ZMQ_PUB),
    e3_sub_socket(zmq_context, ZMQ_SUB)
{
}

// Destructor
E3Agent::~E3Agent()
{
    shutdown();
}

// Initialize E3 agent - bind sockets and start threads
bool E3Agent::init()
{
    if (e3_running.load()) {
        return true;
    }

    NVLOGC_FMT(TAG_E3, "Initializing E3 Agent...");

    try {
        e3_rep_socket.bind("tcp://*:" + std::to_string(e3RepPort));
        e3_pub_socket.bind("tcp://*:" + std::to_string(e3PubPort));
        e3_sub_socket.bind("tcp://*:" + std::to_string(e3SubPort));

        e3_rep_socket.set(zmq::sockopt::tcp_keepalive, 1);
        e3_rep_socket.set(zmq::sockopt::tcp_keepalive_idle, 5);
        e3_rep_socket.set(zmq::sockopt::tcp_keepalive_intvl, 2);
        e3_rep_socket.set(zmq::sockopt::tcp_keepalive_cnt, 3);

        e3_sub_socket.set(zmq::sockopt::subscribe, "");
        e3_sub_socket.set(zmq::sockopt::linger, 1000);  // 1 second linger to allow graceful shutdown
        e3_sub_socket.set(zmq::sockopt::rcvtimeo, 20);   // block per-message; bounded idle wakeups for shutdown

        NVLOGC_FMT(TAG_E3, "E3 sockets initialized - REP: {}, PUB: {}, SUB: {}", e3RepPort, e3PubPort, e3SubPort);
    } catch (const zmq::error_t& e) {
        NVLOGC_FMT(TAG_E3, "Failed to initialize E3 sockets: {}", e.what());
        return false;
    }

    e3_running = true;
    e3_data_thread = std::thread(&E3Agent::dataServerThread, this);

    e3_reaper_running = true;
    e3_reaper_thread = std::thread(&E3Agent::reaperThread, this);

    e3_sub_running = true;
    e3_sub_thread = std::thread(&E3Agent::managerSubscriptionThread, this);

    e3_notifier_running = true;
    e3_notifier_thread = std::thread(&E3Agent::notifierThread, this);

    {
        std::lock_guard<std::mutex> lock(dataLake->e3_buffer_mutex);
        dataLake->e3_buffer_info = {};
    }

    NVLOGC_FMT(TAG_E3, "E3 Agent initialized successfully.");
    return true;
}

// Shutdown E3 agent - stop threads and close sockets
void E3Agent::shutdown()
{
    if (e3_running) {
        e3_running = false;
        if (e3_data_thread.joinable()) {
            e3_data_thread.join();
        }
        NVLOGC_FMT(TAG_E3, "E3 data server thread shutdown");
    }

    if (e3_sub_running) {
        e3_sub_running = false;
        if (e3_sub_thread.joinable()) {
            e3_sub_thread.join();
        }
        NVLOGC_FMT(TAG_E3, "E3 subscription thread shutdown");
    }

    if (e3_notifier_running) {
        e3_notifier_running = false;
        if (e3_notifier_thread.joinable()) {
            e3_notifier_thread.join();
        }
        NVLOGC_FMT(TAG_E3, "E3 notifier thread shutdown");
    }

    if (e3_reaper_running) {
        e3_reaper_running = false;
        if (e3_reaper_thread.joinable()) {
            e3_reaper_thread.join();
        }
        NVLOGC_FMT(TAG_E3, "E3 reaper thread shutdown");
    }

    try {
        e3_pub_socket.close();
        e3_rep_socket.close();
        e3_sub_socket.close();
        NVLOGC_FMT(TAG_E3, "E3 sockets closed successfully");
    } catch (const zmq::error_t& e) {
        NVLOGC_FMT(TAG_E3, "Error closing E3 sockets: {}", e.what());
    }
}

// Notifier thread - independent of DataLake's data-refresh cadence, this
// just wakes on NOTIFIER_TICK_INTERVAL and asks sendDueIndications() which
// (if any) subscriptions are due, per their own configured periodicity_us.
void E3Agent::notifierThread()
{
    NVLOGC_FMT(TAG_E3, "E3 notifier thread started");

    while (e3_notifier_running) {
        sendDueIndications();
        std::this_thread::sleep_for(NOTIFIER_TICK_INTERVAL);
    }

    NVLOGC_FMT(TAG_E3, "E3 notifier thread stopped");
}

// Send indications to subscriptions whose periodicity has elapsed, using
// whatever L2 KPI snapshot is currently in DataLake (read fresh each call -
// DataLake refreshes it independently, on its own cadence).
// Flow: gate → periodicity → bit-walks (root → cell → per-UE) → empty check → send → update timestamp.
void E3Agent::sendDueIndications()
{
    if (!e3_running) {
        return;
    }

    // Read the snapshot in place rather than copying it: E3L2BufferInfo's
    // fixed E3_MAX_CELLS * E3_MAX_UES_PER_CELL arrays make a by-value copy
    // tens of KB, and this runs on every notifier tick regardless of
    // whether anything is actually due. Holding the buffer lock across JSON
    // building (below) is cheap in comparison - no I/O happens in between.
    std::lock_guard<std::mutex> buffer_lock(dataLake->e3_buffer_mutex);
    const E3L2BufferInfo& buffer_info = dataLake->e3_buffer_info;

    std::lock_guard<std::mutex> lock(e3_subscriptions_mutex);
    for (auto& [sub_id, sub] : e3_subscriptions) {
        if ((sub.stream_bitfield & e3::ALL_PROVIDABLE_STREAMS) == e3::StreamType::NONE) {
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::microseconds>(now - sub.last_update).count() < sub.periodicity_us) {
            continue;
        }

        json notif_json;
        notif_json["type"] = "indicationMessage";
        notif_json["id"] = generateMessageId();
        notif_json["dAppIdentifier"] = sub.dapp_id;
        notif_json["ranFunctionIdentifier"] = sub.ran_function_id;
        notif_json["subscriptionId"] = sub.subscription_id;

        json protocolData;

        // Slot-shared (root) streams.
        const e3::StreamType cell_streams = sub.stream_bitfield & e3::ALL_PROVIDABLE_STREAMS & ~e3::PER_UE_STREAMS;
        {
            uint64_t remaining = static_cast<uint64_t>(cell_streams);
            while (remaining != 0) {
                const uint64_t lowest_bit = remaining & (~remaining + 1);
                switch (static_cast<e3::StreamType>(lowest_bit)) {
                    case e3::StreamType::TIMESTAMP:
                        protocolData["timestamp"] = buffer_info.timestamp_ns;
                        break;
                    case e3::StreamType::TIMESTAMP_TAI:
                        protocolData["timestamp_tai"] = buffer_info.timestamp_tai_ns;
                        break;
                    case e3::StreamType::SFN:
                        protocolData["sfn"] = buffer_info.sfn;
                        break;
                    case e3::StreamType::SLOT:
                        protocolData["slot"] = buffer_info.slot;
                        break;
                    case e3::StreamType::N_CELLS:
                        protocolData["n_cells"] = buffer_info.n_cells;
                        break;
                    default:
                        break;
                }
                remaining &= ~lowest_bit;
            }
        }

        // Per-cell array: cell-level scalars + nested ues[].
        const e3::StreamType ue_streams = sub.stream_bitfield & e3::PER_UE_STREAMS;
        json cells_arr = json::array();
        for (uint16_t ci = 0; ci < buffer_info.n_cells; ++ci) {
            const E3CellL2Info& cell = buffer_info.cells[ci];
            json cell_obj;
            uint64_t remaining = static_cast<uint64_t>(cell_streams);
            while (remaining != 0) {
                const uint64_t lowest_bit = remaining & (~remaining + 1);
                switch (static_cast<e3::StreamType>(lowest_bit)) {
                    case e3::StreamType::CELL_ID:
                        cell_obj["cell_id"] = cell.cell_id;
                        break;
                    case e3::StreamType::N_UE:
                        cell_obj["n_ue"] = cell.n_ue;
                        break;
                    default:
                        break;
                }
                remaining &= ~lowest_bit;
            }

            if (static_cast<uint64_t>(ue_streams) != 0 && cell.n_ue > 0) {
                json ues_arr = json::array();
                for (uint16_t ui = 0; ui < cell.n_ue; ++ui) {
                    const E3UeL2Stats& ue = cell.ues[ui];
                    json ue_obj;
                    uint64_t ue_remaining = static_cast<uint64_t>(ue_streams);
                    while (ue_remaining != 0) {
                        const uint64_t lowest_bit = ue_remaining & (~ue_remaining + 1);
                        switch (static_cast<e3::StreamType>(lowest_bit)) {
                            case e3::StreamType::RNTI:
                                ue_obj["rnti"] = ue.rnti;
                                break;
                            case e3::StreamType::DL_PRB:
                                ue_obj["dl_prb"] = ue.prb_stats.dl_prb;
                                break;
                            case e3::StreamType::UL_PRB:
                                ue_obj["ul_prb"] = ue.prb_stats.ul_prb;
                                break;
                            case e3::StreamType::DL_PRB_RETX:
                                ue_obj["dl_prb_retx"] = ue.prb_stats.dl_prb_retx;
                                break;
                            case e3::StreamType::UL_PRB_RETX:
                                ue_obj["ul_prb_retx"] = ue.prb_stats.ul_prb_retx;
                                break;
                            case e3::StreamType::DL_AGGR_TBS:
                                ue_obj["dl_aggr_tbs"] = ue.tbs_stats.dl_aggr_tbs;
                                break;
                            case e3::StreamType::UL_AGGR_TBS:
                                ue_obj["ul_aggr_tbs"] = ue.tbs_stats.ul_aggr_tbs;
                                break;
                            case e3::StreamType::DL_CURR_TBS:
                                ue_obj["dl_curr_tbs"] = ue.tbs_stats.dl_curr_tbs;
                                break;
                            case e3::StreamType::UL_CURR_TBS:
                                ue_obj["ul_curr_tbs"] = ue.tbs_stats.ul_curr_tbs;
                                break;
                            case e3::StreamType::PER_LCID_DL_BYTES:
                                ue_obj["per_lcid_dl_bytes"] = std::vector<uint64_t>(
                                    std::begin(ue.per_lcid_bytes.dl_lc_bytes), std::end(ue.per_lcid_bytes.dl_lc_bytes));
                                break;
                            case e3::StreamType::PER_LCID_UL_BYTES:
                                ue_obj["per_lcid_ul_bytes"] = std::vector<uint64_t>(
                                    std::begin(ue.per_lcid_bytes.ul_lc_bytes), std::end(ue.per_lcid_bytes.ul_lc_bytes));
                                break;
                            case e3::StreamType::DL_MCS:
                                ue_obj["dl_mcs"] = ue.mcs_stats.dl_mcs;
                                break;
                            case e3::StreamType::UL_MCS:
                                ue_obj["ul_mcs"] = ue.mcs_stats.ul_mcs;
                                break;
                            case e3::StreamType::WB_CQI:
                                ue_obj["wb_cqi"] = ue.wb_cqi.cqi;
                                break;
                            case e3::StreamType::DL_HARQ_ROUNDS:
                                ue_obj["dl_harq_rounds"] = std::vector<uint32_t>(
                                    std::begin(ue.tb_stats.dl_harq_rounds), std::end(ue.tb_stats.dl_harq_rounds));
                                break;
                            case e3::StreamType::DL_ERRORS:
                                ue_obj["dl_errors"] = ue.tb_stats.dl_errors;
                                break;
                            case e3::StreamType::UL_ERRORS:
                                ue_obj["ul_errors"] = ue.tb_stats.ul_errors;
                                break;
                            case e3::StreamType::DL_BLER:
                                ue_obj["dl_bler"] = ue.tb_stats.dl_bler;
                                break;
                            case e3::StreamType::UL_BLER:
                                ue_obj["ul_bler"] = ue.tb_stats.ul_bler;
                                break;
                            case e3::StreamType::PUSCH_SNR:
                                ue_obj["pusch_snr"] = ue.snr_stats.pusch_snr;
                                break;
                            case e3::StreamType::PUCCH_SNR:
                                ue_obj["pucch_snr"] = ue.snr_stats.pucch_snr;
                                break;
                            case e3::StreamType::TOTAL_BSR:
                                ue_obj["total_bsr"] = ue.bsr_stats.total_bsr;
                                break;
                            case e3::StreamType::PHR:
                                ue_obj["phr"] = ue.phr_stats.phr;
                                break;
                            default:
                                break;
                        }
                        ue_remaining &= ~lowest_bit;
                    }
                    if (!ue_obj.empty()) {
                        ues_arr.push_back(std::move(ue_obj));
                    }
                }
                if (!ues_arr.empty()) {
                    cell_obj["ues"] = std::move(ues_arr);
                }
            }

            if (!cell_obj.empty()) {
                cells_arr.push_back(std::move(cell_obj));
            }
        }
        if (!cells_arr.empty()) {
            protocolData["cells"] = std::move(cells_arr);
        }

        if (protocolData.empty()) {
            continue;
        }
        notif_json["protocolData"] = protocolData;

        try {
            const std::string message = notif_json.dump();
            {
                std::lock_guard<std::mutex> lock(e3_pub_socket_mutex_);
                e3_pub_socket.send(zmq::buffer(message), zmq::send_flags::dontwait);
            }
            NVLOGD_FMT(TAG_E3, "Sent L2 E3 indication to dApp {} for subscription {}", sub.dapp_id, sub.subscription_id);
        } catch (const zmq::error_t& e) {
            NVLOGD_FMT(TAG_E3, "L2 E3 indication send failed: {}", e.what());
        }
        sub.last_update = now;
    }
}

// Thread functions

// E3 data server thread - handles ZMQ request/reply
void E3Agent::dataServerThread()
{
    e3_rep_socket.set(zmq::sockopt::rcvtimeo, 1000);

    NVLOGC_FMT(TAG_E3, "E3 data server thread started");

    while (e3_running) {
        zmq::message_t request;
        if (e3_rep_socket.recv(request, zmq::recv_flags::none)) {
            std::string response;
            try {
                const json req_json = json::parse(std::string(static_cast<char*>(request.data()), request.size()));
                const std::string type = req_json.value("type", "");

                NVLOGC_FMT(TAG_E3, "Received E3 request: {}", req_json.dump());

                if (type == "setupRequest") {
                    handleSetupRequest(req_json, response);
                } else {
                    json error_resp;
                    error_resp["type"] = type;
                    error_resp["id"] = generateMessageId();
                    error_resp["requestId"] = req_json.value("id", 0u);
                    error_resp["responseCode"] = "negative";
                    error_resp["message"] = "unknown request type";
                    response = error_resp.dump();
                }
            } catch (const json::parse_error& e) {
                json error_resp;
                error_resp["responseCode"] = "negative";
                error_resp["message"] = "invalid JSON format";
                response = error_resp.dump();
                NVLOGC_FMT(TAG_E3, "Failed to parse request: {}", e.what());
            }
            e3_rep_socket.send(zmq::buffer(response));
        }
    }
    NVLOGC_FMT(TAG_E3, "E3 data server thread stopped");
}

// E3 reaper thread - cleanup disconnected dApps
void E3Agent::reaperThread()
{
    NVLOGC_FMT(TAG_E3, "E3 reaper thread started");

    while (e3_reaper_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        reapTimedOutDapps();
    }
    NVLOGC_FMT(TAG_E3, "E3 reaper thread stopped");
}

// Remove timed-out dApps
// NOTE: dApps with active subscriptions are kept alive even if inactive, since indications
// are fire-and-forget (no ACK). A crashed dApp with subscriptions won't be reaped until
// an explicit release message is sent.
void E3Agent::reapTimedOutDapps()
{
    constexpr auto ACTIVITY_TIMEOUT_SECONDS = 1800;

    const auto now = std::chrono::steady_clock::now();

    // Expire time-bounded subscriptions
    {
        std::lock_guard<std::mutex> lock(e3_subscriptions_mutex);
        for (auto it = e3_subscriptions.begin(); it != e3_subscriptions.end(); ) {
            if (now >= it->second.expiry_time) {
                NVLOGC_FMT(TAG_E3, "Subscription {} expired for dApp {}", it->first, it->second.dapp_id);
                it = e3_subscriptions.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::vector<uint32_t> timed_out_dapps;

    {
        std::lock_guard<std::mutex> lock(e3_dapps_mutex);
        for (auto const& [dapp_id, conn_info] : e3_connected_dapps) {
            if (std::chrono::duration_cast<std::chrono::seconds>(now - conn_info.last_activity_time).count() > ACTIVITY_TIMEOUT_SECONDS) {
                bool has_active_subscriptions = false;
                {
                    std::lock_guard<std::mutex> subs_lock(e3_subscriptions_mutex);
                    for (const auto& [sub_id, sub] : e3_subscriptions) {
                        if (sub.dapp_id == dapp_id) {
                            has_active_subscriptions = true;
                            break;
                        }
                    }
                }

                if (!has_active_subscriptions) {
                    timed_out_dapps.push_back(dapp_id);
                }
            }
        }
    }

    for (const uint32_t dapp_id : timed_out_dapps) {
        NVLOGC_FMT(TAG_E3, "dApp {} timed out after {} seconds of inactivity. Releasing.", dapp_id, ACTIVITY_TIMEOUT_SECONDS);
        sendRelease(dapp_id);
    }
}

// Manager subscription thread - receives commands from E3 Manager
void E3Agent::managerSubscriptionThread()
{
    NVLOGC_FMT(TAG_E3, "E3 Manager subscription thread started");

    while (e3_sub_running) {
        try {
            zmq::message_t msg;
            // Blocking recv (rcvtimeo-bounded) so control messages are drained at
            // arrival rate. A dontwait+sleep loop caps intake and backs up the SUB
            // queue, delaying/dropping controls under load.
            const auto result = e3_sub_socket.recv(msg);

            if (result) {
                try {
                    const json msg_json = json::parse(msg.to_string());
                    handleManagerMessage(msg_json);
                } catch (const json::exception& e) {
                    NVLOGC_FMT(TAG_E3, "Failed to parse dApp message: {}", e.what());
                }
            }
        } catch (const zmq::error_t& e) {
            if (e.num() == ETERM) {
                // Context terminated during shutdown - exit gracefully
                break;
            } else if (e.num() != EAGAIN) {
                NVLOGC_FMT(TAG_E3, "Error receiving from Manager: {}", e.what());
            }
        }
    }

    NVLOGC_FMT(TAG_E3, "E3 Manager subscription thread stopped");
}

// Handle messages asynchronously received from E3 Manager via PUB-SUB
void E3Agent::handleManagerMessage(const json& message)
{
    NVLOGD_FMT(TAG_E3, "Handling Manager message: {}", message.dump());

    const std::string type = message.value("type", "");

    if (type == "releaseMessage") {
        const uint32_t dapp_id = message.value("dAppIdentifier", 0u);
        if (dapp_id == 0) {
            NVLOGC_FMT(TAG_E3, "Received e3_release with invalid dAppIdentifier");
            return;
        }
        NVLOGC_FMT(TAG_E3, "Received e3_release from dApp {}", dapp_id);
        releaseDapp(dapp_id);

    } else if (type == "subscriptionRequest" || type == "subscriptionDelete") {
        // Subscription Request/Delete via PUB-SUB: process and publish response on Agent PUB
        const uint32_t dapp_id = message.value("dAppIdentifier", 0u);

        // Silently ignore requests for dApps we don't own (multi-agent correctness)
        {
            std::lock_guard<std::mutex> lock(e3_dapps_mutex);
            if (e3_connected_dapps.find(dapp_id) == e3_connected_dapps.end()) {
                return;
            }
        }

        std::string response;
        if (type == "subscriptionRequest") {
            handleSubscriptionRequest(message, response);
        } else if (type == "subscriptionDelete") {
            handleSubscriptionDelete(message, response);
        }

        // Publish response on Agent PUB socket
        try {
            {
                std::lock_guard<std::mutex> lock(e3_pub_socket_mutex_);
                e3_pub_socket.send(zmq::buffer(response), zmq::send_flags::dontwait);
            }
            NVLOGC_FMT(TAG_E3, "Published subscription response to dApp {}", dapp_id);
        } catch (const zmq::error_t& e) {
            NVLOGC_FMT(TAG_E3, "Failed to publish subscription response to dApp {}: {}", dapp_id, e.what());
        }

    } else if (type == "dAppControlAction") {
        const uint32_t dapp_id = message.value("dAppIdentifier", 0u);

        {
            std::lock_guard<std::mutex> lock(e3_dapps_mutex);
            if (e3_connected_dapps.find(dapp_id) == e3_connected_dapps.end()) {
                return;
            }
        }

        std::string response;
        handleControlMessage(message, response);

        // Optional ack to control message
        try {
            {
                std::lock_guard<std::mutex> lock(e3_pub_socket_mutex_);
                e3_pub_socket.send(zmq::buffer(response), zmq::send_flags::dontwait);
            }
        } catch (const zmq::error_t& e) {
            NVLOGC_FMT(TAG_E3, "Failed to publish control ack to dApp {}: {}", dapp_id, e.what());
        }
    }
}

bool E3Agent::updateDappActivity(uint32_t dapp_id)
{
    std::lock_guard<std::mutex> lock(e3_dapps_mutex);
    auto it = e3_connected_dapps.find(dapp_id);
    if (it == e3_connected_dapps.end()) return false;
    it->second.last_activity_time = std::chrono::steady_clock::now();
    return true;
}

// Release a dApp: remove all subscriptions and connection state
void E3Agent::releaseDapp(uint32_t dapp_id)
{
    std::lock_guard<std::mutex> dapps_lock(e3_dapps_mutex);
    std::lock_guard<std::mutex> subs_lock(e3_subscriptions_mutex);

    auto it = e3_connected_dapps.find(dapp_id);
    if (it == e3_connected_dapps.end()) {
        NVLOGC_FMT(TAG_E3, "Release: dApp {} not found, ignoring", dapp_id);
        return;
    }

    e3_connected_dapps.erase(it);

    for (auto sub_it = e3_subscriptions.begin(); sub_it != e3_subscriptions.end(); ) {
        if (sub_it->second.dapp_id == dapp_id) {
            NVLOGC_FMT(TAG_E3, "Release: removing subscription {} for dApp {}", sub_it->first, dapp_id);
            sub_it = e3_subscriptions.erase(sub_it);
        } else {
            ++sub_it;
        }
    }

    NVLOGC_FMT(TAG_E3, "dApp {} released successfully", dapp_id);
}

// Send e3_release to a dApp via PUB socket
bool E3Agent::sendRelease(uint32_t dapp_id)
{
    // Verify dApp exists before publishing
    {
        std::lock_guard<std::mutex> lock(e3_dapps_mutex);
        if (e3_connected_dapps.find(dapp_id) == e3_connected_dapps.end()) {
            NVLOGC_FMT(TAG_E3, "sendRelease: dApp {} not found", dapp_id);
            return false;
        }
    }

    json release_msg;
    release_msg["type"] = "releaseMessage";
    release_msg["id"] = generateMessageId();
    release_msg["dAppIdentifier"] = dapp_id;

    const std::string message = release_msg.dump();

    try {
        {
            std::lock_guard<std::mutex> lock(e3_pub_socket_mutex_);
            e3_pub_socket.send(zmq::buffer(message), zmq::send_flags::dontwait);
        }
        NVLOGC_FMT(TAG_E3, "Sent e3_release to dApp {}", dapp_id);
    } catch (const zmq::error_t& e) {
        NVLOGC_FMT(TAG_E3, "Failed to send e3_release to dApp {}: {}", dapp_id, e.what());
        return false;
    }

    releaseDapp(dapp_id);
    return true;
}

// E3AP Message helpers

uint32_t E3Agent::generateMessageId()
{
    static std::atomic<uint32_t> message_counter{1};
    return message_counter.fetch_add(1);
}

uint32_t E3Agent::generateDappId()
{
    static std::atomic<uint32_t> dapp_counter{1};
    return dapp_counter.fetch_add(1);
}

uint32_t E3Agent::generateSubscriptionId()
{
    static std::atomic<uint32_t> sub_counter{1};
    return sub_counter.fetch_add(1);
}

// Stream creation helper

json E3Agent::createIndicationPayloadDelivery(const std::string& stream_id) const
{
    json delivery;
    delivery["transport_type"] = "protocolData";
    delivery["keyword"] = stream_id;
    delivery["encoding"] = "json";
    return delivery;
}

json E3Agent::createIndicationPayloadStream(
    const std::string& stream_id,
    const std::string& data_type,
    const std::string& description
) const
{
    json stream;
    uint64_t val = static_cast<uint64_t>(e3::streamNameToType(stream_id));
    if (val == 0) {
        NVLOGC_FMT(TAG_E3, "createIndicationPayloadStream: unknown stream_id '{}', telemetryIdentifier will be 0", stream_id);
        stream["telemetryIdentifier"] = 0;
    } else {
        uint32_t pos = 0;
        uint64_t tmp = val;
        while (tmp >>= 1) ++pos;
        stream["telemetryIdentifier"] = pos + 1;
    }
    stream["stream_id"] = stream_id;
    stream["data_type"] = data_type;
    stream["description"] = description;
    stream["status"] = "available";
    stream["delivery_method"] = createIndicationPayloadDelivery(stream_id);
    return stream;
}

// Request handlers

void E3Agent::handleSetupRequest(const json& request, std::string& response) {
    json response_json;
    json e3_setup_response;
    uint32_t request_id = 0;

    try {
        const json& e3_setup_req = request;

        request_id = e3_setup_req.value("id", 0u);
        std::string protocol_version = e3_setup_req.value("e3apProtocolVersion", "");
        std::string dapp_name = e3_setup_req.value("dAppName", "unknown");
        std::string dapp_version = e3_setup_req.value("dAppVersion", "unknown");
        std::string vendor = e3_setup_req.value("vendor", "unknown");

        NVLOGC_FMT(TAG_E3, "E3 Setup Request from dApp '{}' v{} by {} (E3AP v{})",
                   dapp_name, dapp_version, vendor, protocol_version);

        if (protocol_version != e3::E3AP_PROTOCOL_VERSION) {
            NVLOGC_FMT(TAG_E3, "E3 Setup rejected: protocol version mismatch (received '{}', expected '{}')",
                       protocol_version, e3::E3AP_PROTOCOL_VERSION);
            json error_resp;
            error_resp["type"] = "setupResponse";
            error_resp["id"] = generateMessageId();
            error_resp["requestId"] = request_id;
            error_resp["responseCode"] = "negative";
            error_resp["message"] = "protocol version mismatch";
            error_resp["e3apProtocolVersion"] = e3::E3AP_PROTOCOL_VERSION;
            response = error_resp.dump();
            return;
        }

        // Generate dApp ID during setup phase
        const uint32_t dapp_id = generateDappId();
        {
            std::lock_guard<std::mutex> lock(e3_dapps_mutex);
            e3_connected_dapps[dapp_id] = {std::chrono::steady_clock::now()};
        }

        // Create E3AP Setup Response
        e3_setup_response["type"] = "setupResponse";
        e3_setup_response["id"] = generateMessageId();
        e3_setup_response["requestId"] = request_id;
        e3_setup_response["responseCode"] = "positive";
        e3_setup_response["e3apProtocolVersion"] = e3::E3AP_PROTOCOL_VERSION;
        e3_setup_response["dAppIdentifier"] = dapp_id;
        e3_setup_response["ranIdentifier"] = e3::RAN_IDENTIFIER;

        // Available data streams - all L2 KPIs, all JSON-delivered inside indications.
        json available_data_streams = json::array();

        // Timing / topology streams
        available_data_streams.push_back(createIndicationPayloadStream("timestamp", "uint64", "Agent-side software timestamp (ns)"));
        available_data_streams.push_back(createIndicationPayloadStream("timestamp_tai", "uint64", "TAI timestamp (ns)"));
        available_data_streams.push_back(createIndicationPayloadStream("sfn", "uint16", "Network frame timing information"));
        available_data_streams.push_back(createIndicationPayloadStream("slot", "uint16", "Network slot timing information"));
        available_data_streams.push_back(createIndicationPayloadStream("cell_id", "uint16", "Physical Cell ID"));
        available_data_streams.push_back(createIndicationPayloadStream("n_cells", "uint16", "Number of cells"));
        available_data_streams.push_back(createIndicationPayloadStream("n_ue", "uint16", "Number of UEs scheduled in this slot [cell-level]"));

        // Per-UE identity
        available_data_streams.push_back(createIndicationPayloadStream("rnti", "uint16", "UE Radio Network Temporary Identifier [per-UE]"));

        // prb_stats_t
        available_data_streams.push_back(createIndicationPayloadStream("dl_prb", "uint32", "DL PRBs scheduled [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_prb", "uint32", "UL PRBs scheduled [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("dl_prb_retx", "uint32", "DL PRBs used for retransmissions [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_prb_retx", "uint32", "UL PRBs used for retransmissions [per-UE]"));

        // tbs_stats_t
        available_data_streams.push_back(createIndicationPayloadStream("dl_aggr_tbs", "uint64", "DL aggregate transport block size (bytes) [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_aggr_tbs", "uint64", "UL aggregate transport block size (bytes) [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("dl_curr_tbs", "uint64", "DL current transport block size (bytes) [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_curr_tbs", "uint64", "UL current transport block size (bytes) [per-UE]"));

        // per_lcid_bytes_t
        available_data_streams.push_back(createIndicationPayloadStream(
            "per_lcid_dl_bytes", "array(uint64)", fmt::format("DL bytes per logical channel, index 0..MAX_LCID-1 ({}) [per-UE]", e3::MAX_LCID)));
        available_data_streams.push_back(createIndicationPayloadStream(
            "per_lcid_ul_bytes", "array(uint64)", fmt::format("UL bytes per logical channel, index 0..MAX_LCID-1 ({}) [per-UE]", e3::MAX_LCID)));

        // mcs_index_stats_t
        available_data_streams.push_back(createIndicationPayloadStream("dl_mcs", "uint32", "DL MCS index [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_mcs", "uint32", "UL MCS index [per-UE]"));

        // wb_cqi_t
        available_data_streams.push_back(createIndicationPayloadStream("wb_cqi", "uint16", "Wideband CQI [per-UE]"));

        // tb_stats_t
        available_data_streams.push_back(createIndicationPayloadStream(
            "dl_harq_rounds", "array(uint32)", fmt::format("DL HARQ round counts, index 0..MAX_DL_HARQ_ROUNDS-1 ({}) [per-UE]", e3::MAX_DL_HARQ_ROUNDS)));
        available_data_streams.push_back(createIndicationPayloadStream("dl_errors", "uint32", "DL transport block errors [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_errors", "uint32", "UL transport block errors [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("dl_bler", "float64", "DL block error rate [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("ul_bler", "float64", "UL block error rate [per-UE]"));

        // snr_stats_t
        available_data_streams.push_back(createIndicationPayloadStream("pusch_snr", "int16", "PUSCH SNR (dB) [per-UE]"));
        available_data_streams.push_back(createIndicationPayloadStream("pucch_snr", "int16", "PUCCH SNR (dB) [per-UE]"));

        // bsr_stats_t
        available_data_streams.push_back(createIndicationPayloadStream("total_bsr", "uint64", "Total buffer status report (bytes) [per-UE]"));

        // phr_stats_t
        available_data_streams.push_back(createIndicationPayloadStream("phr", "int32", "Power headroom report (dB) [per-UE]"));

        // E3-RanFunctionDefinition: telemetry IDs 1..STREAM_TYPE_COUNT
        json telemetry_id_list = json::array();
        for (uint32_t id = 1; id <= e3::STREAM_TYPE_COUNT; ++id) {
            telemetry_id_list.push_back(id);
        }

        json ran_function;
        ran_function["ranFunctionIdentifier"] = e3::RAN_FUNCTION_ID_NVIDIA_KPM;
        ran_function["telemetryIdentifierList"] = telemetry_id_list;
        ran_function["controlIdentifierList"] = json::array();
        ran_function["ranFunctionData"] = available_data_streams;

        e3_setup_response["ranFunctionList"] = json::array({ran_function});

        response_json = e3_setup_response;

        NVLOGC_FMT(TAG_E3, "E3 Setup successful for dApp '{}' assigned ID: {}", dapp_name, dapp_id);

    } catch (const json::exception& e) {
        NVLOGC_FMT(TAG_E3, "Error processing E3 Setup Request: {}", e.what());
        json error_resp;
        error_resp["type"] = "setupResponse";
        error_resp["id"] = generateMessageId();
        error_resp["requestId"] = request_id;
        error_resp["responseCode"] = "negative";
        error_resp["message"] = "invalid setup request format";
        response = error_resp.dump();
        return;
    }

    response = response_json.dump();
}

void E3Agent::handleSubscriptionRequest(const json& request, std::string& response)
{
    json response_json;
    json e3_sub_response;
    uint32_t dapp_id = 0;
    uint32_t request_id = 0;

    try {
        const json& e3_sub_req = request;
        dapp_id = e3_sub_req.at("dAppIdentifier").get<uint32_t>();
        request_id = e3_sub_req.value("id", 0u);

        if (!updateDappActivity(dapp_id)) {
            NVLOGC_FMT(TAG_E3, "Subscription rejected for non-connected dApp {}", dapp_id);
            e3_sub_response["responseCode"] = "negative";
            e3_sub_response["message"] = "dApp not connected or timed out";
        } else {
            uint32_t ran_func_id = e3_sub_req.at("ranFunctionIdentifier").get<uint32_t>();
            if (ran_func_id != e3::RAN_FUNCTION_ID_NVIDIA_KPM) {
                NVLOGC_FMT(TAG_E3, "Subscription rejected: unsupported ranFunctionIdentifier {} (expected {})",
                           ran_func_id, e3::RAN_FUNCTION_ID_NVIDIA_KPM);
                e3_sub_response["responseCode"] = "negative";
                e3_sub_response["message"] = "unsupported ranFunctionIdentifier";
            } else {
                auto telemetry_ids = e3_sub_req.value("telemetryIdentifierList", std::vector<uint32_t>{});
                auto control_ids = e3_sub_req.value("controlIdentifierList", std::vector<uint32_t>{});
                uint32_t periodicity_us = e3_sub_req.value("periodicity", 100000u);
                uint32_t subscription_time_s = e3_sub_req.value("subscriptionTime", 0u);

                e3::StreamType stream_bitfield = e3::StreamType::NONE;
                bool valid = true;

                // NVIDIA KPM: telemetry-only, no control dispatch yet.
                // Relax to (telemetry_ids.empty() && control_ids.empty()) when controls are implemented.
                if (telemetry_ids.empty()) {
                    NVLOGC_FMT(TAG_E3, "Subscription rejected: empty telemetryIdentifierList");
                    e3_sub_response["responseCode"] = "negative";
                    e3_sub_response["message"] = "telemetryIdentifierList must not be empty";
                    valid = false;
                }

                // Validate all telemetry IDs
                for (uint32_t tid : telemetry_ids) {
                    e3::StreamType st = e3::telemetryIdToStreamType(tid);
                    if (st == e3::StreamType::NONE) {
                        NVLOGC_FMT(TAG_E3, "Subscription rejected: invalid telemetry ID {}", tid);
                        e3_sub_response["responseCode"] = "negative";
                        e3_sub_response["message"] = "invalid telemetry identifier";
                        valid = false;
                        break;
                    }
                    stream_bitfield |= st;
                }

                if (valid) {
                    uint32_t sub_id = generateSubscriptionId();
                    auto now = std::chrono::steady_clock::now();
                    auto expiry = (subscription_time_s > 0)
                        ? now + std::chrono::seconds(subscription_time_s)
                        : std::chrono::steady_clock::time_point::max();

                    {
                        std::lock_guard<std::mutex> lock(e3_subscriptions_mutex);
                        e3_subscriptions[sub_id] = {sub_id, dapp_id, ran_func_id, telemetry_ids, stream_bitfield, periodicity_us, now, expiry};
                    }

                    std::string ids_str = fmt::format("[{}]", fmt::join(telemetry_ids, ","));
                    NVLOGC_FMT(TAG_E3, "E3 Subscription {} created for dApp {} (ranFunction={}, telemetryIds={})",
                               sub_id, dapp_id, ran_func_id, ids_str);

                    e3_sub_response["responseCode"] = "positive";
                    e3_sub_response["subscriptionId"] = sub_id;
                    e3_sub_response["ranFunctionIdentifier"] = ran_func_id;
                    e3_sub_response["telemetryGrantedList"] = telemetry_ids;
                    e3_sub_response["controlGrantedList"] = json::array();
                    e3_sub_response["periodicity"] = periodicity_us;
                }
            }
        }

        e3_sub_response["type"] = "subscriptionResponse";
        e3_sub_response["id"] = generateMessageId();
        e3_sub_response["requestId"] = request_id;
        e3_sub_response["dAppIdentifier"] = dapp_id;
        response_json = e3_sub_response;

    } catch (const json::exception& e) {
        NVLOGC_FMT(TAG_E3, "Error processing E3 Subscription Request: {}", e.what());
        json e3_err_resp;
        e3_err_resp["type"] = "subscriptionResponse";
        e3_err_resp["id"] = generateMessageId();
        e3_err_resp["requestId"] = request_id;
        e3_err_resp["responseCode"] = "negative";
        e3_err_resp["message"] = "missing or invalid parameters in subscription request";
        e3_err_resp["dAppIdentifier"] = dapp_id;
        response_json = e3_err_resp;
    }
    response = response_json.dump();
}

void E3Agent::handleSubscriptionDelete(const json& request, std::string& response)
{
    json response_json;
    json e3_unsub_response;
    uint32_t dapp_id = 0;
    uint32_t sub_id = 0;
    uint32_t request_id = 0;

    try {
        const json& e3_unsub_req = request;
        dapp_id = e3_unsub_req.at("dAppIdentifier").get<uint32_t>();
        sub_id = e3_unsub_req.at("subscriptionId").get<uint32_t>();
        request_id = e3_unsub_req.value("id", 0u);

        bool found = false;
        {
            std::lock_guard<std::mutex> lock(e3_subscriptions_mutex);
            auto it = e3_subscriptions.find(sub_id);
            if (it != e3_subscriptions.end() && it->second.dapp_id == dapp_id) {
                e3_subscriptions.erase(it);
                found = true;
            }
        }

        updateDappActivity(dapp_id);

        if (found) {
            NVLOGC_FMT(TAG_E3, "E3 Subscription Delete successful for subscription {}", sub_id);
            e3_unsub_response["responseCode"] = "positive";
        } else {
            NVLOGC_FMT(TAG_E3, "E3 Subscription Delete failed for sub_id {}, dApp_id {}", sub_id, dapp_id);
            e3_unsub_response["responseCode"] = "negative";
            e3_unsub_response["message"] = "subscription not found or dApp ID mismatch";
        }

        e3_unsub_response["subscriptionId"] = sub_id;
        e3_unsub_response["type"] = "subscriptionResponse";
        e3_unsub_response["id"] = generateMessageId();
        e3_unsub_response["requestId"] = request_id;
        e3_unsub_response["dAppIdentifier"] = dapp_id;
        response_json = e3_unsub_response;

    } catch (const json::exception& e) {
        NVLOGC_FMT(TAG_E3, "Error processing E3 Subscription Delete Request: {}", e.what());
        json e3_err_resp;
        e3_err_resp["type"] = "subscriptionResponse";
        e3_err_resp["id"] = generateMessageId();
        e3_err_resp["requestId"] = request_id;
        e3_err_resp["responseCode"] = "negative";
        e3_err_resp["message"] = "missing or invalid parameters in subscription delete request";
        e3_err_resp["dAppIdentifier"] = dapp_id;
        e3_err_resp["subscriptionId"] = sub_id;
        response_json = e3_err_resp;
    }
    response = response_json.dump();
}


void E3Agent::handleControlMessage(const json& request, std::string& response)
{
    json ack;
    uint32_t dapp_id = 0;
    uint32_t request_id = 0;

    try {
        dapp_id = request.at("dAppIdentifier").get<uint32_t>();
        request_id = request.value("id", 0u);

        updateDappActivity(dapp_id);

        // Control actions not implemented
        ack["responseCode"] = "negative";
        ack["message"] = "control actions not implemented";

    } catch (const json::exception& e) {
        ack["responseCode"] = "negative";
        ack["message"] = "invalid control message format";
        NVLOGC_FMT(TAG_E3, "Error processing E3 Control Message: {}. Request: {}", e.what(), request.dump());
    }

    ack["type"] = "messageAck";
    ack["id"] = generateMessageId();
    ack["requestId"] = request_id;
    ack["dAppIdentifier"] = dapp_id;
    response = ack.dump();
}
