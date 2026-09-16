/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * E3 interface - Layer-2 (MAC) KPI edition.
 *
 * This supersedes the original L1 (PHY telemetry, SHM-backed) E3 interface
 * vendored from NVIDIA's cuPHY-CP/data_lake. All L1 streams (IQ samples,
 * PUSCH PDU bytes, H-estimates, SRS) and the POSIX shared-memory data
 * channel they required are gone - every stream here is a small per-UE
 * scalar/array KPI that fits comfortably in the indication's JSON
 * protocolData, so there is no SHM channel at all in this version.
 *
 * The session mechanics (REQ/REP setup, PUB/SUB subscribe/indications) are
 * unchanged in spirit from the original. Same wire/ABI caution applies to
 * e3::StreamType here as it did there: IDs are stable values used in
 * E3-SubscriptionRequest telemetryIdentifierList and E3-RanFunctionDefinition.
 * Append-only; never reorder or reuse a bit position.
 */

#ifndef E3_AGENT_HPP
#define E3_AGENT_HPP

#include <string_view>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <map>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <cstdint>

#include <zmq.hpp>
#include <nlohmann/json.hpp>

#include "nvlog.hpp"
#include "e3_l2_kpi.h"

#define TAG_E3 (NVLOG_TAG_BASE_CUPHY_CONTROLLER + 7) // "CTL.E3"

// Forward declaration
class DataLake;
using json = nlohmann::json;

// E3 Protocol definitions
namespace e3 {

/**
 * E3AP Telemetry stream types as bit flags for efficient internal processing.
 *
 * Telemetry ID = bit_position + 1 (e.g. RNTI = bit 7 -> telemetry ID 8).
 *
 * DO NOT reorder or remove. IDs are stable wire protocol values used in
 * E3-SubscriptionRequest telemetryIdentifierList and E3-RanFunctionDefinition.
 * New entries go at the end only. A plain uint64_t comfortably covers this
 * L2 KPI set (30 streams); switch to __uint128_t/std::bitset if it ever
 * grows past 64.
 */
enum class StreamType : uint64_t {
    NONE                = 0,

    // Slot-shared (root-level in protocolData)
    TIMESTAMP           = uint64_t(1) << 0,
    TIMESTAMP_TAI       = uint64_t(1) << 1,
    SFN                 = uint64_t(1) << 2,
    SLOT                = uint64_t(1) << 3,
    CELL_ID             = uint64_t(1) << 4,
    N_CELLS             = uint64_t(1) << 5,
    N_UE                = uint64_t(1) << 6,

    // Per-UE (delivered inside cells[].ues[])
    RNTI                = uint64_t(1) << 7,

    // prb_stats_t
    DL_PRB              = uint64_t(1) << 8,
    UL_PRB              = uint64_t(1) << 9,
    DL_PRB_RETX         = uint64_t(1) << 10,
    UL_PRB_RETX         = uint64_t(1) << 11,

    // tbs_stats_t
    DL_AGGR_TBS         = uint64_t(1) << 12,
    UL_AGGR_TBS         = uint64_t(1) << 13,
    DL_CURR_TBS         = uint64_t(1) << 14,
    UL_CURR_TBS         = uint64_t(1) << 15,

    // per_lcid_bytes_t (each an array of MAX_LCID elements)
    PER_LCID_DL_BYTES   = uint64_t(1) << 16,
    PER_LCID_UL_BYTES   = uint64_t(1) << 17,

    // mcs_index_stats_t
    DL_MCS              = uint64_t(1) << 18,
    UL_MCS              = uint64_t(1) << 19,

    // wb_cqi_t
    WB_CQI              = uint64_t(1) << 20,

    // tb_stats_t
    DL_HARQ_ROUNDS      = uint64_t(1) << 21, // array of MAX_DL_HARQ_ROUNDS elements
    DL_ERRORS           = uint64_t(1) << 22,
    UL_ERRORS           = uint64_t(1) << 23,
    DL_BLER             = uint64_t(1) << 24,
    UL_BLER             = uint64_t(1) << 25,

    // snr_stats_t
    PUSCH_SNR           = uint64_t(1) << 26,
    PUCCH_SNR           = uint64_t(1) << 27,

    // bsr_stats_t
    TOTAL_BSR           = uint64_t(1) << 28,

    // phr_stats_t
    PHR                 = uint64_t(1) << 29,
};

constexpr uint32_t STREAM_TYPE_COUNT = 30;

/** Max logical channels reported in per_lcid_bytes_t arrays. Single source
 * of truth is E3_MAX_LCID in e3_l2_kpi.h (the DU-facing C header) - this is
 * just a typed C++ alias for use in this file's fmt::format() calls. */
constexpr uint32_t MAX_LCID = E3_MAX_LCID;
/** Max HARQ rounds reported in tb_stats_t::dl_harq_rounds; see MAX_LCID note. */
constexpr uint32_t MAX_DL_HARQ_ROUNDS = E3_MAX_DL_HARQ_ROUNDS;

/** E3AP protocol version supported by this agent implementation */
constexpr std::string_view E3AP_PROTOCOL_VERSION = "2.0.0";
/** RAN identifier used in E3 Setup messages */
constexpr std::string_view RAN_IDENTIFIER = "NVIDIA_L2";
/** RAN function ID for L2 KPM (Key Performance Monitoring) */
constexpr uint32_t RAN_FUNCTION_ID_NVIDIA_KPM = 2;

/**
 * Convert telemetry ID (1-based) to StreamType.
 *
 * @param[in] id Telemetry identifier (1-based, valid range 1 to STREAM_TYPE_COUNT).
 *               IDs of 0 or greater than STREAM_TYPE_COUNT are treated as invalid.
 * @return Corresponding StreamType bitfield value, or StreamType::NONE for invalid IDs.
 */
constexpr StreamType telemetryIdToStreamType(uint32_t id) noexcept
{
    if (id == 0 || id > STREAM_TYPE_COUNT) return StreamType::NONE;
    return static_cast<StreamType>(uint64_t(1) << (id - 1));
}

/**
 * Converts string stream name to StreamType enum
 *
 * @param[in] stream_name The stream name as string
 * @return Corresponding StreamType enum value
 */
constexpr StreamType streamNameToType(const std::string_view stream_name) noexcept
{
    if (stream_name == "timestamp") return StreamType::TIMESTAMP;
    if (stream_name == "timestamp_tai") return StreamType::TIMESTAMP_TAI;
    if (stream_name == "sfn") return StreamType::SFN;
    if (stream_name == "slot") return StreamType::SLOT;
    if (stream_name == "cell_id") return StreamType::CELL_ID;
    if (stream_name == "n_cells") return StreamType::N_CELLS;
    if (stream_name == "n_ue") return StreamType::N_UE;
    if (stream_name == "rnti") return StreamType::RNTI;
    if (stream_name == "dl_prb") return StreamType::DL_PRB;
    if (stream_name == "ul_prb") return StreamType::UL_PRB;
    if (stream_name == "dl_prb_retx") return StreamType::DL_PRB_RETX;
    if (stream_name == "ul_prb_retx") return StreamType::UL_PRB_RETX;
    if (stream_name == "dl_aggr_tbs") return StreamType::DL_AGGR_TBS;
    if (stream_name == "ul_aggr_tbs") return StreamType::UL_AGGR_TBS;
    if (stream_name == "dl_curr_tbs") return StreamType::DL_CURR_TBS;
    if (stream_name == "ul_curr_tbs") return StreamType::UL_CURR_TBS;
    if (stream_name == "per_lcid_dl_bytes") return StreamType::PER_LCID_DL_BYTES;
    if (stream_name == "per_lcid_ul_bytes") return StreamType::PER_LCID_UL_BYTES;
    if (stream_name == "dl_mcs") return StreamType::DL_MCS;
    if (stream_name == "ul_mcs") return StreamType::UL_MCS;
    if (stream_name == "wb_cqi") return StreamType::WB_CQI;
    if (stream_name == "dl_harq_rounds") return StreamType::DL_HARQ_ROUNDS;
    if (stream_name == "dl_errors") return StreamType::DL_ERRORS;
    if (stream_name == "ul_errors") return StreamType::UL_ERRORS;
    if (stream_name == "dl_bler") return StreamType::DL_BLER;
    if (stream_name == "ul_bler") return StreamType::UL_BLER;
    if (stream_name == "pusch_snr") return StreamType::PUSCH_SNR;
    if (stream_name == "pucch_snr") return StreamType::PUCCH_SNR;
    if (stream_name == "total_bsr") return StreamType::TOTAL_BSR;
    if (stream_name == "phr") return StreamType::PHR;
    return StreamType::NONE;
}

/** Bitwise OR operator for StreamType flags */
constexpr StreamType operator|(const StreamType lhs, const StreamType rhs) noexcept
{
    return static_cast<StreamType>(static_cast<uint64_t>(lhs) | static_cast<uint64_t>(rhs));
}

/** Bitwise OR assignment operator for StreamType flags */
constexpr StreamType& operator|=(StreamType& lhs, const StreamType rhs) noexcept
{
    lhs = lhs | rhs;
    return lhs;
}

/** Bitwise AND operator for StreamType flags */
constexpr StreamType operator&(const StreamType lhs, const StreamType rhs) noexcept
{
    return static_cast<StreamType>(static_cast<uint64_t>(lhs) & static_cast<uint64_t>(rhs));
}

/** Bitwise NOT operator for StreamType flags */
constexpr StreamType operator~(const StreamType val) noexcept
{
    return static_cast<StreamType>(~static_cast<uint64_t>(val));
}

// Streams emitted inside ue_metrics[] (per-UE); everything else is cell-level.
constexpr StreamType PER_UE_STREAMS =
    StreamType::RNTI |
    StreamType::DL_PRB | StreamType::UL_PRB | StreamType::DL_PRB_RETX | StreamType::UL_PRB_RETX |
    StreamType::DL_AGGR_TBS | StreamType::UL_AGGR_TBS | StreamType::DL_CURR_TBS | StreamType::UL_CURR_TBS |
    StreamType::PER_LCID_DL_BYTES | StreamType::PER_LCID_UL_BYTES |
    StreamType::DL_MCS | StreamType::UL_MCS |
    StreamType::WB_CQI |
    StreamType::DL_HARQ_ROUNDS | StreamType::DL_ERRORS | StreamType::UL_ERRORS |
    StreamType::DL_BLER | StreamType::UL_BLER |
    StreamType::PUSCH_SNR | StreamType::PUCCH_SNR |
    StreamType::TOTAL_BSR |
    StreamType::PHR;

// All streams the (single) L2 indication path can provide. There is only
// one data path now (no PUSCH/SRS split like the L1 version had), so
// there's just one mask and sendDueIndications() fires for any non-empty
// subscription, gated only by periodicity.
constexpr StreamType ALL_PROVIDABLE_STREAMS =
    StreamType::TIMESTAMP | StreamType::TIMESTAMP_TAI | StreamType::SFN | StreamType::SLOT |
    StreamType::CELL_ID | StreamType::N_CELLS | StreamType::N_UE |
    PER_UE_STREAMS;

} // namespace e3

class E3Agent {
public:
    E3Agent(
        DataLake* dataLake,
        const uint16_t e3RepPort,
        const uint16_t e3PubPort,
        const uint16_t e3SubPort
    );
    ~E3Agent();

    bool init();
    void shutdown();

private:
    DataLake* dataLake;

    // How often the notifier thread wakes to check subscriptions for due
    // indications. This is a polling granularity, not a KPI refresh rate -
    // DataLake updates its buffer on its own cadence (see main.cpp's slot
    // clock); this just bounds how promptly a subscription's periodicity_us
    // is honored. 1ms is comfortably finer than any sane periodicity.
    static constexpr std::chrono::milliseconds NOTIFIER_TICK_INTERVAL{1};

    // E3 Agent configuration
    uint16_t e3RepPort;
    uint16_t e3PubPort;
    uint16_t e3SubPort;

    // ZMQ components
    zmq::context_t zmq_context;
    zmq::socket_t e3_rep_socket;  // Manager → Agent (REQ-REP)
    zmq::socket_t e3_pub_socket;  // Agent → Manager (indications)
    std::mutex e3_pub_socket_mutex_;
    zmq::socket_t e3_sub_socket;  // Manager → Agent (PUB-SUB commands)

    // Thread management
    std::thread e3_data_thread;
    std::thread e3_reaper_thread;
    std::thread e3_sub_thread;
    std::thread e3_notifier_thread;
    std::atomic<bool> e3_running{false};
    std::atomic<bool> e3_reaper_running{false};
    std::atomic<bool> e3_sub_running{false};
    std::atomic<bool> e3_notifier_running{false};

    // Active subscriptions
    struct E3Subscription {
        uint32_t subscription_id;
        uint32_t dapp_id;
        uint32_t ran_function_id;
        std::vector<uint32_t> telemetry_ids;      // Granted telemetry IDs (wire protocol values)
        e3::StreamType stream_bitfield;           // Internal bitfield for indication processing
        uint32_t periodicity_us;
        std::chrono::steady_clock::time_point last_update;
        std::chrono::steady_clock::time_point expiry_time;  // time_point::max() = indefinite
    };
    std::unordered_map<uint32_t, E3Subscription> e3_subscriptions;
    std::mutex e3_subscriptions_mutex;

    // Connected dApp managers
    struct DAppConnectionInfo {
        std::chrono::steady_clock::time_point last_activity_time;
    };
    std::map<uint32_t, DAppConnectionInfo> e3_connected_dapps;
    std::mutex e3_dapps_mutex;

    // Thread functions
    void dataServerThread();
    void reaperThread();
    void notifierThread();

    /** Walk active subscriptions and send indications to any that are due
     * (periodicity elapsed since last_update), reading the current snapshot
     * from DataLake. Called from notifierThread() on NOTIFIER_TICK_INTERVAL.
     */
    void sendDueIndications();
    void reapTimedOutDapps();
    void managerSubscriptionThread();

    /** Handle E3 Setup request (REQ-REP)
     *
     * @param[in] request JSON request containing dApp setup parameters
     * @param[out] response JSON response with dAppIdentifier and available streams
     */
    void handleSetupRequest(const json& request, std::string& response);

    /** Handle subscription request from dApp (PUB-SUB)
     *
     * @param[in] request JSON request with telemetryIdentifierList and ranFunctionIdentifier
     * @param[out] response JSON response with responseCode (positive/negative)
     */
    void handleSubscriptionRequest(const json& request, std::string& response);

    /** Handle subscription deletion request from dApp (PUB-SUB)
     *
     * @param[in] request JSON request containing subscriptionId
     * @param[out] response JSON response with responseCode (positive/negative)
     */
    void handleSubscriptionDelete(const json& request, std::string& response);

    /** Handle control action from dApp (PUB-SUB) - placeholder, no control dispatch yet
     *
     * @param[in] request JSON request containing dAppControlAction fields
     * @param[out] response JSON ack with responseCode
     */
    void handleControlMessage(const json& request, std::string& response);

    /** Dispatch incoming PUB-SUB messages by type
     *
     * @param[in] message Parsed JSON message from the dApp PUB socket
     */
    void handleManagerMessage(const json& message);

    /** Release a dApp connection and clean up associated subscriptions
     *
     * @param[in] dapp_id Identifier of the dApp to release
     */
    void releaseDapp(uint32_t dapp_id);

    /** Update last activity time for a connected dApp
     *
     * @param[in] dapp_id Identifier of the dApp
     * @return true if dApp exists and was updated, false if not found
     */
    bool updateDappActivity(uint32_t dapp_id);

    /** Broadcast E3 release message for the specified dApp
     *
     * @param[in] dapp_id Identifier of the dApp being released
     * @return true if message was sent successfully, false otherwise
     */
    bool sendRelease(uint32_t dapp_id);

    /** Generate a unique message identifier for E3AP messages
     *
     * @return Unique uint32_t message identifier
     */
    uint32_t generateMessageId();

    /** Generate a unique dApp identifier for new connections
     *
     * @return Unique uint32_t dApp identifier
     */
    uint32_t generateDappId();

    /** Generate a unique subscription identifier
     *
     * @return Unique uint32_t subscription identifier
     */
    uint32_t generateSubscriptionId();

    // Stream creation helper - every L2 KPI stream is JSON-delivered inside
    // protocolData; there is no shared-memory delivery method in this version.
    json createIndicationPayloadDelivery(const std::string& stream_id) const;
    json createIndicationPayloadStream(
        const std::string& stream_id,
        const std::string& data_type,
        const std::string& description
    ) const;
};

#endif // E3_AGENT_HPP
