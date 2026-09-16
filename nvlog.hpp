/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Minimal drop-in replacement for NVIDIA's internal nvlog.hpp.
 *
 * The production nvlog library (structured, ring-buffered, multi-sink logger)
 * is internal to the Aerial SDK and not part of this public interface. This
 * shim reproduces just the macro surface that e3_agent.hpp / e3_agent.cpp
 * call, so those two files can be vendored into a standalone process
 * byte-for-byte unmodified. All it does is print to stderr with fmt.
 */
#ifndef NVLOG_SHIM_HPP
#define NVLOG_SHIM_HPP

#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <fmt/format.h>
#include <fmt/ranges.h>

// Tag namespace base; e3_agent.hpp derives TAG_E3 from this.
#define NVLOG_TAG_BASE_CUPHY_CONTROLLER 700

// Event codes passed to NVLOGE_FMT/NVLOGF_FMT (ignored by this shim; kept so
// call sites compile unmodified).
enum AerialEventCode {
    AERIAL_SYSTEM_API_EVENT = 1,
    AERIAL_CONFIG_EVENT     = 2,
};

namespace nvlog_shim {

// Real nvlog is level-filtered; e3_agent.cpp logs a DBG line on every single
// slot (500us cadence), so an unfiltered shim floods stdout/pipes within
// seconds. Default to INFO-and-above, like a normal production log level;
// override with NVLOG_SHIM_LEVEL=CRIT|ERR|WARN|INFO|DBG|VERB for debugging.
enum class Level { CRIT = 0, ERR = 1, WARN = 2, INFO = 3, DBG = 4, VERB = 5 };

inline Level currentLevel()
{
    static const Level lvl = [] {
        const char* env = std::getenv("NVLOG_SHIM_LEVEL");
        if (!env) return Level::INFO;
        std::string s(env);
        if (s == "CRIT") return Level::CRIT;
        if (s == "ERR") return Level::ERR;
        if (s == "WARN") return Level::WARN;
        if (s == "INFO") return Level::INFO;
        if (s == "DBG") return Level::DBG;
        if (s == "VERB") return Level::VERB;
        return Level::INFO;
    }();
    return lvl;
}

inline void emit(const char* levelName, Level level, int tag, const std::string& msg)
{
    if (level > currentLevel()) return;
    const auto now = std::chrono::system_clock::now();
    const auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
    localtime_r(&t, &tm_buf);
    fmt::print(stderr, "{:02}:{:02}:{:02}.{:03} [{}] [tag={}] {}\n",
               tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, ms, levelName, tag, msg);
}
} // namespace nvlog_shim

#define NVLOGC_FMT(tag, ...) ::nvlog_shim::emit("CRIT", ::nvlog_shim::Level::CRIT, (tag), fmt::format(__VA_ARGS__))
#define NVLOGE_FMT(tag, event, ...) ::nvlog_shim::emit("ERR ", ::nvlog_shim::Level::ERR, (tag), fmt::format(__VA_ARGS__))
#define NVLOGW_FMT(tag, ...) ::nvlog_shim::emit("WARN", ::nvlog_shim::Level::WARN, (tag), fmt::format(__VA_ARGS__))
#define NVLOGI_FMT(tag, ...) ::nvlog_shim::emit("INFO", ::nvlog_shim::Level::INFO, (tag), fmt::format(__VA_ARGS__))
#define NVLOGD_FMT(tag, ...) ::nvlog_shim::emit("DBG ", ::nvlog_shim::Level::DBG, (tag), fmt::format(__VA_ARGS__))
#define NVLOGV_FMT(tag, ...) ::nvlog_shim::emit("VERB", ::nvlog_shim::Level::VERB, (tag), fmt::format(__VA_ARGS__))
#define NVLOGF_FMT(tag, event, ...) \
    do { ::nvlog_shim::emit("FATAL", ::nvlog_shim::Level::CRIT, (tag), fmt::format(__VA_ARGS__)); std::abort(); } while (0)

#endif // NVLOG_SHIM_HPP
