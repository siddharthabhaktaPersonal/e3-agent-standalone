# SPDX-License-Identifier: Apache-2.0
#
# Standalone E3 Agent + sample E3 Manager - Layer-2 (MAC) KPI edition.
#
# Plain Linux CMake project, no CUDA/cuPHY/ClickHouse/nvipc - only needs
# libzmq, cppzmq, nlohmann-json, and fmt (for the nvlog shim). No shared
# memory dependency either: every L2 KPI stream travels inline in JSON.
#
# LANGUAGES includes C because examples/du_integration_example.c is
# compiled with a real C compiler (not just C++) to prove e3_l2_kpi.h -
# the header a DU includes - has zero C++ dependency.

cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(e3_agent_standalone LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(fmt REQUIRED)

# cppzmq (the C++ zmq.hpp binding) isn't packaged on every distro (e.g.
# Ubuntu 20.04 ships libzmq3-dev with only the C API). Prefer the system
# package if present; otherwise fall back to the single-header copy vendored
# in third_party/ (from https://github.com/zeromq/cppzmq, MIT licensed).
find_package(cppzmq QUIET)
if(NOT cppzmq_FOUND)
    message(STATUS "cppzmq CMake package not found; using vendored third_party/zmq.hpp")
endif()

# ----------------------------------------------------------------------
# e3agent: the E3 interface + DataLake + the C API a DU links against.
# Everything that isn't a driver program (main.cpp, e3_manager.cpp, the DU
# example) lives here so all three consumers share one build of it.
add_library(e3agent STATIC
    e3_agent.cpp
    data_lake.cpp
    e3_c_api.cpp
)
target_include_directories(e3agent PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(e3agent PUBLIC
    Threads::Threads
    nlohmann_json::nlohmann_json
    fmt::fmt
)
if(cppzmq_FOUND)
    target_link_libraries(e3agent PUBLIC cppzmq)
else()
    target_include_directories(e3agent PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
    target_link_libraries(e3agent PUBLIC zmq)
endif()

# ----------------------------------------------------------------------
# E3 Agent standalone process (synthetic slot-clock feeder, no real DU)
add_executable(e3_agent_standalone main.cpp)
target_link_libraries(e3_agent_standalone PRIVATE e3agent)

# ----------------------------------------------------------------------
# Sample E3 Manager (dApp-side reference client) - independent of e3agent,
# it only ever speaks the wire protocol, never touches DataLake directly.
add_executable(e3_manager_sample
    e3_manager.cpp
)
target_include_directories(e3_manager_sample PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(e3_manager_sample PRIVATE
    Threads::Threads
    nlohmann_json::nlohmann_json
    fmt::fmt
)
if(cppzmq_FOUND)
    target_link_libraries(e3_manager_sample PRIVATE cppzmq)
else()
    target_include_directories(e3_manager_sample PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
    target_link_libraries(e3_manager_sample PRIVATE zmq)
endif()

# ----------------------------------------------------------------------
# Sample DU integration, in plain C - demonstrates the push-model C API a
# real DU would call (e3_l2_kpi.h). Compiled as C to prove that header has
# no C++ dependency; links against the same e3agent library main.cpp uses.
add_executable(du_integration_example examples/du_integration_example.c)
target_link_libraries(du_integration_example PRIVATE e3agent)
