# SPDX-License-Identifier: Apache-2.0
#
# Standalone E3 Agent + sample E3 Manager - Layer-2 (MAC) KPI edition.
#
# Plain Linux CMake project, no CUDA/cuPHY/ClickHouse/nvipc - only needs
# libzmq, cppzmq, nlohmann-json, and fmt (for the nvlog shim). No shared
# memory dependency either: every L2 KPI stream travels inline in JSON.

cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(e3_agent_standalone LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
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
# E3 Agent standalone process
add_executable(e3_agent_standalone
    main.cpp
    data_lake.cpp
    e3_agent.cpp
)
target_include_directories(e3_agent_standalone PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(e3_agent_standalone PRIVATE
    Threads::Threads
    nlohmann_json::nlohmann_json
    fmt::fmt
)
if(cppzmq_FOUND)
    target_link_libraries(e3_agent_standalone PRIVATE cppzmq)
else()
    target_include_directories(e3_agent_standalone PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
    target_link_libraries(e3_agent_standalone PRIVATE zmq)
endif()

# ----------------------------------------------------------------------
# Sample E3 Manager (dApp-side reference client)
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
