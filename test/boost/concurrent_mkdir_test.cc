/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN

#include <seastar/coroutine/all.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/seastar.hh>
#include "test/lib/log.hh"
#include <fmt/core.h>

#include <string_view>

using namespace seastar;

BOOST_AUTO_TEST_SUITE(concurrent_mkdir_test)

static logger mkdirlog("concurrent_mkdir");

constexpr int ITERATIONS_COUNT = 100000;

// Stress test for concurrent mkdir to reproduce EPERM errors.
// Related to: https://github.com/scylladb/scylladb/issues/28259
// Run with: ./test.py --mode=dev test/boost/concurrent_mkdir_test.cc --smp 16
SEASTAR_TEST_CASE(test_concurrent_mkdir_stress) {
    for (int i = 0; i < ITERATIONS_COUNT; ++i) {
        auto dir = fmt::format("testlog/test_dir_{}/node/status", i);

        co_await smp::invoke_on_all([dir] () -> future<> {
            try {
                co_await recursive_touch_directory(dir);
                co_await touch_directory(dir + "/upload");
            } catch (const std::system_error& e) {
                mkdirlog.warn("Error on shard {}: errno={} {}", this_shard_id(), e.code().value(), e.what());
                _exit(1);
            }
        });
    }

    for (int i = 0; i < ITERATIONS_COUNT; ++i) {
        auto dir = fmt::format("testlog/test_dir_{}", i);
        co_await remove_file(dir + "/node/status/upload");
        co_await remove_file(dir + "/node/status");
        co_await remove_file(dir + "/node");
        co_await remove_file(dir);
    }
    
    mkdirlog.info("Test completed with {} shards", smp::count);
}

// Aggressive stress test that thrashes a single directory with concurrent create/delete operations.
// This more closely mimics the conditions that trigger EPERM on OverlayFS.
// Related to: https://github.com/scylladb/scylladb/issues/28259
SEASTAR_TEST_CASE(test_concurrent_mkdir_delete_thrashing) {
    const auto target_dir = std::string_view{"testlog/race_target_dir"};

    // Shared cleanup helper
    auto cleanup = [&target_dir]() -> future<> {
        co_await remove_file(target_dir).then_wrapped([](auto f) {
            try { f.get(); } catch (...) { /* ignore cleanup errors */ }
        });
    };

    // Clean start
    co_await cleanup();

    std::atomic<int> eperm_count{0};

    // Only shard 0 deletes, all other shards create - maximizes concurrent mkdir collisions
    co_await smp::invoke_on_all([target_dir, &eperm_count] () -> future<> {
        auto shard = this_shard_id();
        for (int i = 0; i < ITERATIONS_COUNT; ++i) {
            if (shard == 0) {
                // Single deleting shard - ignore all errors
                co_await remove_file(target_dir).then_wrapped([](auto f) {
                    try { f.get(); } catch (...) { /* ignore all delete errors */ }
                });
            } else {
                // All other shards create
                try {
                    co_await touch_directory(target_dir);
                } catch (const std::system_error& e) {
                    if (e.code().value() == EPERM) {
                        eperm_count.fetch_add(1);
                        mkdirlog.error("EPERM on shard {} iteration {}: {}", shard, i, e.what());
                    } else {
                        mkdirlog.warn("Unexpected error on shard {} iteration {}: errno={} {}", 
                                    shard, i, e.code().value(), e.what());
                    }
                }
            }
        }
    });

    // Clean up
    co_await cleanup();

    if (eperm_count.load() > 0) {
        mkdirlog.error("REPRODUCED: EPERM occurred {} times across {} shards!", 
                      eperm_count.load(), smp::count);
    } else {
        mkdirlog.info("Thrashing test completed with {} shards, no EPERM detected", smp::count);
    }
}

BOOST_AUTO_TEST_SUITE_END()