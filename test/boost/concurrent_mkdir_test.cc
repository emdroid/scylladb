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

using namespace seastar;

BOOST_AUTO_TEST_SUITE(concurrent_mkdir_test)

static logger mkdirlog("concurrent_mkdir");

constexpr int DIR_COUNT = 10000;

// Stress test for concurrent mkdir to reproduce EPERM errors.
// Related to: https://github.com/scylladb/scylladb/issues/28259
// Run with: ./test.py --mode=dev test/boost/concurrent_mkdir_test.cc --smp 16
SEASTAR_TEST_CASE(test_concurrent_mkdir_stress) {
    for (int i = 0; i < DIR_COUNT; ++i) {
        auto dir = fmt::format("testlog/test_dir_{}", i);
        
        co_await smp::invoke_on_all([dir] () -> future<> {
            try {
                co_await touch_directory(dir);
            } catch (const std::system_error& e) {
                mkdirlog.warn("Error on shard {}: errno={} {}", this_shard_id(), e.code().value(), e.what());
                _exit(1);
            }
        });
    }

    for (int i = 0; i < DIR_COUNT; ++i) {
        auto dir = fmt::format("testlog/test_dir_{}", i);
        co_await remove_file(dir);
    }
    
    mkdirlog.info("Test completed with {} shards", smp::count);
}

BOOST_AUTO_TEST_SUITE_END()