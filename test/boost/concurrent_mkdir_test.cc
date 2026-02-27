/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN

#include <seastar/coroutine/all.hh>
#include <seastar/testing/test_case.hh>
#include "seastar/core/loop.hh"
#include <seastar/core/smp.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/do_with.hh>
#include <seastar/util/file.hh>
#include "test/lib/log.hh"
#include <fmt/core.h>

#include <boost/range/irange.hpp>

using namespace seastar;

BOOST_AUTO_TEST_SUITE(concurrent_mkdir_test)

static logger mkdirlog("concurrent_mkdir");

constexpr int ITERATIONS_COUNT = 100000;

// Ignore all errors from a filesystem operation.
static future<> ignore_errors(future<> f) {
    return std::move(f).then_wrapped([](auto f) {
        try { f.get(); } catch (...) { }
    });
}

// Recursively remove a directory tree.
// Ignores errors (the tree may not exist).
static future<> force_remove_tree(std::string path) {
    co_await ignore_errors(recursive_remove_directory(std::filesystem::path(std::move(path))));
}

// Helper to report EPERM.
static void check_eperm(const std::system_error& err, std::atomic<int>& eperm_count,
                         unsigned shard_id, int iteration, const char* operation) {
    if (err.code().value() == EPERM) {
        eperm_count.fetch_add(1, std::memory_order_relaxed);
        mkdirlog.error("EPERM in {} on shard {} iteration {}: {}", operation, shard_id, iteration, err.what());
    }
    // Anything else (ENOENT, EEXIST, etc.) is expected under concurrent create/delete.
}

// ---------------------------------------------------------------------------
// Test 1 — Concurrent recursive_touch_directory from all shards
//
// All shards race to create the same nested directory tree. This is the
// simplest reproducer: many concurrent mkdir(2) calls for the same path
// components hitting the same XFS allocation group.
//
// Related to: https://github.com/scylladb/scylladb/issues/28259
// Run with: ./test.py --mode=dev test/boost/concurrent_mkdir_test.cc --smp 16
// ---------------------------------------------------------------------------
SEASTAR_TEST_CASE(test_concurrent_mkdir_stress) {
    // Each iteration creates a unique directory tree; all shards race to create
    // the same path simultaneously. Iterations are independent so they run in
    // parallel via max_concurrent_for_each.
    co_await max_concurrent_for_each(boost::irange(0, ITERATIONS_COUNT), smp::count,
            [](int idx) -> future<> {
        auto dir = fmt::format("testlog/test_dir_{}/node/status", idx);

        co_await smp::invoke_on_all([dir] () -> future<> {
            co_await parallel_for_each(boost::irange(0, 16), [dir] (int) -> future<> {
                try {
                    co_await recursive_touch_directory(dir);
                    co_await touch_directory(dir + "/upload");
                } catch (const std::system_error& e) {
                    mkdirlog.warn("Error on shard {}: errno={} {}", this_shard_id(), e.code().value(), e.what());
                    _exit(1);
                }
            });
        });
    });

    co_await max_concurrent_for_each(boost::irange(0, ITERATIONS_COUNT), smp::count, [](int idx) -> future<> {
        auto dir = fmt::format("testlog/test_dir_{}", idx);
        co_await ignore_errors(remove_file(dir + "/node/status/upload"));
        co_await ignore_errors(remove_file(dir + "/node/status"));
        co_await ignore_errors(remove_file(dir + "/node"));
        co_await ignore_errors(remove_file(dir));
    });

    mkdirlog.info("Test completed with {} shards", smp::count);
}

// ---------------------------------------------------------------------------
// Test 2 — mkdir + delete thrashing on a single path
//
// Shard 0 continuously deletes the target directory while every other shard
// races to create it. This turns a single directory inode into a hot
// allocation/deallocation cycle in the XFS journal.
//
// Related to: https://github.com/scylladb/scylladb/issues/28259
// ---------------------------------------------------------------------------
SEASTAR_TEST_CASE(test_concurrent_mkdir_delete_thrashing) {
    auto target_dir = std::string{"testlog/race_target_dir"};

    co_await ignore_errors(remove_file(target_dir));

    std::atomic<int> eperm_count{0};

    co_await smp::invoke_on_all([target_dir, &eperm_count] () -> future<> {
        auto shard = this_shard_id();
        co_await max_concurrent_for_each(boost::irange(0, ITERATIONS_COUNT), 16, [shard, target_dir, &eperm_count](int iter) -> future<> {
            if (shard == 0) {
                co_await ignore_errors(remove_file(target_dir));
            } else {
                try {
                    co_await touch_directory(target_dir);
                } catch (const std::system_error& err) {
                    check_eperm(err, eperm_count, shard, iter, "mkdir");
                }
            }
        });
    });

    co_await ignore_errors(remove_file(target_dir));

    if (eperm_count.load() > 0) {
        mkdirlog.error("REPRODUCED: EPERM occurred {} times across {} shards!",
                      eperm_count.load(), smp::count);
    } else {
        mkdirlog.info("Thrashing test completed with {} shards, no EPERM detected", smp::count);
    }
}

// ---------------------------------------------------------------------------
// Test 3 — "Two-step" metadata race (mkdir + file create + nested mkdir)
//
// This test targets the exact race condition that can produce spurious EPERM
// on XFS (and OverlayFS-on-XFS):
//
//   Shard A: mkdir("dir")                          — allocates parent inode
//   Shard B: open("dir/file", O_CREAT | O_EXCL)   — creates file inside it
//   Shard C: mkdir("dir/sub")                      — creates subdirectory inside it
//   Shard D: rmdir("dir/sub") + unlink("dir/file") — tears it all down
//
// All four operation types run concurrently from every shard in a tight loop,
// each shard picking a random role per iteration. This maximizes contention on
// the same XFS allocation group and inode, since all operations target the same
// small directory tree.
//
// The create/delete cycle keeps the XFS journal hot and forces constant inode
// allocation/deallocation in the same AG — the condition under which the
// spurious EPERM has been observed in production.
//
// Related to: https://github.com/scylladb/scylladb/issues/28259
// ---------------------------------------------------------------------------
SEASTAR_TEST_CASE(test_metadata_race_two_step) {
    auto base = std::string{"testlog/two_step_race"};

    co_await force_remove_tree(base);
    co_await recursive_touch_directory(base);

    std::atomic<int> eperm_count{0};

    co_await smp::invoke_on_all([base, &eperm_count] () -> future<> {
        auto shard = this_shard_id();
        co_await max_concurrent_for_each(boost::irange(0, ITERATIONS_COUNT), 16,
                [shard, base, &eperm_count](int iter) -> future<> {
            // Each shard picks a role based on (shard + iteration) to ensure
            // all roles are exercised and collisions are maximized.
            auto role = (shard + iter) % 4;
            auto dir  = base + "/d";
            auto sub  = dir  + "/sub";
            auto file = dir  + fmt::format("/f_{}", shard);

            switch (role) {
            case 0: {
                // Create parent directory
                try {
                    co_await touch_directory(dir);
                } catch (const std::system_error& err) {
                    check_eperm(err, eperm_count, shard, iter, "mkdir(dir)");
                }
                break;
            }
            case 1: {
                // Create a file inside the directory (O_CREAT)
                try {
                    auto flags = open_flags::create | open_flags::wo;
                    auto fh = co_await open_file_dma(file, flags);
                    co_await fh.close();
                } catch (const std::system_error& err) {
                    check_eperm(err, eperm_count, shard, iter, "open(O_CREAT)");
                }
                break;
            }
            case 2: {
                // Create a nested subdirectory
                try {
                    co_await touch_directory(sub);
                } catch (const std::system_error& err) {
                    check_eperm(err, eperm_count, shard, iter, "mkdir(sub)");
                }
                break;
            }
            case 3: {
                // Tear down: unlink file, rmdir sub, rmdir dir (ignore errors)
                co_await ignore_errors(remove_file(file));
                co_await ignore_errors(remove_file(sub));
                co_await ignore_errors(remove_file(dir));
                break;
            }
            default:
                break;
            }
        });
    });

    co_await force_remove_tree(base);

    auto eperm_total = eperm_count.load();
    if (eperm_total > 0) {
        mkdirlog.error("REPRODUCED: EPERM occurred {} times across {} shards!", eperm_total, smp::count);
    } else {
        mkdirlog.info("Two-step race test completed with {} shards, no EPERM detected", smp::count);
    }
}

// ---------------------------------------------------------------------------
// Test 4 — Deep-tree concurrent create/destroy
//
// Multiple shards concurrently create and destroy deeply nested directory
// trees rooted in the same base directory. This stresses XFS inode allocation
// across multiple levels and forces the journal to handle many concurrent
// metadata transactions for the same allocation group.
//
// Unlike the previous tests which focus on a single directory, this exercises
// concurrent recursive_touch_directory (which calls mkdir for each path
// component) racing against rm -rf style recursive deletion.
//
// Related to: https://github.com/scylladb/scylladb/issues/28259
// ---------------------------------------------------------------------------
SEASTAR_TEST_CASE(test_deep_tree_concurrent_create_destroy) {
    auto base = std::string{"testlog/deep_tree_race"};

    co_await force_remove_tree(base);
    co_await recursive_touch_directory(base);

    std::atomic<int> eperm_count{0};

    // Use fewer iterations since each one does more work (deep tree ops).
    constexpr int deep_iterations = ITERATIONS_COUNT / 10;

    co_await smp::invoke_on_all([base, &eperm_count] () -> future<> {
        auto shard = this_shard_id();
        co_await max_concurrent_for_each(boost::irange(0, deep_iterations), 16,
                [shard, base, &eperm_count](int iter) -> future<> {
            // All shards target the SAME few deep paths to maximize contention.
            constexpr int num_slots = 8;
            auto slot = iter % num_slots;
            auto deep = fmt::format("{}/slot_{}/a/b/c/d", base, slot);

            if (shard % 3 == 0) {
                // Destroyer shard: tear down the entire slot subtree
                auto slot_dir = fmt::format("{}/slot_{}", base, slot);
                co_await force_remove_tree(slot_dir);
            } else {
                // Creator shards: build the deep tree and create files at leaves
                try {
                    co_await recursive_touch_directory(deep);
                } catch (const std::system_error& err) {
                    check_eperm(err, eperm_count, shard, iter, "recursive_touch_directory");
                }

                // Also create a file at the leaf to exercise inode alloc
                try {
                    auto file = fmt::format("{}/leaf_{}", deep, shard);
                    auto fh = co_await open_file_dma(file, open_flags::create | open_flags::wo);
                    co_await fh.close();
                } catch (const std::system_error& err) {
                    check_eperm(err, eperm_count, shard, iter, "open(leaf)");
                }
            }
        });
    });

    co_await force_remove_tree(base);

    auto eperm_total = eperm_count.load();
    if (eperm_total > 0) {
        mkdirlog.error("REPRODUCED: EPERM occurred {} times across {} shards!", eperm_total, smp::count);
    } else {
        mkdirlog.info("Deep-tree race test completed with {} shards, no EPERM detected", smp::count);
    }
}

BOOST_AUTO_TEST_SUITE_END()