#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


async def get_injection_params(manager: ManagerClient, node_ip, injection):
    res = await manager.api.get_injection(node_ip, injection)
    logger.debug(f"get_injection_params({injection}): {res}")
    assert len(res) == 1
    shard_res = res[0]
    assert shard_res["enabled"]
    if "parameters" in shard_res:
        return {item["key"]: item["value"] for item in shard_res["parameters"]}
    else:
        return {}


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_enable_compacting_data_for_streaming_and_repair_live_update(manager: ManagerClient):
    """
    Check that enable_compacting_data_for_streaming_and_repair is live_update.
    This config item has a non-trivial path of propagation and live-update was
    silently broken in the past.
    """
    cmdline = ["--enable-compacting-data-for-streaming-and-repair", "0", "--smp", "1", "--logger-log-level", "api=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY)")

    config_item = "enable_compacting_data_for_streaming_and_repair"

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    for host in (host1, host2):
        res = list(cql.execute(f"SELECT value FROM system.config WHERE name = '{config_item}'", host=host))
        assert res[0].value == "false"

    await manager.api.enable_injection(node1.ip_addr, "maybe_compact_for_streaming", False, {})

    # Before the first repair, there should be no parameters present
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {}

    # After the initial repair, we should see the config item value matching the value set via the command-line.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming"))["compaction_enabled"] == "false"

    for host in (host1, host2):
        cql.execute(f"UPDATE system.config SET value = '1' WHERE name = '{config_item}'", host=host)

    # After the update to the config above, the next repair should pick up the updated value.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming"))["compaction_enabled"] == "true"


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tombstone_gc_for_streaming_and_repair(manager: ManagerClient):
    """
    Check that:
    * enable_tombstone_gc_for_streaming_and_repair=1 works as expected
    * enable_tombstone_gc_for_streaming_and_repair=0 works as expected
    * enable_tombstone_gc_for_streaming_and_repair is live-update
    """
    cmdline = [
            "--enable-compacting-data-for-streaming-and-repair", "1",
            "--enable-tombstone-gc-for-streaming-and-repair", "1",
            "--enable-cache", "0",
            "--hinted-handoff-enabled", "0",
            "--smp", "1",
            "--logger-log-level", "api=trace:database=trace"]
    node1 = await manager.server_add(cmdline=cmdline)
    node2 = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH compaction = {'class': 'NullCompactionStrategy'}")

    await manager.server_stop_gracefully(node2.server_id)

    stmt = SimpleStatement("DELETE FROM ks.tbl WHERE pk = 0 AND ck = 0", consistency_level=ConsistencyLevel.ONE)
    cql.execute(stmt)

    await manager.server_start(node2.server_id, wait_others=1)

    # Flush memtables and remove commitlog, so we can freely GC tombstones.
    await manager.server_restart(node1.server_id, wait_others=1)

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    config_item = "enable_tombstone_gc_for_streaming_and_repair"

    def check_nodes_have_data(node1_has_data, node2_has_data):
        for (host, host_has_data) in ((host1, node1_has_data), (host2, node2_has_data)):
            res = list(cql.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0", host=host))
            print(res)
            if host_has_data:
                assert len(res) == 3
            else:
                assert len(res) < 3

    # Initial start-condition check
    check_nodes_have_data(True, False)

    await manager.api.enable_injection(node1.ip_addr, "maybe_compact_for_streaming", False, {})

    # Make the tombstone purgeable
    cql.execute("ALTER TABLE ks.tbl WITH tombstone_gc = {'mode': 'immediate'}")

    # With enable_tombstone_gc_for_streaming_and_repair=1, repair
    # should not find any differences and thus not replicate the GCable
    # tombstone.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {
            "compaction_enabled": "true", "compaction_can_gc": "true"}
    check_nodes_have_data(True, False)

    for host in (host1, host2):
        cql.execute(f"UPDATE system.config SET value = '0' WHERE name = '{config_item}'", host=host)

    # With enable_tombstone_gc_for_streaming_and_repair=0, repair
    # should find the differences and replicate the GCable tombstone.
    await manager.api.repair(node1.ip_addr, "ks", "tbl")
    assert (await get_injection_params(manager, node1.ip_addr, "maybe_compact_for_streaming")) == {
            "compaction_enabled": "true", "compaction_can_gc": "false"}
    check_nodes_have_data(True, True)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_repair_succeeds_with_unitialized_bm(manager: ManagerClient):
    await manager.server_add()
    await manager.server_add()
    servers = await manager.running_servers()

    cql = manager.get_cql()

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {'mode': 'repair'}")

    await manager.api.enable_injection(servers[1].ip_addr, "repair_flush_hints_batchlog_handler_bm_uninitialized", True, {})
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    await manager.api.repair(servers[0].ip_addr, "ks", "tbl")

    matches = await logs[1].grep("Failed to process repair_flush_hints_batchlog_request", from_mark=marks[1])
    assert len(matches) == 1
    matches = await logs[0].grep("failed, continue to run repair", from_mark=marks[0])
    assert len(matches) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("restart_server",
                         [True, pytest.param(False, marks=pytest.mark.xfail(reason="Expected failure because of commitlog issue"))])
async def test_tombstone_gc_repair(manager: ManagerClient, restart_server):
    cmdline = [
        "--enable-cache", "0",
        "--logger-log-level", "database=trace:commitlog=trace",
    ]

    servers = [await manager.server_add(cmdline=cmdline) for _ in range(2)]
    cql = manager.get_cql()

    host1, *_ = await wait_for_cql_and_get_hosts(cql, servers[:1], time.time() + 30)

    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck))"
                " WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '10'} AND gc_grace_seconds = 10")

    # create some tombstones
    await cql.run_async("INSERT INTO ks.tbl (pk, ck) VALUES (1, 2)")
    await cql.run_async("INSERT INTO ks.tbl (pk, ck) VALUES (3, 4)")
    await cql.run_async("DELETE FROM ks.tbl WHERE pk = 1")

    # check that we have the expected tombstone
    res = list(cql.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 1", host=host1))
    logger.info(res)
    logger.info("Result size: %d", len(res))
    assert len(res) == 2

    test_val = 5

    async def verify_tombstone_gc():
        nonlocal test_val

        # adding data is needed for some reason with the repair mode for the tombstone GC to trigger
        await cql.run_async(f"INSERT INTO ks.tbl (pk, ck) VALUES ({test_val}, {test_val+1})")
        await cql.run_async(f"DELETE FROM ks.tbl WHERE pk = {test_val}")
        test_val += 2

        await manager.api.repair(servers[0].ip_addr, "ks", "tbl")

        # enabling the following lines makes the test pass
        # (note that restarting just the servers[0] is not enough)
        if restart_server:
            await manager.rolling_restart(servers)
            host1, *_ = await wait_for_cql_and_get_hosts(cql, servers[:1], time.time() + 30)

        # await manager.api.client.post("/storage_service/flush", host=servers[0].ip_addr)
        await manager.api.keyspace_flush(servers[0].ip_addr, "ks")
        await manager.api.keyspace_compaction(servers[0].ip_addr, "ks")
        await manager.api.drop_sstable_caches(servers[0].ip_addr)

        # exercise the read path to trigger the tombstone gc
        cql.execute("SELECT * FROM ks.tbl WHERE pk = 1", host=host1)
        # check that we the tombstones have been cleaned up
        res = list(cql.execute("SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 1", host=host1))
        logger.info(res)
        logger.info("Result size: %d", len(res))
        return True if len(res) < 2 else None

    await wait_for(verify_tombstone_gc, time.time() + 150, period=15)
