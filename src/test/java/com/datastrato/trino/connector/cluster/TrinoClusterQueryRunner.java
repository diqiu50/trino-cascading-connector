/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TrinoClusterQueryRunner {
    private TrinoClusterQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner(
            TestingTrinoServer testingTrinoServer, Iterable<TpchTable<?>> tables) throws Exception {
        Session session =
                testSessionBuilder()
                        .setSource("test")
                        .setCatalog("trino_memory")
                        .setSchema("tpch")
                        .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        try {
            queryRunner.installPlugin(new TrinoClusterPlugin());

            HashMap<String, String> properties = new HashMap<>();
            properties.put("connection-url", testingTrinoServer.getJdbcUrl() + "/memory");
            properties.put("connection-user", "admin");
            queryRunner.createCatalog("trino_memory", "trino", properties);

            properties = new HashMap<>();
            properties.put("connection-url", testingTrinoServer.getJdbcUrl() + "/tpcds");
            properties.put("connection-user", "admin");
            queryRunner.createCatalog("trino_tpcds", "trino", properties);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", properties);

            testingTrinoServer.execute("create schema memory.tpch");
            testingTrinoServer.execute(
                    "create table memory.tpch.customer as select * from tpch.tiny.customer");
            testingTrinoServer.execute(
                    "create table memory.tpch.lineitem as select * from tpch.tiny.lineitem");
            testingTrinoServer.execute(
                    "create table memory.tpch.nation as select * from tpch.tiny.nation");
            testingTrinoServer.execute(
                    "create table memory.tpch.orders as select * from tpch.tiny.orders");
            testingTrinoServer.execute(
                    "create table memory.tpch.part as select * from tpch.tiny.part");
            testingTrinoServer.execute(
                    "create table memory.tpch.partsupp as select * from tpch.tiny.partsupp");
            testingTrinoServer.execute(
                    "create table memory.tpch.region as select * from tpch.tiny.region");
            testingTrinoServer.execute(
                    "create table memory.tpch.supplier as select * from tpch.tiny.supplier");

            return queryRunner;
        } catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        Logger log = Logger.get(TrinoClusterQueryRunner.class);
        log.info("======== TEST STARTED ========");
        TestingTrinoServer server = new TestingTrinoServer();
        DistributedQueryRunner queryRunner = createQueryRunner(server, TpchTable.getTables());
        Thread.sleep(10);

        log.info("======== SERVER STARTED ========");
        log.info("\n==trino worker url ==\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        log.info("\n==trino server jdbc ==\n%s\n====", server.getJdbcUrl());
    }
}
