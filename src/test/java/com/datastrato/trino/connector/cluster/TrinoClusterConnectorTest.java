/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import io.trino.Session;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TrinoClusterConnectorTest extends BaseConnectorTest {

    private TestingTrinoServer testingTrinoServer;
    private Session session = createSession("trino_memory", "test");

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        testingTrinoServer = closeAfterClass(new TestingTrinoServer());
        DistributedQueryRunner queryRunner = TrinoClusterQueryRunner.createQueryRunner(testingTrinoServer, REQUIRED_TPCH_TABLES);

        //create schema and use it by default
        testingTrinoServer.execute("CREATE SCHEMA memory.test");
        testingTrinoServer.execute("USE memory.test");
        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                    SUPPORTS_ADD_FIELD,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_DELETE,
                    SUPPORTS_INSERT,
                    SUPPORTS_MERGE,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_RENAME_FIELD,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TRUNCATE,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Ignore
    @Override
    public void testCreateSchema() {}

    @Ignore
    @Override
    public void testCreateTable() {}

    @Ignore
    @Override
    public void testDropColumn() {}

    @Ignore
    @Override
    public void testAddColumn() {}

    @Ignore
    @Override
    public void testRenameColumn() {}

    @Ignore
    @Override
    public void testCreateTableAsSelect() {}

    @Ignore
    @Override
    public void testTruncateTable() {}

    @Ignore
    @Override
    public void testInsert() {}

    @Ignore
    @Override
    public void testCommentTable() {}

    @Ignore
    @Override
    public void testInsertNegativeDate() {}

    @Test
    public void testShowTablesWithTPCDS() {
        assertQuerySucceeds(createSession("sf1"), "SHOW TABLES");
        assertQuerySucceeds(createSession("sf10"), "SHOW TABLES");
        assertQuerySucceeds(createSession("tiny"), "SHOW TABLES FROM sf1");
        assertQuerySucceeds(createSession("tiny"), "SHOW TABLES FROM \"sf10\"");
        assertQueryFails(
                createSession("tiny"),
                "SHOW TABLES FROM sf0",
                "line 1:1: Schema 'sf0' does not exist");
    }

    @Test
    public void testSelectWithTPCDS() {
        assertQuery(createSession("tiny"), "SELECT count(*) FROM sf1.customer", "SELECT 100000");
        // Test select different type of fields and verify result
        assertQuery(
                createSession("tiny"),
                "SELECT c_customer_sk, c_customer_id , c_current_cdemo_sk, c_salutation,  c_birth_country, c_birth_day"
                        + " FROM sf1.customer ORDER BY c_customer_sk limit 1",
                "SELECT 1, 'AAAAAAAABAAAAAAA', 980124, 'Mr.       ', 'CHILE', 9");
        assertQuery(
                createSession("tiny"),
                "SELECT ss_wholesale_cost FROM tiny.store_sales WHERE ss_sold_date_sk = 2451438 AND ss_item_sk = 344",
                "SELECT 54.34");

        assertQuerySucceeds(
                createSession("tiny"),
                "SELECT * FROM call_center ORDER BY cc_call_center_sk, cc_call_center_id, cc_rec_start_date LIMIT 10");
    }

    @Test
    public void testJoinWithTPCDS() {
        MaterializedResult actual =
                computeActual(
                        createSession("sf1"),
                        "SELECT c_first_name, c_last_name, ca_address_sk, ca_gmt_offset "
                                + "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk "
                                + "WHERE ca_address_sk = 4");
        MaterializedResult expected =
                resultBuilder(createSession("sf1"), actual.getTypes())
                        // note that c_first_name and c_last_name are both of type CHAR(X) so the
                        // results
                        // are padded with whitespace
                        .row(
                                "James               ",
                                "Brown                         ",
                                4L,
                                new BigDecimal("-7.00"))
                        .build();
        assertThat(actual).containsExactlyElementsOf(expected);

        actual =
                computeActual(
                        createSession("sf1"),
                        "SELECT c_first_name, c_last_name "
                                + "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk "
                                + "WHERE ca_address_sk = 4 AND ca_gmt_offset = DECIMAL '-7.00'");
        expected =
                resultBuilder(getSession(), actual.getTypes())
                        .row("James               ", "Brown                         ")
                        .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private Session createSession(String schemaName) {
        return createSession("trino_tpcds", schemaName);
    }

    private Session createSession(String catalogName, String schemaName) {
        return testSessionBuilder()
                .setSource("test")
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .build();
    }

    @Test
    public void testDataType() {
        testingTrinoServer.execute("CREATE TABLE test01(col1 BOOLEAN, col2 TINYINT, col3 SMALLINT, col4 INTEGER, col5 BIGINT)");
        testingTrinoServer.execute("INSERT INTO test01 VALUES(true, 1, 1, 1, 1)");
        assertQuery(session, "SELECT * FROM test01", "VALUES (true, 1, 1, 1, 1)");

        testingTrinoServer.execute("CREATE TABLE test02(col1 REAL, col2 DOUBLE, col3 DECIMAL(10, 5))");
        testingTrinoServer.execute("INSERT INTO test02 VALUES(1.0, 1.0, 1.0)");
        assertQuery(session, "SELECT * FROM test02", "VALUES (1.0, 1.0, 1.0)");

        testingTrinoServer.execute("CREATE TABLE test03(col1 VARCHAR, col2 VARCHAR(200), col3 CHAR, col4 CHAR(10), col5 VARBINARY)");
        testingTrinoServer.execute("INSERT INTO test03 VALUES('test', 'test', 't', 'test', X'01')");
        assertQuery(session, "SELECT * FROM test03", "VALUES ('test', 'test', 't', 'test      ', X'01')");

        testingTrinoServer.execute("CREATE TABLE test04(col1 DATE, col2 TIME, col3 TIMESTAMP)");
        testingTrinoServer.execute("INSERT INTO test04 VALUES(DATE '2021-01-01', TIME '00:00:00', TIMESTAMP '2021-01-01 00:00:00')");
        assertQuery(session, "SELECT * FROM test04", "VALUES (DATE '2021-01-01', TIME '00:00:00', TIMESTAMP '2021-01-01 00:00:00')");
    }

    @Test
    public void testTimestampWithTimeZone() {
        testingTrinoServer.execute("CREATE TABLE test05(col1 TIMESTAMP WITH TIME ZONE)");
        testingTrinoServer.execute("INSERT INTO test05 VALUES(TIMESTAMP '2021-01-01 00:00:00 +08:00')");
        MaterializedResult actual = computeActual(session, "SELECT * FROM test05");
        MaterializedResult except = resultBuilder(getSession(), actual.getTypes())
                .row(ZonedDateTime.parse("2020-12-31T16:00Z[UTC]"))
                .build();
        assertThat(actual).containsExactlyElementsOf(except);
    }
}
