/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** CONNECTION parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserConnectionTest extends FlinkSqlParserTestBase {

    @Test
    void testCreateConnection() {
        sql("create connection conn1\n"
                        + " COMMENT 'connection_comment'\n"
                        + " WITH (\n"
                        + "  'type'='basic',\n"
                        + "  'url'='http://example.com',\n"
                        + "  'username'='user1',\n"
                        + "  'password'='pass1'\n"
                        + " )\n")
                .ok(
                        "CREATE CONNECTION `CONN1`\n"
                                + "COMMENT 'connection_comment'\n"
                                + "WITH (\n"
                                + "  'type' = 'basic',\n"
                                + "  'url' = 'http://example.com',\n"
                                + "  'username' = 'user1',\n"
                                + "  'password' = 'pass1'\n"
                                + ")");
    }

    @Test
    void testCreateConnectionIfNotExists() {
        sql("create connection if not exists conn1\n"
                        + " WITH (\n"
                        + "  'type'='bearer',\n"
                        + "  'token'='my_token'\n"
                        + " )\n")
                .ok(
                        "CREATE CONNECTION IF NOT EXISTS `CONN1`\n"
                                + "WITH (\n"
                                + "  'type' = 'bearer',\n"
                                + "  'token' = 'my_token'\n"
                                + ")");
    }

    @Test
    void testCreateTemporaryConnection() {
        sql("create temporary connection conn1\n"
                        + " WITH (\n"
                        + "  'type'='oauth',\n"
                        + "  'client_id'='client1'\n"
                        + " )\n")
                .ok(
                        "CREATE TEMPORARY CONNECTION `CONN1`\n"
                                + "WITH (\n"
                                + "  'type' = 'oauth',\n"
                                + "  'client_id' = 'client1'\n"
                                + ")");
    }

    @Test
    void testCreateSystemConnection() {
        sql("create ^system^ connection conn1\n"
                        + " WITH (\n"
                        + "  'type'='basic',\n"
                        + "  'url'='http://example.com'\n"
                        + " )\n")
                .fails(
                        "(?s)CREATE SYSTEM CONNECTION is not supported, "
                                + "system connections can only be registered as temporary "
                                + "connections, you can use CREATE TEMPORARY SYSTEM CONNECTION "
                                + "instead\\..*");
    }

    @Test
    void testCreateTemporarySystemConnection() {
        sql("create temporary system connection conn1\n"
                        + " WITH (\n"
                        + "  'type'='custom_type',\n"
                        + "  'api_key'='key123'\n"
                        + " )\n")
                .ok(
                        "CREATE TEMPORARY SYSTEM CONNECTION `CONN1`\n"
                                + "WITH (\n"
                                + "  'type' = 'custom_type',\n"
                                + "  'api_key' = 'key123'\n"
                                + ")");
    }

    @Test
    void testCreateConnectionWithQualifiedName() {
        sql("create connection catalog1.db1.conn1\n"
                        + " WITH ('type'='basic', 'url'='http://example.com')\n")
                .ok(
                        "CREATE CONNECTION `CATALOG1`.`DB1`.`CONN1`\n"
                                + "WITH (\n"
                                + "  'type' = 'basic',\n"
                                + "  'url' = 'http://example.com'\n"
                                + ")");
    }

    @Test
    void testDropConnection() {
        sql("drop connection conn1").ok("DROP CONNECTION `CONN1`");
        sql("drop connection db1.conn1").ok("DROP CONNECTION `DB1`.`CONN1`");
        sql("drop connection catalog1.db1.conn1").ok("DROP CONNECTION `CATALOG1`.`DB1`.`CONN1`");
    }

    @Test
    void testDropConnectionIfExists() {
        sql("drop connection if exists catalog1.db1.conn1")
                .ok("DROP CONNECTION IF EXISTS `CATALOG1`.`DB1`.`CONN1`");
    }

    @Test
    void testDropTemporaryConnection() {
        sql("drop temporary connection conn1").ok("DROP TEMPORARY CONNECTION `CONN1`");
        sql("drop temporary connection if exists conn1")
                .ok("DROP TEMPORARY CONNECTION IF EXISTS `CONN1`");
    }

    @Test
    void testDropTemporarySystemConnection() {
        sql("drop temporary system connection conn1")
                .ok("DROP TEMPORARY SYSTEM CONNECTION `CONN1`");
        sql("drop temporary system connection if exists conn1")
                .ok("DROP TEMPORARY SYSTEM CONNECTION IF EXISTS `CONN1`");
    }

    @Test
    void testDropSystemConnection() {
        sql("drop ^system^ connection conn1")
                .fails(
                        "(?s)DROP SYSTEM CONNECTION is not supported, "
                                + "system connections can only be dropped as temporary "
                                + "connections, you can use DROP TEMPORARY SYSTEM CONNECTION "
                                + "instead\\..*");
    }

    @Test
    void testAlterConnectionSet() {
        final String sql =
                "alter connection conn1 set ('password' = 'new_password','url' = 'http://new.com')";
        final String expected =
                "ALTER CONNECTION `CONN1` SET (\n"
                        + "  'password' = 'new_password',\n"
                        + "  'url' = 'http://new.com'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionSetWithQualifiedName() {
        final String sql = "alter connection catalog1.db1.conn1 set ('token' = 'new_token')";
        final String expected =
                "ALTER CONNECTION `CATALOG1`.`DB1`.`CONN1` SET (\n"
                        + "  'token' = 'new_token'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionRename() {
        final String sql = "alter connection conn1 rename to conn2";
        final String expected = "ALTER CONNECTION `CONN1` RENAME TO `CONN2`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionRenameWithQualifiedName() {
        final String sql = "alter connection catalog1.db1.conn1 rename to conn2";
        final String expected = "ALTER CONNECTION `CATALOG1`.`DB1`.`CONN1` RENAME TO `CONN2`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionReset() {
        final String sql = "alter connection conn1 reset ('password', 'url')";
        final String expected = "ALTER CONNECTION `CONN1` RESET (\n  'password',\n  'url'\n)";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionResetWithQualifiedName() {
        final String sql = "alter connection catalog1.db1.conn1 reset ('token')";
        final String expected = "ALTER CONNECTION `CATALOG1`.`DB1`.`CONN1` RESET (\n  'token'\n)";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionIfExists() {
        final String sql =
                "alter connection if exists conn1 set ('password' = 'new_password','url' = 'http://new.com')";
        final String expected =
                "ALTER CONNECTION IF EXISTS `CONN1` SET (\n"
                        + "  'password' = 'new_password',\n"
                        + "  'url' = 'http://new.com'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionRenameIfExists() {
        final String sql = "alter connection if exists conn1 rename to conn2";
        final String expected = "ALTER CONNECTION IF EXISTS `CONN1` RENAME TO `CONN2`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterConnectionResetIfExists() {
        final String sql = "alter connection if exists conn1 reset ('password', 'url')";
        final String expected =
                "ALTER CONNECTION IF EXISTS `CONN1` RESET (\n  'password',\n  'url'\n)";
        sql(sql).ok(expected);
    }

    @Test
    void testShowConnections() {
        sql("show connections").ok("SHOW CONNECTIONS");
        sql("show connections from db1").ok("SHOW CONNECTIONS FROM `DB1`");
        sql("show connections from catalog1.db1").ok("SHOW CONNECTIONS FROM `CATALOG1`.`DB1`");
        sql("show connections in db1").ok("SHOW CONNECTIONS IN `DB1`");
        sql("show connections in catalog1.db1").ok("SHOW CONNECTIONS IN `CATALOG1`.`DB1`");
    }

    @Test
    void testShowConnectionsLike() {
        sql("show connections like '%conn%'").ok("SHOW CONNECTIONS LIKE '%CONN%'");
        sql("show connections from db1 like 'my_%'").ok("SHOW CONNECTIONS FROM `DB1` LIKE 'MY_%'");
        sql("show connections not like 'temp_%'").ok("SHOW CONNECTIONS NOT LIKE 'TEMP_%'");
    }

    @Test
    void testShowCreateConnection() {
        sql("show create connection conn1").ok("SHOW CREATE CONNECTION `CONN1`");
        sql("show create connection catalog1.db1.conn1")
                .ok("SHOW CREATE CONNECTION `CATALOG1`.`DB1`.`CONN1`");
    }

    @Test
    void testDescribeConnection() {
        sql("describe connection conn1").ok("DESCRIBE CONNECTION `CONN1`");
        sql("describe connection catalog1.db1.conn1")
                .ok("DESCRIBE CONNECTION `CATALOG1`.`DB1`.`CONN1`");
        sql("describe connection extended conn1").ok("DESCRIBE CONNECTION EXTENDED `CONN1`");

        sql("desc connection conn1").ok("DESCRIBE CONNECTION `CONN1`");
        sql("desc connection catalog1.db1.conn1")
                .ok("DESCRIBE CONNECTION `CATALOG1`.`DB1`.`CONN1`");
        sql("desc connection extended catalog1.db1.conn1")
                .ok("DESCRIBE CONNECTION EXTENDED `CATALOG1`.`DB1`.`CONN1`");
    }
}
