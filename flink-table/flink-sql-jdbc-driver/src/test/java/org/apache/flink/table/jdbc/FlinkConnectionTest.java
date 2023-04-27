/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.jdbc;

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkConnection}. */
public class FlinkConnectionTest extends FlinkJdbcDriverTestBase {

    @Test
    public void testCatalogSchema() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            assertEquals("default_catalog", connection.getCatalog());
            assertEquals("default_database", connection.getSchema());

            assertThrowsExactly(
                    SQLException.class,
                    () -> connection.setCatalog("invalid_catalog"),
                    "Set catalog[invalid_catalog] fail");
            assertThrowsExactly(
                    SQLException.class,
                    () -> connection.setSchema("invalid_database"),
                    "Set schema[invalid_database] fail");
            assertEquals("default_catalog", connection.getCatalog());
            assertEquals("default_database", connection.getSchema());

            // Create new catalog and database
            Executor executor = connection.getExecutor();
            StatementResult result =
                    executor.executeStatement(
                            "CREATE CATALOG test_catalog WITH ('type'='generic_in_memory');");
            assertTrue(result.hasNext());
            assertEquals("OK", result.next().getString(0).toString());
            connection.setCatalog("test_catalog");

            result = executor.executeStatement("CREATE DATABASE test_database;");
            assertTrue(result.hasNext());
            assertEquals("OK", result.next().getString(0).toString());
            connection.setSchema("test_database");

            assertEquals("test_catalog", connection.getCatalog());
            assertEquals("test_database", connection.getSchema());
        }
    }

    @Test
    public void testClientInfo() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("key3", "val3");
        DriverUri driverUri = getDriverUri("jdbc:flink://%s:%s?key1=val1&key2=val2", properties);
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            assertEquals("val1", connection.getClientInfo("key1"));
            assertEquals("val2", connection.getClientInfo("key2"));
            assertEquals("val3", connection.getClientInfo("key3"));

            connection.setClientInfo("key1", "val11");
            Properties resetProp = new Properties();
            resetProp.setProperty("key2", "val22");
            resetProp.setProperty("key3", "val33");
            resetProp.setProperty("key4", "val44");
            connection.setClientInfo(resetProp);
            assertEquals("val11", connection.getClientInfo("key1"));
            assertEquals("val44", connection.getClientInfo("key4"));
            Properties clientInfo = connection.getClientInfo();
            assertEquals("val11", clientInfo.getProperty("key1"));
            assertEquals("val22", clientInfo.getProperty("key2"));
            assertEquals("val33", clientInfo.getProperty("key3"));
            assertEquals("val44", clientInfo.getProperty("key4"));
        }
    }
}
