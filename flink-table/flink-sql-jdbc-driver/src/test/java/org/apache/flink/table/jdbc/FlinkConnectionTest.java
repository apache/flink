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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkConnection}. */
public class FlinkConnectionTest extends FlinkJdbcDriverTestBase {

    @Test
    void testCatalogSchema() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            assertThat(connection.getCatalog()).isEqualTo("default_catalog");
            assertThat(connection.getSchema()).isEqualTo("default_database");

            assertThatThrownBy(() -> connection.setCatalog("invalid_catalog"))
                    .as("Set catalog[invalid_catalog] fail")
                    .isExactlyInstanceOf(SQLException.class);
            assertThatThrownBy(() -> connection.setSchema("invalid_database"))
                    .as("Set schema[invalid_database] fail")
                    .isExactlyInstanceOf(SQLException.class);
            assertThat(connection.getCatalog()).isEqualTo("default_catalog");
            assertThat(connection.getSchema()).isEqualTo("default_database");

            // Create new catalog and database
            Executor executor = connection.getExecutor();
            StatementResult result =
                    executor.executeStatement(
                            "CREATE CATALOG test_catalog WITH ('type'='generic_in_memory');");
            assertThat(result.hasNext()).isTrue();
            assertThat(result.next().getString(0).toString()).isEqualTo("OK");
            connection.setCatalog("test_catalog");

            result = executor.executeStatement("CREATE DATABASE test_database;");
            assertThat(result.hasNext()).isTrue();
            assertThat(result.next().getString(0).toString()).isEqualTo("OK");
            connection.setSchema("test_database");

            assertThat(connection.getCatalog()).isEqualTo("test_catalog");
            assertThat(connection.getSchema()).isEqualTo("test_database");
        }
    }

    @Test
    void testSetCatalogWithQuotedName() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            Executor executor = connection.getExecutor();
            StatementResult result =
                    executor.executeStatement(
                            "CREATE CATALOG `my catalog` WITH ('type'='generic_in_memory');");
            assertThat(result.hasNext()).isTrue();
            assertThat(result.next().getString(0).toString()).isEqualTo("OK");

            connection.setCatalog("my catalog");
            assertThat(connection.getCatalog()).isEqualTo("my catalog");
        }
    }

    @Test
    void testSetClientInfoValueWithQuote() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            connection.setClientInfo("pipeline.name", "it's a job");
            assertThat(connection.getClientInfo("pipeline.name")).isEqualTo("it's a job");
        }
    }

    @Test
    void testSetClientInfoNullValue() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            assertThatCode(() -> connection.setClientInfo("pipeline.name", null))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testSetClientInfoNullName() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            assertThatCode(() -> connection.setClientInfo(null, "v")).doesNotThrowAnyException();
        }
    }

    @Test
    void testSetSchemaQualifiedName() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            connection.setSchema("default_catalog.default_database");
            assertThat(connection.getCatalog()).isEqualTo("default_catalog");
            assertThat(connection.getSchema()).isEqualTo("default_database");
        }
    }

    @Test
    void testClientInfo() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("key3", "val3");
        DriverUri driverUri = getDriverUri("jdbc:flink://%s:%s?key1=val1&key2=val2", properties);
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            assertThat(connection.getClientInfo("key1")).isEqualTo("val1");
            assertThat(connection.getClientInfo("key2")).isEqualTo("val2");
            assertThat(connection.getClientInfo("key3")).isEqualTo("val3");

            connection.setClientInfo("key1", "val11");
            Properties resetProp = new Properties();
            resetProp.setProperty("key2", "val22");
            resetProp.setProperty("key3", "val33");
            resetProp.setProperty("key4", "val44");
            connection.setClientInfo(resetProp);
            assertThat(connection.getClientInfo("key1")).isEqualTo("val11");
            assertThat(connection.getClientInfo("key4")).isEqualTo("val44");
            Properties clientInfo = connection.getClientInfo();
            assertThat(clientInfo.getProperty("key1")).isEqualTo("val11");
            assertThat(clientInfo.getProperty("key2")).isEqualTo("val22");
            assertThat(clientInfo.getProperty("key3")).isEqualTo("val33");
            assertThat(clientInfo.getProperty("key4")).isEqualTo("val44");
        }
    }
}
