/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.gateway.api.endpoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.gateway.api.utils.FakeSqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayService;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.createSqlGatewayEndpoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test {@link SqlGatewayEndpointFactoryUtils}. */
public class SqlGatewayEndpointFactoryUtilsTest {

    @Test
    public void testCreateEndpoints() {
        String id = UUID.randomUUID().toString();
        Map<String, String> config = getDefaultConfig(id);
        config.put("sql-gateway.endpoint.type", "mocked;fake");
        List<SqlGatewayEndpoint> actual =
                createSqlGatewayEndpoint(
                        new MockedSqlGatewayService(), Configuration.fromMap(config));
        MockedSqlGatewayEndpoint expectedMocked =
                new MockedSqlGatewayEndpoint(id, "localhost", 9999, "Hello World.");
        assertThat(actual)
                .isEqualTo(Arrays.asList(expectedMocked, FakeSqlGatewayEndpoint.INSTANCE));
    }

    @Test
    public void testCreateEndpointWithDuplicateIdentifier() {
        Map<String, String> config = getDefaultConfig();
        config.put("sql-gateway.endpoint.type", "mocked;mocked");
        validateException(
                config,
                "Get the duplicate endpoint identifier 'mocked' for the option 'sql-gateway.endpoint.type'. "
                        + "Please keep the specified endpoint identifier unique.");
    }

    @Test
    public void testCreateEndpointWithoutType() {
        Map<String, String> config = getDefaultConfig();
        config.remove("sql-gateway.endpoint.type");
        validateException(
                config,
                "Could not find any factory for identifier 'rest' that implements 'org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactory' in the classpath.");
    }

    @Test
    public void testCreateUnknownEndpoint() {
        Map<String, String> config = getDefaultConfig();
        config.put("sql-gateway.endpoint.type", "mocked;unknown");
        validateException(
                config,
                String.format(
                        "Could not find any factory for identifier 'unknown' "
                                + "that implements '%s' in the classpath.",
                        SqlGatewayEndpointFactory.class.getCanonicalName()));
    }

    @Test
    public void testCreateEndpointWithMissingOptions() {
        Map<String, String> config = getDefaultConfig();
        config.remove("sql-gateway.endpoint.mocked.host");

        validateException(
                config,
                "One or more required options are missing.\n\n"
                        + "Missing required options are:\n\n"
                        + "host");
    }

    @Test
    public void testCreateEndpointWithUnconsumedOptions() {
        Map<String, String> config = getDefaultConfig();
        config.put("sql-gateway.endpoint.mocked.unconsumed-option", "error");

        validateException(
                config,
                "Unsupported options found for 'mocked'.\n\n"
                        + "Unsupported options:\n\n"
                        + "unconsumed-option\n\n"
                        + "Supported options:\n\n"
                        + "description\n"
                        + "host\n"
                        + "id\n"
                        + "port");
    }

    // --------------------------------------------------------------------------------------------

    private void validateException(Map<String, String> config, String errorMessage) {
        assertThatThrownBy(
                        () ->
                                createSqlGatewayEndpoint(
                                        new MockedSqlGatewayService(),
                                        Configuration.fromMap(config)))
                .satisfies(anyCauseMatches(ValidationException.class, errorMessage));
    }

    private Map<String, String> getDefaultConfig() {
        return getDefaultConfig(UUID.randomUUID().toString());
    }

    private Map<String, String> getDefaultConfig(String id) {
        Map<String, String> config = new HashMap<>();
        config.put("sql-gateway.endpoint.mocked.id", id);
        config.put("sql-gateway.endpoint.type", "mocked");
        config.put("sql-gateway.endpoint.mocked.host", "localhost");
        config.put("sql-gateway.endpoint.mocked.port", "9999");
        return config;
    }
}
