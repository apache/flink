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
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayService;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.createSqlGatewayEndpoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test {@link SqlGatewayEndpointFactoryUtils}. */
public class SqlGatewayEndpointFactoryUtilsTest {

    @Test
    public void testCreateEndpoint() {
        Configuration config = Configuration.fromMap(getDefaultConfig());
        SqlGatewayEndpoint actual =
                createSqlGatewayEndpoint("mocked", new MockedSqlGatewayService(), config);
        MockedSqlGatewayEndpoint expected = new MockedSqlGatewayEndpoint("localhost", 9999, null);
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateUnknownEndpoint() {
        Map<String, String> config = getDefaultConfig();

        validateException(
                "unknown",
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
                        + "port");
    }

    // --------------------------------------------------------------------------------------------

    private void validateException(Map<String, String> config, String errorMessage) {
        validateException("mocked", config, errorMessage);
    }

    private void validateException(
            String identifier, Map<String, String> config, String errorMessage) {
        assertThatThrownBy(
                        () ->
                                createSqlGatewayEndpoint(
                                        identifier,
                                        new MockedSqlGatewayService(),
                                        Configuration.fromMap(config)))
                .satisfies(anyCauseMatches(ValidationException.class, errorMessage));
    }

    private Map<String, String> getDefaultConfig() {
        Map<String, String> configs = new HashMap<>();
        configs.put("sql-gateway.endpoint.mocked.host", "localhost");
        configs.put("sql-gateway.endpoint.mocked.port", "9999");
        return configs;
    }
}
