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

package org.apache.flink.table.gateway.rest.compatibility;

import org.apache.flink.runtime.rest.compatibility.RestAPIStabilityTestUtils;
import org.apache.flink.table.gateway.rest.util.DocumentingSqlGatewayRestEndpoint;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.util.ConfigurationException;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/** Stability test and snapshot generator for the SqlGateway REST API. */
final class SqlGatewayRestAPIStabilityTest {

    private static final String REGENERATE_SNAPSHOT_PROPERTY = "generate-sql-gateway-rest-snapshot";

    private static final String SNAPSHOT_RESOURCE_PATTERN = "sql_gateway_rest_api_%s.snapshot";

    private static class StableSqlGatewayRestApiVersionProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Arrays.stream(SqlGatewayRestAPIVersion.values())
                    .filter(SqlGatewayRestAPIVersion::isStableVersion)
                    .map(Arguments::of);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(StableSqlGatewayRestApiVersionProvider.class)
    void testSqlGatewayRestAPIStability(SqlGatewayRestAPIVersion apiVersion)
            throws IOException, ConfigurationException {
        RestAPIStabilityTestUtils.testStability(
                SNAPSHOT_RESOURCE_PATTERN,
                REGENERATE_SNAPSHOT_PROPERTY,
                apiVersion,
                new DocumentingSqlGatewayRestEndpoint());
    }
}
