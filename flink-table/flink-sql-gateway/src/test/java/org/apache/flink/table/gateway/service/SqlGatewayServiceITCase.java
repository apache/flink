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

package org.apache.flink.table.gateway.service;

import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.util.AbstractTestBase;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

/** ITCase for {@link SqlGatewayServiceImpl}. */
public class SqlGatewayServiceITCase extends AbstractTestBase {

    @RegisterExtension
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension();

    private static SqlGatewayServiceImpl service;

    @BeforeAll
    public static void setUp() {
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @Test
    public void testOpenSessionWithConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "val1");
        options.put("key2", "val2");
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .addSessionConfig(options)
                        .build();

        SessionHandle sessionHandle = service.openSession(environment);
        Map<String, String> actualConfig = service.getSessionConfig(sessionHandle);

        options.forEach(
                (k, v) ->
                        assertThat(
                                String.format(
                                        "Should contains (%s, %s) in the actual config.", k, v),
                                actualConfig,
                                Matchers.hasEntry(k, v)));
    }
}
