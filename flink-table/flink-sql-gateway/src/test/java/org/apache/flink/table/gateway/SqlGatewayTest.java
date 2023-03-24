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

package org.apache.flink.table.gateway;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayEndpoint;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for the {@link SqlGateway}. */
class SqlGatewayTest {

    private Map<String, String> originalEnv;
    private ByteArrayOutputStream output;

    @BeforeEach
    void before(@TempDir File tempFolder) throws IOException {
        originalEnv = System.getenv();

        // prepare yaml
        File confYaml = new File(tempFolder, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }
        // adjust the test environment for the purposes of this test
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, tempFolder.getAbsolutePath());
        CommonTestUtils.setEnv(map);

        output = new ByteArrayOutputStream(256);
    }

    @AfterEach
    void cleanup() throws Exception {
        CommonTestUtils.setEnv(originalEnv);
        if (output != null) {
            output.close();
        }
    }

    @Test
    void testPrintStartGatewayHelp() {
        String[] args = new String[] {"--help"};
        SqlGateway.startSqlGateway(new PrintStream(output), args);

        assertThat(output.toString())
                .contains(
                        "Start the Flink SQL Gateway as a daemon to submit Flink SQL.\n"
                                + "\n"
                                + "  Syntax: start [OPTIONS]\n"
                                + "     -D <property=value>   Use value for given property\n"
                                + "     -h,--help             Show the help message with descriptions of all\n"
                                + "                           options.\n\n");
    }

    @Test
    void testConfigureSqlGateway() throws Exception {
        String id = UUID.randomUUID().toString();
        String[] args =
                new String[] {
                    "-Dsql-gateway.endpoint.type=mocked",
                    "-Dsql-gateway.endpoint.mocked.id=" + id,
                    "-Dsql-gateway.endpoint.mocked.host=localhost",
                    "-Dsql-gateway.endpoint.mocked.port=9999"
                };
        try (PrintStream stream = new PrintStream(output)) {
            Thread thread =
                    new ExecutorThreadFactory(
                                    "SqlGateway-thread-pool",
                                    (t, exception) -> exception.printStackTrace(stream))
                            .newThread(() -> SqlGateway.startSqlGateway(stream, args));
            thread.start();

            CommonTestUtils.waitUtil(
                    () -> MockedSqlGatewayEndpoint.isRunning(id),
                    Duration.ofSeconds(10),
                    "Failed to get the endpoint starts.");

            thread.interrupt();
            CommonTestUtils.waitUtil(
                    () -> !thread.isAlive(),
                    Duration.ofSeconds(10),
                    "Failed to get the endpoint starts.");
            assertThat(output.toString())
                    .doesNotContain(
                            "Unexpected exception. This is a bug. Please consider filing an issue.");
        }
    }

    @Test
    void testFailedToStartSqlGateway() {
        try (PrintStream stream = new PrintStream(output)) {
            assertThatThrownBy(() -> SqlGateway.startSqlGateway(stream, new String[0]))
                    .doesNotHaveToString(
                            "Unexpected exception. This is a bug. Please consider filing an issue.");
        }
    }
}
