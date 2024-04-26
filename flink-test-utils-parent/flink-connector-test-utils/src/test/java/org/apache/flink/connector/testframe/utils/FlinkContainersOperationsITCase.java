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

package org.apache.flink.connector.testframe.utils;

import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class FlinkContainersOperationsITCase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlinkContainersOperationsITCase.class);
    public static final Network NETWORK = Network.newNetwork();
    private static final String OUTPUT_PATH = "/tmp/output";

    @RegisterExtension
    static FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .baseImage("flink:1.19.0-scala_2.12-java11")
                                    .numTaskManagers(2)
                                    .build())
                    .withTestcontainersSettings(
                            TestcontainersSettings.builder()
                                    .network(NETWORK)
                                    .logger(LOGGER)
                                    .environmentVariable("JOB_MANAGER_RPC_ADDRESS", "jobmanager")
                                    .build())
                    .build();

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        for (int i = 0; i < flink.getTaskManagers().size(); i++) {
            setup(flink.getTaskManagers().get(i), "0" + i);
        }
    }

    private static void setup(final GenericContainer<?> taskManager, final String folder)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                taskManager.execInContainer("mkdir", "-p", OUTPUT_PATH + "/" + folder + "/");
        validate(result);
        result =
                taskManager.execInContainer(
                        "sh",
                        "-c",
                        "echo 'aaa\nbbb\nccc' > " + OUTPUT_PATH + "/" + folder + "/part-11");
        validate(result);
        result =
                taskManager.execInContainer(
                        "sh",
                        "-c",
                        "echo '123\n000\n456' > " + OUTPUT_PATH + "/" + folder + "/part-22");
        validate(result);
    }

    private static void validate(Container.ExecResult result) {
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Command returned non-zero exit code.");
        }
    }

    @Test
    void testGetOutputContentUnsorted() throws IOException {
        final String content = flink.getOutputPathContent(OUTPUT_PATH, "part-11", false);
        assertThat(content).isEqualTo("aaa\nbbb\nccc\naaa\nbbb\nccc\n");
    }

    @Test
    void testGetOutputContentUnsortedSingleTaskManager() throws IOException {
        final String content = flink.getOutputPathContent(OUTPUT_PATH + "/01/", "part-22", false);
        assertThat(content).isEqualTo("123\n000\n456\n");
    }

    @Test
    void testGetOutputContentSorted() throws IOException {
        final String content = flink.getOutputPathContent(OUTPUT_PATH, "part-22", true);
        assertThat(content).isEqualTo("000\n000\n123\n123\n456\n456\n");
    }

    @Test
    void testGetOutputContentFilePatternNotPresent() throws IOException {
        final String content = flink.getOutputPathContent(OUTPUT_PATH, "dummyPattern", false);
        assertThat(content).isEmpty();
    }

    @Test
    void testGetOutputContentOutputPathNotPresent() throws IOException {
        final String content = flink.getOutputPathContent("missing", "part-*", false);
        assertThat(content).isEmpty();
    }
}
