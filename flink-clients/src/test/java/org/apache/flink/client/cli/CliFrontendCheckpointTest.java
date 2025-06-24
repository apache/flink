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

package org.apache.flink.client.cli;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.client.program.TestingClusterClient;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the CHECKPOINT command. */
class CliFrontendCheckpointTest extends CliFrontendTestBase {

    @Test
    void testTriggerCheckpointSuccess() throws Exception {
        JobID jobId = new JobID();
        long checkpointId = 15L;
        String[] parameters = {jobId.toString()};

        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();

        try {
            clusterClient.setTriggerCheckpointFunction(
                    (ignore, checkpointType) -> {
                        return CompletableFuture.completedFuture(checkpointId);
                    });

            MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
            testFrontend.checkpoint(parameters);
        } finally {
            clusterClient.close();
        }
    }

    @Test
    void testMissingJobId() {
        assertThatThrownBy(
                        () -> {
                            String[] parameters = {};
                            Configuration configuration = getConfiguration();
                            CliFrontend testFrontend =
                                    new CliFrontend(
                                            configuration, Collections.singletonList(getCli()));
                            testFrontend.checkpoint(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testFullCheckpoint() throws Exception {
        JobID jobId = new JobID();
        long checkpointId = 15L;
        String[] parameters = {"-full", jobId.toString()};

        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();

        try {
            clusterClient.setTriggerCheckpointFunction(
                    (ignore, checkpointType) -> {
                        return CompletableFuture.completedFuture(checkpointId);
                    });

            MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
            testFrontend.checkpoint(parameters);
        } finally {
            clusterClient.close();
        }
    }
}
