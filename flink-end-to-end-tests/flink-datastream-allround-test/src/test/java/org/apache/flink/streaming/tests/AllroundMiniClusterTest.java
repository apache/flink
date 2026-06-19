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

package org.apache.flink.streaming.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;

/** DataStreamAllroundTestProgram on MiniCluster for manual debugging purposes. */
@Disabled("Test is already part of end-to-end tests. This is for manual debugging.")
@ExtendWith(TestLoggerExtension.class)
class AllroundMiniClusterTest {

    @BeforeAll
    static void beforeClass() {
        org.apache.log4j.PropertyConfigurator.configure(
                AllroundMiniClusterTest.class.getClassLoader().getResource("log4j.properties"));
    }

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(4)
                            .setNumberSlotsPerTaskManager(2)
                            .setConfiguration(createConfiguration())
                            .build());

    @TempDir private static Path temporaryFolder;

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.LOCAL_RECOVERY, true);
        configuration.setString(EXECUTION_FAILOVER_STRATEGY.key(), "region");
        return configuration;
    }

    @Test
    void runTest() throws Exception {
        Path checkpointDir = temporaryFolder.resolve("checkpoints");
        java.nio.file.Files.createDirectories(checkpointDir);
        DataStreamAllroundTestProgram.main(
                new String[] {
                    "--environment.parallelism", "8",
                    "--state_backend.checkpoint_directory", checkpointDir.toUri().toString(),
                    "--state_backend", "rocks",
                    "--state_backend.rocks.incremental", "true",
                    "--test.simulate_failure", "true",
                    "--test.simulate_failure.max_failures", String.valueOf(Integer.MAX_VALUE),
                    "--test.simulate_failure.num_records", "100000"
                });
    }
}
