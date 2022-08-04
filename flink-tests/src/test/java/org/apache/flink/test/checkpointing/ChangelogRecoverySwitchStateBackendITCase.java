/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;

/**
 * This verifies that switching state backend works correctly for Changelog state backend with
 * materialized state / non-materialized state.
 */
public class ChangelogRecoverySwitchStateBackendITCase extends ChangelogRecoverySwitchEnvTestBase {

    public ChangelogRecoverySwitchStateBackendITCase(AbstractStateBackend delegatedStateBackend) {
        super(delegatedStateBackend);
    }

    @Before
    @Override
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        // reduce file threshold to reproduce FLINK-28843
        configuration.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.parse("20b"));
        FsStateChangelogStorageFactory.configure(
                configuration, TEMPORARY_FOLDER.newFolder(), Duration.ofMinutes(1), 10);
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(4)
                                .build());
        cluster.before();
        cluster.getMiniCluster().overrideRestoreModeForChangelogStateBackend();
    }

    @Test
    public void testSwitchFromEnablingToDisabling() throws Exception {
        testSwitchEnv(getEnv(true), getEnv(false));
    }

    @Test
    public void testSwitchFromEnablingToDisablingWithRescalingOut() throws Exception {
        testSwitchEnv(getEnv(true, NUM_SLOTS / 2), getEnv(false, NUM_SLOTS));
    }

    @Test
    public void testSwitchFromEnablingToDisablingWithRescalingIn() throws Exception {
        testSwitchEnv(getEnv(true, NUM_SLOTS), getEnv(false, NUM_SLOTS / 2));
    }

    @Test
    public void testSwitchFromDisablingToEnablingInClaimMode() throws Exception {
        File firstCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        MiniCluster miniCluster = cluster.getMiniCluster();
        StreamExecutionEnvironment env1 =
                getEnv(delegatedStateBackend, firstCheckpointFolder, false, 100, -1);
        SharedReference<MiniCluster> miniClusterRef = sharedObjects.add(miniCluster);

        JobGraph firstJobGraph =
                buildJobGraph(env1, TOTAL_ELEMENTS / 5, TOTAL_ELEMENTS / 4, miniClusterRef);

        try {
            miniCluster.submitJob(firstJobGraph).get();
            miniCluster.requestJobResult(firstJobGraph.getJobID()).get();
        } catch (Exception ex) {
            Preconditions.checkState(
                    ExceptionUtils.findThrowable(ex, ArtificialFailure.class).isPresent());
        }

        String firstRestorePath =
                getLatestCompletedCheckpointPath(firstJobGraph.getJobID(), miniCluster).get();

        // 1st restore, switch from disable to enable.
        File secondCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        StreamExecutionEnvironment env2 =
                getEnv(delegatedStateBackend, secondCheckpointFolder, true, 100, -1);
        JobGraph secondJobGraph =
                buildJobGraph(env2, TOTAL_ELEMENTS / 3, TOTAL_ELEMENTS / 2, miniClusterRef);
        setSavepointRestoreSettings(secondJobGraph, firstRestorePath);
        try {
            miniCluster.submitJob(secondJobGraph).get();
            miniCluster.requestJobResult(secondJobGraph.getJobID()).get();
        } catch (Exception ex) {
            Preconditions.checkState(
                    ExceptionUtils.findThrowable(ex, ArtificialFailure.class).isPresent());
        }

        String secondRestorePath =
                getLatestCompletedCheckpointPath(secondJobGraph.getJobID(), miniCluster).get();

        // 2nd restore, private state of first restore checkpoint still exist.
        File thirdCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        StreamExecutionEnvironment env3 =
                getEnv(delegatedStateBackend, thirdCheckpointFolder, true, 100, 1000);
        JobGraph thirdJobGraph =
                buildJobGraph(env3, TOTAL_ELEMENTS, TOTAL_ELEMENTS * 2 / 3, miniClusterRef);
        setSavepointRestoreSettings(thirdJobGraph, secondRestorePath);
        miniCluster.submitJob(thirdJobGraph).get();
        miniCluster.requestJobResult(thirdJobGraph.getJobID()).get();
    }

    private StreamExecutionEnvironment getEnv(boolean enableChangelog) {
        return getEnv(enableChangelog, NUM_SLOTS);
    }

    private StreamExecutionEnvironment getEnv(boolean enableChangelog, int parallelism) {
        StreamExecutionEnvironment env = getEnv(delegatedStateBackend, 100, 0, 500, 0);
        env.enableChangelogStateBackend(enableChangelog);
        env.setParallelism(parallelism);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    private StreamExecutionEnvironment getEnv(
            StateBackend stateBackend,
            File checkpointFile,
            boolean changelogEnabled,
            long checkpointInterval,
            long materializationInterval) {
        StreamExecutionEnvironment env =
                getEnv(
                        stateBackend,
                        checkpointFile,
                        checkpointInterval,
                        0,
                        materializationInterval,
                        0);
        env.enableChangelogStateBackend(changelogEnabled);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    private void setSavepointRestoreSettings(JobGraph jobGraph, String restorePath) {
        jobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(restorePath, false, RestoreMode.CLAIM));
    }
}
