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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.CollectionSink;
import org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.CountFunction;
import org.apache.flink.test.util.InfiniteIntegerSource;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CheckpointingOptions.LOCAL_RECOVERY;
import static org.apache.flink.configuration.ClusterOptions.JOB_MANAGER_PROCESS_WORKING_DIR_BASE;
import static org.apache.flink.configuration.ClusterOptions.PROCESS_WORKING_DIR_BASE;
import static org.apache.flink.configuration.ClusterOptions.TASK_MANAGER_PROCESS_WORKING_DIR_BASE;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.apache.flink.test.checkpointing.ChangelogRecoveryITCaseBase.getAllStateHandleId;

/**
 * Local recovery IT case for changelog. It never fails because local recovery is nice but not
 * necessary.
 */
@RunWith(Parameterized.class)
public class ChangelogLocalRecoveryITCase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 2;
    private static final int NUM_TASK_SLOTS = 1;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Parameterized.Parameter public AbstractStateBackend delegatedStateBackend;

    @Parameterized.Parameters(name = "delegated state backend type = {0}")
    public static Collection<AbstractStateBackend> parameter() {
        return Arrays.asList(
                new HashMapStateBackend(),
                new EmbeddedRocksDBStateBackend(false),
                new EmbeddedRocksDBStateBackend(true));
    }

    private MiniClusterWithClientResource cluster;
    private static String workingDir;

    @BeforeClass
    public static void setWorkingDir() throws IOException {
        workingDir = TEMPORARY_FOLDER.newFolder("work").getAbsolutePath();
    }

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);

        configuration.setString(PROCESS_WORKING_DIR_BASE, workingDir);
        configuration.setString(JOB_MANAGER_PROCESS_WORKING_DIR_BASE, workingDir);
        configuration.setString(TASK_MANAGER_PROCESS_WORKING_DIR_BASE, workingDir);
        configuration.setBoolean(LOCAL_RECOVERY, true);
        FsStateChangelogStorageFactory.configure(
                configuration, TEMPORARY_FOLDER.newFolder(), Duration.ofMillis(1000), 1);
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                                .build());
        cluster.before();
        cluster.getMiniCluster().overrideRestoreModeForChangelogStateBackend();
    }

    @After
    public void teardown() {
        cluster.after();
    }

    private JobGraph buildJobGraph(StreamExecutionEnvironment env) {
        env.addSource(new InfiniteIntegerSource())
                .setParallelism(1)
                .keyBy(element -> element)
                .process(new CountFunction())
                .addSink(new CollectionSink())
                .setParallelism(1);
        return env.getStreamGraph().getJobGraph();
    }

    @Test
    public void testRestartTM() throws Exception {
        File checkpointFolder = TEMPORARY_FOLDER.newFolder();
        MiniCluster miniCluster = cluster.getMiniCluster();
        StreamExecutionEnvironment env1 =
                getEnv(delegatedStateBackend, checkpointFolder, true, 200, 800);
        JobGraph firstJobGraph = buildJobGraph(env1);

        miniCluster.submitJob(firstJobGraph).get();
        waitForAllTaskRunning(miniCluster, firstJobGraph.getJobID(), false);
        // wait job for doing materialization.
        waitUntilCondition(
                () -> !getAllStateHandleId(firstJobGraph.getJobID(), miniCluster).isEmpty());
        CompletableFuture<Void> terminationFuture = miniCluster.terminateTaskManager(1);
        terminationFuture.get();
        miniCluster.startTaskManager();
        waitForAllTaskRunning(
                () ->
                        miniCluster
                                .getExecutionGraph(firstJobGraph.getJobID())
                                .get(500, TimeUnit.SECONDS),
                false);
        miniCluster.triggerCheckpoint(firstJobGraph.getJobID());
    }

    private StreamExecutionEnvironment getEnv(
            StateBackend stateBackend,
            File checkpointFile,
            boolean changelogEnabled,
            long checkpointInterval,
            long materializationInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);
        env.setStateBackend(stateBackend)
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10));
        env.configure(new Configuration().set(LOCAL_RECOVERY, true));

        env.getCheckpointConfig().setCheckpointStorage(checkpointFile.toURI());
        env.enableChangelogStateBackend(changelogEnabled);
        env.configure(
                new Configuration()
                        .set(
                                StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL,
                                Duration.ofMillis(materializationInterval))
                        .set(StateChangelogOptions.MATERIALIZATION_MAX_FAILURES_ALLOWED, 1));
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        env.configure(configuration);
        return env;
    }
}
