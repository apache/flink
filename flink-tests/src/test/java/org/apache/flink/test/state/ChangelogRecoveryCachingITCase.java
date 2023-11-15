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

package org.apache.flink.test.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.CACHE_IDLE_TIMEOUT;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.LOCAL_RECOVERY;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG;
import static org.apache.flink.configuration.StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_ENABLED;
import static org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_MODE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_UNALIGNED;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE;
import static org.apache.flink.util.Preconditions.checkState;

/** Tests caching of changelog segments downloaded during recovery. */
public class ChangelogRecoveryCachingITCase extends TestLogger {
    private static final int ACCUMULATE_TIME_MILLIS = 500; // high enough to build some state
    private static final int PARALLELISM = 10; // high enough to trigger DSTL file multiplexing

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private OpenOnceFileSystem fileSystem;

    private MiniClusterWithClientResource cluster;

    @Before
    public void before() throws Exception {
        File tmpFolder = temporaryFolder.newFolder();
        registerFileSystem(fileSystem = new OpenOnceFileSystem(), tmpFolder.toURI().getScheme());

        Configuration configuration = new Configuration();
        configuration.set(CACHE_IDLE_TIMEOUT, Duration.ofDays(365)); // cache forever

        FsStateChangelogStorageFactory.configure(
                configuration, tmpFolder, Duration.ofMinutes(1), 10);
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(PARALLELISM)
                                .build());
        cluster.before();
    }

    @After
    public void after() throws Exception {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
        FileSystem.initialize(new Configuration(), null);
    }

    @Test
    public void test() throws Exception {
        JobID jobID1 = submit(configureJob(temporaryFolder.newFolder()), graph -> {});

        Thread.sleep(ACCUMULATE_TIME_MILLIS);
        String cpLocation = checkpointAndCancel(jobID1);

        JobID jobID2 =
                submit(
                        configureJob(temporaryFolder.newFolder()),
                        graph -> graph.setSavepointRestoreSettings(forPath(cpLocation)));
        waitForAllTaskRunning(cluster.getMiniCluster(), jobID2, true);
        cluster.getClusterClient().cancel(jobID2).get();

        checkState(fileSystem.hasOpenedPaths());
    }

    private JobID submit(Configuration conf, Consumer<JobGraph> updateGraph)
            throws InterruptedException, ExecutionException {
        JobGraph jobGraph = createJobGraph(conf);
        updateGraph.accept(jobGraph);
        return cluster.getClusterClient().submitJob(jobGraph).get();
    }

    private JobGraph createJobGraph(Configuration conf) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE)
                .keyBy(num -> num % 1000)
                .map(
                        new RichMapFunction<Long, Long>() {
                            @Override
                            public Long map(Long value) throws Exception {
                                getRuntimeContext()
                                        .getState(new ValueStateDescriptor<>("state", Long.class))
                                        .update(value);
                                return value;
                            }
                        })
                .sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private Configuration configureJob(File cpDir) {
        Configuration conf = new Configuration();

        conf.set(EXTERNALIZED_CHECKPOINT, RETAIN_ON_CANCELLATION);
        conf.set(DEFAULT_PARALLELISM, PARALLELISM);
        conf.set(ENABLE_STATE_CHANGE_LOG, true);
        conf.set(CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        conf.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(10));
        conf.set(CHECKPOINT_STORAGE, "filesystem");
        conf.set(CHECKPOINTS_DIRECTORY, cpDir.toURI().toString());
        conf.set(STATE_BACKEND, "hashmap");
        conf.set(LOCAL_RECOVERY, false); // force download
        // tune changelog
        conf.set(PREEMPTIVE_PERSIST_THRESHOLD, MemorySize.ofMebiBytes(10));
        conf.set(PERIODIC_MATERIALIZATION_INTERVAL, Duration.ofDays(-1));

        conf.set(ENABLE_UNALIGNED, true); // speedup
        conf.set(ALIGNED_CHECKPOINT_TIMEOUT, Duration.ZERO); // prevent randomization
        conf.set(
                UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE,
                1); // prevent file is opened multiple times
        conf.set(BUFFER_DEBLOAT_ENABLED, false); // prevent randomization
        conf.set(RESTART_STRATEGY, "none"); // not expecting any failures

        return conf;
    }

    private String checkpointAndCancel(JobID jobID) throws Exception {
        waitForCheckpoint(jobID, cluster.getMiniCluster(), 1);
        cluster.getClusterClient().cancel(jobID).get();
        checkStatus(jobID);
        return CommonTestUtils.getLatestCompletedCheckpointPath(jobID, cluster.getMiniCluster())
                .<NoSuchElementException>orElseThrow(
                        () -> {
                            throw new NoSuchElementException("No checkpoint was created yet");
                        });
    }

    private void checkStatus(JobID jobID) throws InterruptedException, ExecutionException {
        if (cluster.getClusterClient().getJobStatus(jobID).get().isGloballyTerminalState()) {
            cluster.getClusterClient()
                    .requestJobResult(jobID)
                    .get()
                    .getSerializedThrowable()
                    .ifPresent(
                            serializedThrowable -> {
                                throw new RuntimeException(serializedThrowable);
                            });
        }
    }

    private static class OpenOnceFileSystem extends LocalFileSystem {
        private final Set<Path> openedPaths = new HashSet<>();

        @Override
        public FSDataInputStream open(Path f) throws IOException {
            Assert.assertTrue(f + " was already opened", openedPaths.add(f));
            return super.open(f);
        }

        @Override
        public boolean isDistributedFS() {
            return true;
        }

        private boolean hasOpenedPaths() {
            return !openedPaths.isEmpty();
        }
    }

    private static void registerFileSystem(FileSystem fs, String scheme) {
        FileSystem.initialize(
                new Configuration(),
                new TestingPluginManager(
                        singletonMap(
                                FileSystemFactory.class,
                                Collections.singleton(
                                                new FileSystemFactory() {
                                                    @Override
                                                    public FileSystem create(URI fsUri) {
                                                        return fs;
                                                    }

                                                    @Override
                                                    public String getScheme() {
                                                        return scheme;
                                                    }
                                                })
                                        .iterator())));
    }
}
