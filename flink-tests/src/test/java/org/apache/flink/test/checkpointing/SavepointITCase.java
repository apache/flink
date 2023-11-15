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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.EntropyInjectingTestFileSystem;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.apache.flink.util.ExceptionUtils.assertThrowable;
import static org.apache.flink.util.ExceptionUtils.assertThrowableWithMessage;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Integration test for triggering and resuming from savepoints. */
@SuppressWarnings("serial")
public class SavepointITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointITCase.class);

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Rule public final TemporaryFolder folder = new TemporaryFolder();

    private File checkpointDir;

    private File savepointDir;

    @Before
    public void setUp() throws Exception {
        final File testRoot = folder.newFolder();

        checkpointDir = new File(testRoot, "checkpoints");
        savepointDir = new File(testRoot, "savepoints");

        if (!checkpointDir.mkdir() || !savepointDir.mkdirs()) {
            fail("Test setup failed: failed to create temporary directories.");
        }
    }

    @Test
    public void testStopWithSavepointForFlip27SourceWithDrain() throws Exception {
        testStopWithSavepointForFlip27Source(true);
    }

    @Test
    public void testStopWithSavepointForFlip27SourceWithoutDrain() throws Exception {
        testStopWithSavepointForFlip27Source(false);
    }

    private void testStopWithSavepointForFlip27Source(boolean drain) throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        BoundedPassThroughOperator<Long> operator =
                new BoundedPassThroughOperator<>(ChainingStrategy.ALWAYS);
        DataStream<Long> stream =
                env.fromSequence(0, Long.MAX_VALUE)
                        .transform("pass-through", BasicTypeInfo.LONG_TYPE_INFO, operator);
        stream.sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        final JobID jobId = jobGraph.getJobID();

        MiniClusterWithClientResource cluster = clusterFactory.get();
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        try {
            BoundedPassThroughOperator.resetForTest(1, true);

            client.submitJob(jobGraph).get();

            BoundedPassThroughOperator.getProgressLatch().await();
            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);

            client.stopWithSavepoint(jobId, drain, null, SavepointFormatType.CANONICAL).get();

            if (drain) {
                Assert.assertTrue(BoundedPassThroughOperator.inputEnded);
            } else {
                Assert.assertFalse(BoundedPassThroughOperator.inputEnded);
            }
        } finally {
            cluster.after();
        }
    }

    @Test
    public void testStopWithSavepointWithDrainCallsFinishBeforeSnapshotState() throws Exception {
        int sinkParallelism = 5;
        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(sinkParallelism + 1)
                                .build());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.addSource(new InfiniteTestSource())
                .setParallelism(1)
                .name("Infinite Source")
                .addSink(new FinishingSink<>())
                // different parallelism to break chaining and add some concurrent tasks
                .setParallelism(sinkParallelism);

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        cluster.before();
        try {
            ClusterClient<?> client = cluster.getClusterClient();
            client.submitJob(jobGraph).get();
            waitUntilAllTasksAreRunning(cluster.getRestClusterClient(), jobGraph.getJobID());

            client.stopWithSavepoint(
                            jobGraph.getJobID(),
                            true,
                            savepointDir.getAbsolutePath(),
                            SavepointFormatType.CANONICAL)
                    .get();
            // there should be no exceptions and the finish should've been called in the
            // FinishingSink
        } finally {
            cluster.after();
        }
    }

    private static class FinishingSink<T> implements SinkFunction<T>, CheckpointedFunction {
        private boolean finishCalled;

        @Override
        public void invoke(T value) throws Exception {
            // ignore
        }

        @Override
        public void finish() throws Exception {
            this.finishCalled = true;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!finishCalled) {
                fail("Finish is expected to be called before taking the savepoint with drain");
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }

    @Test
    public void testStopWithSavepointFailsOverToSavepoint() throws Throwable {
        int sinkParallelism = 5;
        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(sinkParallelism + 1)
                                .build());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10));
        env.setParallelism(1);
        env.addSource(new InfiniteTestSource())
                .name("Infinite Source")
                .map(new FailingOnCompletedSavepointMapFunction(2))
                .sinkTo(new DiscardingSink<>())
                // different parallelism to break chaining and add some concurrent tasks
                .setParallelism(sinkParallelism);

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        cluster.before();
        try {
            ClusterClient<?> client = cluster.getClusterClient();
            client.submitJob(jobGraph).get();
            waitUntilAllTasksAreRunning(cluster.getRestClusterClient(), jobGraph.getJobID());

            cluster.getMiniCluster().triggerCheckpoint(jobGraph.getJobID()).get();
            final CompletableFuture<String> savepointCompleted =
                    client.stopWithSavepoint(
                            jobGraph.getJobID(),
                            true,
                            savepointDir.getAbsolutePath(),
                            SavepointFormatType.CANONICAL);

            final Throwable savepointException =
                    assertThrows(ExecutionException.class, savepointCompleted::get).getCause();
            assertThrowable(
                    savepointException,
                    throwable ->
                            throwable instanceof StopWithSavepointStoppingException
                                    && throwable
                                            .getMessage()
                                            .startsWith("A savepoint has been created at: "));
            assertThat(
                    client.getJobStatus(jobGraph.getJobID()).get(),
                    either(is(JobStatus.FAILED)).or(is(JobStatus.FAILING)));
        } finally {
            cluster.after();
        }
    }

    private static final class FailingOnCompletedSavepointMapFunction
            extends RichMapFunction<Integer, Integer> implements CheckpointListener {
        private final long savepointId;

        private FailingOnCompletedSavepointMapFunction(long savepointId) {
            this.savepointId = savepointId;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (checkpointId == savepointId) {
                throw new ExpectedTestException();
            }
        }
    }

    /**
     * Triggers a savepoint for a job that uses the FsStateBackend. We expect that all checkpoint
     * files are written to a new savepoint directory.
     *
     * <ol>
     *   <li>Submit job, wait for some progress
     *   <li>Trigger savepoint and verify that savepoint has been created
     *   <li>Shut down the cluster, re-submit the job from the savepoint, verify that the initial
     *       state has been reset, and all tasks are running again
     *   <li>Cancel job, dispose the savepoint, and verify that everything has been cleaned up
     * </ol>
     */
    @Test
    public void testTriggerSavepointAndResumeWithFileBasedCheckpoints() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

        final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism);
        verifySavepoint(parallelism, savepointPath);

        restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism);
    }

    @Test
    public void testTriggerSavepointAndResumeWithClaim() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

        final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism);
        verifySavepoint(parallelism, savepointPath);
        restoreJobAndVerifyState(
                clusterFactory,
                parallelism,
                SavepointRestoreSettings.forPath(savepointPath, false, RestoreMode.CLAIM),
                cluster -> {
                    cluster.after();

                    assertFalse(
                            "Savepoint not properly cleaned up.",
                            new File(new URI(savepointPath)).exists());
                });
    }

    @Test
    public void testTriggerSavepointAndResumeWithLegacyMode() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

        final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism);
        verifySavepoint(parallelism, savepointPath);
        restoreJobAndVerifyState(
                clusterFactory,
                parallelism,
                SavepointRestoreSettings.forPath(savepointPath, false, RestoreMode.LEGACY),
                cluster -> {
                    cluster.after();

                    assertTrue(
                            "Savepoint unexpectedly cleaned up.",
                            new File(new URI(savepointPath)).exists());
                });
    }

    @Rule public SharedObjects sharedObjects = SharedObjects.create();

    @Test
    @Ignore("Disabling this test because it regularly fails on AZP. See FLINK-25427.")
    public void testTriggerSavepointAndResumeWithNoClaim() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(folder.newFolder().toURI());
        env.setParallelism(parallelism);

        final SharedReference<CountDownLatch> counter =
                sharedObjects.add(new CountDownLatch(10_000));
        env.fromSequence(1, Long.MAX_VALUE)
                .keyBy(i -> i % parallelism)
                .process(
                        new KeyedProcessFunction<Long, Long, Long>() {
                            private ListState<Long> last;

                            @Override
                            public void open(OpenContext openContext) {
                                // we use list state here to create sst files of a significant size
                                // if sst files do not reach certain thresholds they are not stored
                                // in files, but as a byte stream in checkpoints metadata
                                last =
                                        getRuntimeContext()
                                                .getListState(
                                                        new ListStateDescriptor<>(
                                                                "last",
                                                                BasicTypeInfo.LONG_TYPE_INFO));
                            }

                            @Override
                            public void processElement(
                                    Long value,
                                    KeyedProcessFunction<Long, Long, Long>.Context ctx,
                                    Collector<Long> out)
                                    throws Exception {
                                last.add(value);
                                out.collect(value);
                            }
                        })
                .addSink(
                        new SinkFunction<Long>() {
                            @Override
                            public void invoke(Long value) {
                                counter.consumeSync(CountDownLatch::countDown);
                            }
                        })
                .setParallelism(1);

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());
        cluster.before();
        try {
            final JobID jobID1 = new JobID();
            jobGraph.setJobID(jobID1);
            cluster.getClusterClient().submitJob(jobGraph).get();
            CommonTestUtils.waitForAllTaskRunning(cluster.getMiniCluster(), jobID1, false);
            // wait for some records to be processed before taking the checkpoint
            counter.get().await();
            final String firstCheckpoint = cluster.getMiniCluster().triggerCheckpoint(jobID1).get();

            cluster.getClusterClient().cancel(jobID1).get();
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(firstCheckpoint, false, RestoreMode.NO_CLAIM));
            final JobID jobID2 = new JobID();
            jobGraph.setJobID(jobID2);
            cluster.getClusterClient().submitJob(jobGraph).get();
            CommonTestUtils.waitForAllTaskRunning(cluster.getMiniCluster(), jobID2, false);
            String secondCheckpoint = cluster.getMiniCluster().triggerCheckpoint(jobID2).get();
            cluster.getClusterClient().cancel(jobID2).get();

            // delete the checkpoint we restored from
            FileUtils.deleteDirectory(Paths.get(new URI(firstCheckpoint)).getParent().toFile());

            // we should be able to restore from the second checkpoint even though it has been built
            // on top of the first checkpoint
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(
                            secondCheckpoint, false, RestoreMode.NO_CLAIM));
            final JobID jobID3 = new JobID();
            jobGraph.setJobID(jobID3);
            cluster.getClusterClient().submitJob(jobGraph).get();
            CommonTestUtils.waitForAllTaskRunning(cluster.getMiniCluster(), jobID3, false);
        } finally {
            cluster.after();
        }
    }

    @Test
    public void testTriggerSavepointAndResumeWithFileBasedCheckpointsAndRelocateBasePath()
            throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

        final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism);
        final org.apache.flink.core.fs.Path oldPath =
                new org.apache.flink.core.fs.Path(savepointPath);
        final org.apache.flink.core.fs.Path newPath =
                new org.apache.flink.core.fs.Path(folder.newFolder().toURI().toString());
        (new org.apache.flink.core.fs.Path(savepointPath).getFileSystem()).rename(oldPath, newPath);
        verifySavepoint(parallelism, newPath.toUri().toString());

        restoreJobAndVerifyState(newPath.toUri().toString(), clusterFactory, parallelism);
    }

    @Test
    public void testShouldAddEntropyToSavepointPath() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers,
                        numSlotsPerTaskManager,
                        getCheckpointingWithEntropyConfig());

        final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism);
        assertThat(savepointDir, hasEntropyInFileStateHandlePaths());

        restoreJobAndVerifyState(
                clusterFactory,
                parallelism,
                SavepointRestoreSettings.forPath(savepointPath),
                cluster -> {
                    final URI localURI = new URI(savepointPath.replace("test-entropy:/", "file:/"));
                    assertTrue("Savepoint has not been created", new File(localURI).exists());
                    cluster.getClusterClient().disposeSavepoint(savepointPath).get();
                    assertFalse("Savepoint not properly cleaned up.", new File(localURI).exists());
                });
    }

    private Configuration getCheckpointingWithEntropyConfig() {
        final String savepointPathWithEntropyPlaceholder =
                new File(savepointDir, EntropyInjectingTestFileSystem.ENTROPY_INJECTION_KEY)
                        .getPath();
        final Configuration config =
                getFileBasedCheckpointsConfig(
                        "test-entropy://" + savepointPathWithEntropyPlaceholder);
        config.setString("s3.entropy.key", EntropyInjectingTestFileSystem.ENTROPY_INJECTION_KEY);
        return config;
    }

    private String submitJobAndTakeSavepoint(
            MiniClusterResourceFactory clusterFactory, int parallelism) throws Exception {
        final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000);
        final JobID jobId = jobGraph.getJobID();
        StatefulCounter.resetForTest(parallelism);

        MiniClusterWithClientResource cluster = clusterFactory.get();
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        try {
            client.submitJob(jobGraph).get();

            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
            StatefulCounter.getProgressLatch().await();

            return client.cancelWithSavepoint(jobId, null, SavepointFormatType.CANONICAL).get();
        } finally {
            cluster.after();
            StatefulCounter.resetForTest(parallelism);
        }
    }

    private void verifySavepoint(final int parallelism, final String savepointPath)
            throws URISyntaxException {
        // Only one savepoint should exist
        File savepointDir = new File(new URI(savepointPath));
        assertTrue("Savepoint directory does not exist.", savepointDir.exists());
        assertTrue(
                "Savepoint did not create self-contained directory.", savepointDir.isDirectory());

        File[] savepointFiles = savepointDir.listFiles();

        if (savepointFiles != null) {
            // Expect one metadata file and one checkpoint file per stateful
            // parallel subtask
            String errMsg =
                    "Did not write expected number of savepoint/checkpoint files to directory: "
                            + Arrays.toString(savepointFiles);
            assertEquals(errMsg, 1 + parallelism, savepointFiles.length);
        } else {
            fail(String.format("Returned savepoint path (%s) is not valid.", savepointPath));
        }
    }

    private void restoreJobAndVerifyState(
            String savepointPath, MiniClusterResourceFactory clusterFactory, int parallelism)
            throws Exception {
        restoreJobAndVerifyState(
                clusterFactory,
                parallelism,
                SavepointRestoreSettings.forPath(savepointPath, false),
                cluster -> {
                    cluster.getClusterClient().disposeSavepoint(savepointPath).get();
                    assertFalse(
                            "Savepoint not properly cleaned up.",
                            new File(new URI(savepointPath)).exists());
                });
    }

    @FunctionalInterface
    interface PostCancelChecker {
        void check(MiniClusterWithClientResource cluster) throws Exception;
    }

    private void restoreJobAndVerifyState(
            MiniClusterResourceFactory clusterFactory,
            int parallelism,
            SavepointRestoreSettings savepointRestoreSettings,
            PostCancelChecker postCancelChecks)
            throws Exception {
        final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000);
        jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
        final JobID jobId = jobGraph.getJobID();
        StatefulCounter.resetForTest(parallelism);

        MiniClusterWithClientResource cluster = clusterFactory.get();
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        try {
            client.submitJob(jobGraph).get();

            // Await state is restored
            StatefulCounter.getRestoreLatch().await();

            // Await some progress after restore
            StatefulCounter.getProgressLatch().await();

            client.cancel(jobId).get();

            FutureUtils.retrySuccessfulWithDelay(
                    () -> client.getJobStatus(jobId),
                    Duration.ofMillis(50),
                    Deadline.now().plus(Duration.ofSeconds(30)),
                    status -> status == JobStatus.CANCELED,
                    new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()));

            postCancelChecks.check(cluster);
        } finally {
            cluster.after();
            StatefulCounter.resetForTest(parallelism);
        }
    }

    @Test
    public void testTriggerSavepointForNonExistingJob() throws Exception {
        // Config
        final int numTaskManagers = 1;
        final int numSlotsPerTaskManager = 1;

        final Configuration config = new Configuration();
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        final MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());
        cluster.before();
        final ClusterClient<?> client = cluster.getClusterClient();

        final JobID jobID = new JobID();

        try {
            client.triggerSavepoint(jobID, null, SavepointFormatType.CANONICAL).get();

            fail();
        } catch (ExecutionException e) {
            assertThrowable(e, FlinkJobNotFoundException.class);
            assertThrowableWithMessage(e, jobID.toString());
        } finally {
            cluster.after();
        }
    }

    @Test
    public void testTriggerSavepointWithCheckpointingDisabled() throws Exception {
        // Config
        final int numTaskManagers = 1;
        final int numSlotsPerTaskManager = 1;

        final Configuration config = new Configuration();

        final MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());
        cluster.before();
        final ClusterClient<?> client = cluster.getClusterClient();

        final JobVertex vertex = new JobVertex("Blocking vertex");
        vertex.setInvokableClass(BlockingNoOpInvokable.class);
        vertex.setParallelism(1);

        final JobGraph graph = JobGraphTestUtils.streamingJobGraph(vertex);

        try {
            client.submitJob(graph).get();
            // triggerSavepoint is only available after all tasks are running
            waitForAllTaskRunning(cluster.getMiniCluster(), graph.getJobID(), false);

            client.triggerSavepoint(graph.getJobID(), null, SavepointFormatType.CANONICAL).get();

            fail();
        } catch (ExecutionException e) {
            assertThrowable(e, IllegalStateException.class);
            assertThrowableWithMessage(e, graph.getJobID().toString());
            assertThrowableWithMessage(e, "is not a streaming job");
        } finally {
            cluster.after();
        }
    }

    @Test
    public void testTriggerSavepointWithoutCheckpointBaseLocations() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().disableCheckpointing();
        env.setParallelism(1);

        env.addSource(new IntegerStreamSource()).sinkTo(new DiscardingSink<>());

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        Configuration config = getFileBasedCheckpointsConfig();
        config.addAll(jobGraph.getJobConfiguration());

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .build());
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        String savepointPath = null;
        try {
            client.submitJob(jobGraph).get();

            waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);

            savepointPath =
                    client.triggerSavepoint(
                                    jobGraph.getJobID(), null, SavepointFormatType.CANONICAL)
                            .get();

            assertNotNull(savepointPath);

            client.cancel(jobGraph.getJobID()).get();
            // checkpoint directory should not be initialized
            assertEquals(0, Objects.requireNonNull(checkpointDir.listFiles()).length);
        } finally {
            if (null != savepointPath) {
                client.disposeSavepoint(savepointPath);
            }
            cluster.after();
        }
    }

    static class BoundedPassThroughOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, BoundedOneInput {
        static volatile CountDownLatch progressLatch;
        static volatile CountDownLatch snapshotAllowedLatch;
        static volatile CountDownLatch snapshotStartedLatch;
        static volatile boolean inputEnded;

        private transient boolean processed;

        BoundedPassThroughOperator(ChainingStrategy chainingStrategy) {
            this.chainingStrategy = chainingStrategy;
        }

        private static void allowSnapshots() {
            snapshotAllowedLatch.countDown();
        }

        public static void awaitSnapshotStarted() throws InterruptedException {
            snapshotStartedLatch.await();
        }

        @Override
        public void endInput() throws Exception {
            inputEnded = true;
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            output.collect(element);
            if (!processed) {
                processed = true;
                progressLatch.countDown();
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            snapshotStartedLatch.countDown();
            snapshotAllowedLatch.await();
            super.snapshotState(context);
        }

        // --------------------------------------------------------------------

        static CountDownLatch getProgressLatch() {
            return progressLatch;
        }

        static void resetForTest(int parallelism, boolean allowSnapshots) {
            progressLatch = new CountDownLatch(parallelism);
            snapshotAllowedLatch = new CountDownLatch(allowSnapshots ? 0 : 1);
            snapshotStartedLatch = new CountDownLatch(parallelism);
            inputEnded = false;
        }
    }

    @Test
    public void testStopSavepointWithBoundedInput() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;

        for (ChainingStrategy chainingStrategy : ChainingStrategy.values()) {
            final MiniClusterResourceFactory clusterFactory =
                    new MiniClusterResourceFactory(
                            numTaskManagers,
                            numSlotsPerTaskManager,
                            getFileBasedCheckpointsConfig());

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            BoundedPassThroughOperator<Integer> operator =
                    new BoundedPassThroughOperator<>(chainingStrategy);
            DataStream<Integer> stream =
                    env.addSource(new InfiniteTestSource())
                            .transform("pass-through", BasicTypeInfo.INT_TYPE_INFO, operator);

            stream.sinkTo(new DiscardingSink<>());

            final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            final JobID jobId = jobGraph.getJobID();

            MiniClusterWithClientResource cluster = clusterFactory.get();
            cluster.before();
            ClusterClient<?> client = cluster.getClusterClient();

            try {
                BoundedPassThroughOperator.resetForTest(1, true);

                client.submitJob(jobGraph).get();

                BoundedPassThroughOperator.getProgressLatch().await();
                waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);

                client.stopWithSavepoint(jobId, false, null, SavepointFormatType.CANONICAL).get();

                Assert.assertFalse(
                        "input ended with chainingStrategy " + chainingStrategy,
                        BoundedPassThroughOperator.inputEnded);
            } finally {
                cluster.after();
            }
        }
    }

    @Test
    public void testSubmitWithUnknownSavepointPath() throws Exception {
        // Config
        int numTaskManagers = 1;
        int numSlotsPerTaskManager = 1;
        int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final Configuration config = new Configuration();
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        try {

            // High value to ensure timeouts if restarted.
            int numberOfRetries = 1000;
            // Submit the job
            // Long delay to ensure that the test times out if the job
            // manager tries to restart the job.
            final JobGraph jobGraph = createJobGraph(parallelism, numberOfRetries, 3600000);

            // Set non-existing savepoint path
            jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("unknown path"));
            assertEquals("unknown path", jobGraph.getSavepointRestoreSettings().getRestorePath());

            LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

            try {
                submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());
            } catch (Exception e) {
                Optional<JobExecutionException> expectedJobExecutionException =
                        findThrowable(e, JobExecutionException.class);
                Optional<FileNotFoundException> expectedFileNotFoundException =
                        findThrowable(e, FileNotFoundException.class);
                if (!(expectedJobExecutionException.isPresent()
                        && expectedFileNotFoundException.isPresent())) {
                    throw e;
                }
            }
        } finally {
            cluster.after();
        }
    }

    @Test
    public void testStopWithSavepointFailingInSnapshotCreation() throws Exception {
        testStopWithFailingSourceInOnePipeline(
                new SnapshotFailingInfiniteTestSource(),
                folder.newFolder(),
                // two restarts expected:
                // 1. task failure restart
                // 2. job failover triggered by the CheckpointFailureManager
                2,
                assertInSnapshotCreationFailure(),
                true);
    }

    @Test
    public void testStopWithSavepointFailingAfterSnapshotCreation() throws Exception {
        // the trigger need to be reset in case the test is run multiple times
        CancelFailingInfiniteTestSource.checkpointCompleteTriggered = false;
        testStopWithFailingSourceInOnePipeline(
                new CancelFailingInfiniteTestSource(),
                folder.newFolder(),
                // two restarts expected:
                // 1. task failure restart
                // 2. job failover triggered by SchedulerBase.stopWithSavepoint
                0,
                (jobId, actualException) -> {
                    Optional<StopWithSavepointStoppingException> actualFlinkException =
                            findThrowable(
                                    actualException, StopWithSavepointStoppingException.class);
                    return actualFlinkException
                            .map(e -> e.getMessage().startsWith("A savepoint has been created at:"))
                            .orElse(false);
                },
                false);
    }

    @Test
    public void testStopWithSavepointWithDrainGlobalFailoverIfSavepointAborted() throws Exception {
        final int parallelism = 2;
        PathFailingFileSystem.resetFailingPath(savepointDir.getAbsolutePath() + ".*/_metadata");
        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(parallelism)
                                .build());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig()
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        env.addSource(new InfiniteTestSource())
                .name("Infinite test source")
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        cluster.before();
        try {
            ClusterClient<?> client = cluster.getClusterClient();
            client.submitJob(jobGraph).get();
            waitUntilAllTasksAreRunning(cluster.getRestClusterClient(), jobGraph.getJobID());

            try {
                client.stopWithSavepoint(
                                jobGraph.getJobID(),
                                true,
                                PathFailingFileSystem.SCHEME
                                        + "://"
                                        + savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();
                fail("The future should fail exceptionally.");
            } catch (ExecutionException ex) {
                // expected
                if (!findThrowableWithMessage(ex, "Expected IO exception").isPresent()) {
                    throw ex;
                }
            }

            // make sure that we restart all tasks after the savepoint failure
            waitUntilAllTasksAreRunning(cluster.getRestClusterClient(), jobGraph.getJobID());
        } finally {
            cluster.after();
        }
    }

    private static BiFunction<JobID, ExecutionException, Boolean>
            assertInSnapshotCreationFailure() {
        return (ignored, actualException) -> {
            if (ClusterOptions.isAdaptiveSchedulerEnabled(new Configuration())) {
                return findThrowable(actualException, FlinkException.class).isPresent();
            } else {
                return findThrowable(actualException, CheckpointException.class).isPresent();
            }
        };
    }

    private static OneShotLatch failingPipelineLatch;
    private static OneShotLatch succeedingPipelineLatch;

    /**
     * FLINK-21030
     *
     * <p>Tests the handling of a failure that happened while stopping an embarrassingly parallel
     * job with a Savepoint. The test expects that the stopping action fails and all executions are
     * in state {@code RUNNING} afterwards.
     *
     * @param failingSource the failing {@link SourceFunction} used in one of the two pipelines.
     * @param expectedMaximumNumberOfRestarts the maximum number of restarts allowed by the restart
     *     strategy.
     * @param exceptionAssertion asserts the client-call exception to verify that the right error
     *     was handled.
     * @see SavepointITCase#failingPipelineLatch The latch used to trigger the successful start of
     *     the later on failing pipeline.
     * @see SavepointITCase#succeedingPipelineLatch The latch that triggers the successful start of
     *     the succeeding pipeline.
     * @throws Exception if an error occurred while running the test.
     */
    private static void testStopWithFailingSourceInOnePipeline(
            InfiniteTestSource failingSource,
            File savepointDir,
            int expectedMaximumNumberOfRestarts,
            BiFunction<JobID, ExecutionException, Boolean> exceptionAssertion,
            boolean shouldRestart)
            throws Exception {
        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder().build());

        failingPipelineLatch = new OneShotLatch();
        succeedingPipelineLatch = new OneShotLatch();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig()
                .setRestartStrategy(
                        RestartStrategies.fixedDelayRestart(expectedMaximumNumberOfRestarts, 0));
        env.addSource(failingSource)
                .name("Failing Source")
                .map(
                        value -> {
                            failingPipelineLatch.trigger();
                            return value;
                        })
                .sinkTo(new DiscardingSink<>());
        env.addSource(new InfiniteTestSource())
                .name("Succeeding Source")
                .map(
                        value -> {
                            succeedingPipelineLatch.trigger();
                            return value;
                        })
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        cluster.before();
        try {
            ClusterClient<?> client = cluster.getClusterClient();
            JobID jobID = client.submitJob(jobGraph).get();

            // we need to wait for both pipelines to be in state RUNNING because that's the only
            // state which allows creating a savepoint
            failingPipelineLatch.await();
            succeedingPipelineLatch.await();
            waitForAllTaskRunning(cluster.getMiniCluster(), jobID, false);

            try {
                client.stopWithSavepoint(
                                jobGraph.getJobID(),
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();
                fail("The future should fail exceptionally.");
            } catch (ExecutionException e) {
                assertThrowable(e, ex -> exceptionAssertion.apply(jobGraph.getJobID(), e));
            }

            if (shouldRestart) {
                waitUntilAllTasksAreRunning(cluster.getRestClusterClient(), jobGraph.getJobID());
            }
        } finally {
            cluster.after();
        }
    }

    public static void waitUntilAllTasksAreRunning(
            RestClusterClient<?> restClusterClient, JobID jobId) throws Exception {
        // access the REST endpoint of the cluster to determine the state of each
        // ExecutionVertex

        final JobDetailsHeaders detailsHeaders = JobDetailsHeaders.getInstance();
        final JobMessageParameters params = detailsHeaders.getUnresolvedMessageParameters();
        params.jobPathParameter.resolve(jobId);

        CommonTestUtils.waitUntilCondition(
                () ->
                        restClusterClient
                                .sendRequest(detailsHeaders, params, EmptyRequestBody.getInstance())
                                .thenApply(
                                        detailsInfo ->
                                                allVerticesRunning(
                                                        detailsInfo.getJobVerticesPerState()))
                                .get());
    }

    private static boolean allVerticesRunning(Map<ExecutionState, Integer> states) {
        return states.entrySet().stream()
                .allMatch(
                        entry -> {
                            if (entry.getKey() == ExecutionState.RUNNING) {
                                return entry.getValue() > 0;
                            } else {
                                return entry.getValue() == 0; // no vertices in non-running state.
                            }
                        });
    }

    /**
     * FLINK-5985
     *
     * <p>This test ensures we can restore from a savepoint under modifications to the job graph
     * that only concern stateless operators.
     */
    @Test
    public void testCanRestoreWithModifiedStatelessOperators() throws Exception {

        // Config
        int numTaskManagers = 2;
        int numSlotsPerTaskManager = 2;
        int parallelism = 2;

        // Test deadline
        final Deadline deadline = Deadline.now().plus(Duration.ofMinutes(5));

        // Flink configuration
        final Configuration config = new Configuration();
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        String savepointPath;

        LOG.info("Flink configuration: " + config + ".");

        // Start Flink
        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());

        LOG.info("Shutting down Flink cluster.");
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();
        try {
            final StatefulCounter statefulCounter = new StatefulCounter();
            StatefulCounter.resetForTest(parallelism);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            env.addSource(new InfiniteTestSource())
                    .shuffle()
                    .map(value -> 4 * value)
                    .shuffle()
                    .map(statefulCounter)
                    .uid("statefulCounter")
                    .shuffle()
                    .map(value -> 2 * value)
                    .sinkTo(new DiscardingSink<>());

            JobGraph originalJobGraph = env.getStreamGraph().getJobGraph();

            JobID jobID = client.submitJob(originalJobGraph).get();

            // wait for the Tasks to be ready
            waitForAllTaskRunning(cluster.getMiniCluster(), jobID, false);
            assertTrue(
                    StatefulCounter.getProgressLatch()
                            .await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

            savepointPath =
                    client.triggerSavepoint(jobID, null, SavepointFormatType.CANONICAL).get();
            LOG.info("Retrieved savepoint: " + savepointPath + ".");
        } finally {
            // Shut down the Flink cluster (thereby canceling the job)
            LOG.info("Shutting down Flink cluster.");
            cluster.after();
        }

        // create a new MiniCluster to make sure we start with completely
        // new resources
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());
        LOG.info("Restarting Flink cluster.");
        cluster.before();
        client = cluster.getClusterClient();
        try {
            // Reset static test helpers
            StatefulCounter.resetForTest(parallelism);

            // Gather all task deployment descriptors
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            // generate a modified job graph that adds a stateless op
            env.addSource(new InfiniteTestSource())
                    .shuffle()
                    .map(new StatefulCounter())
                    .uid("statefulCounter")
                    .shuffle()
                    .map(value -> value)
                    .sinkTo(new DiscardingSink<>());

            JobGraph modifiedJobGraph = env.getStreamGraph().getJobGraph();

            // Set the savepoint path
            modifiedJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath));

            LOG.info(
                    "Resubmitting job "
                            + modifiedJobGraph.getJobID()
                            + " with "
                            + "savepoint path "
                            + savepointPath
                            + " in detached mode.");

            // Submit the job
            client.submitJob(modifiedJobGraph).get();
            // Await state is restored
            assertTrue(
                    StatefulCounter.getRestoreLatch()
                            .await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

            // Await some progress after restore
            assertTrue(
                    StatefulCounter.getProgressLatch()
                            .await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));
        } finally {
            cluster.after();
        }
    }

    // ------------------------------------------------------------------------
    // Test program
    // ------------------------------------------------------------------------

    /** Creates a streaming JobGraph from the StreamEnvironment. */
    private JobGraph createJobGraph(int parallelism, int numberOfRetries, long restartDelay) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.disableOperatorChaining();
        env.getConfig()
                .setRestartStrategy(
                        RestartStrategies.fixedDelayRestart(numberOfRetries, restartDelay));

        DataStream<Integer> stream =
                env.addSource(new InfiniteTestSource()).shuffle().map(new StatefulCounter());

        stream.sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class InfiniteTestSource implements ParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private volatile boolean suspended = false;
        private static final Collection<InfiniteTestSource> createdSources =
                new CopyOnWriteArrayList<>();
        private transient volatile CompletableFuture<Void> completeFuture;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            completeFuture = new CompletableFuture<>();
            createdSources.add(this);
            try {
                while (running) {
                    if (!suspended) {
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(1);
                        }
                    }
                    Thread.sleep(1);
                }
                completeFuture.complete(null);
            } catch (Exception e) {
                completeFuture.completeExceptionally(e);
                throw e;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        public void suspend() {
            suspended = true;
        }

        public static void resetForTest() {
            createdSources.clear();
        }

        public CompletableFuture<Void> getCompleteFuture() {
            return completeFuture;
        }

        public static void cancelAllAndAwait() throws ExecutionException, InterruptedException {
            createdSources.forEach(InfiniteTestSource::cancel);
            allOf(
                            createdSources.stream()
                                    .map(InfiniteTestSource::getCompleteFuture)
                                    .toArray(CompletableFuture[]::new))
                    .get();
        }

        public static void suspendAll() {
            createdSources.forEach(InfiniteTestSource::suspend);
        }
    }

    /**
     * An {@link InfiniteTestSource} implementation that fails when cancel is called for the first
     * time.
     */
    private static class CancelFailingInfiniteTestSource extends InfiniteTestSource
            implements CheckpointListener {

        private static volatile boolean checkpointCompleteTriggered = false;

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (!checkpointCompleteTriggered) {
                checkpointCompleteTriggered = true;
                throw new RuntimeException("Expected RuntimeException after snapshot creation.");
            }
            super.cancel();
        }
    }

    /** An {@link InfiniteTestSource} implementation that fails while creating a snapshot. */
    private static class SnapshotFailingInfiniteTestSource extends InfiniteTestSource
            implements CheckpointedFunction {

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            throw new Exception(
                    "Expected Exception happened during snapshot creation within test source");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // all good here
        }
    }

    private static class StatefulCounter extends RichMapFunction<Integer, Integer>
            implements ListCheckpointed<byte[]> {

        private static volatile CountDownLatch progressLatch = new CountDownLatch(0);
        private static volatile CountDownLatch restoreLatch = new CountDownLatch(0);

        private int numCollectedElements = 0;

        private static final long serialVersionUID = 7317800376639115920L;
        private byte[] data;

        @Override
        public void open(OpenContext openContext) throws Exception {
            if (data == null) {
                // We need this to be large, because we want to test with files
                Random rand = new Random(getRuntimeContext().getIndexOfThisSubtask());
                data =
                        new byte
                                [(int)
                                                CheckpointingOptions.FS_SMALL_FILE_THRESHOLD
                                                        .defaultValue()
                                                        .getBytes()
                                        + 1];
                rand.nextBytes(data);
            }
        }

        @Override
        public Integer map(Integer value) throws Exception {
            for (int i = 0; i < data.length; i++) {
                data[i] += 1;
            }

            if (numCollectedElements++ > 10) {
                progressLatch.countDown();
            }

            return value;
        }

        @Override
        public List<byte[]> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(data);
        }

        @Override
        public void restoreState(List<byte[]> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.data = state.get(0);

            restoreLatch.countDown();
        }

        // --------------------------------------------------------------------

        static CountDownLatch getProgressLatch() {
            return progressLatch;
        }

        static CountDownLatch getRestoreLatch() {
            return restoreLatch;
        }

        static void resetForTest(int parallelism) {
            progressLatch = new CountDownLatch(parallelism);
            restoreLatch = new CountDownLatch(parallelism);
        }
    }

    private static final int ITER_TEST_PARALLELISM = 1;
    private static OneShotLatch[] iterTestSnapshotWait = new OneShotLatch[ITER_TEST_PARALLELISM];
    private static OneShotLatch[] iterTestRestoreWait = new OneShotLatch[ITER_TEST_PARALLELISM];
    private static int[] iterTestCheckpointVerify = new int[ITER_TEST_PARALLELISM];

    @Test
    public void testSavepointForJobWithIteration() throws Exception {

        for (int i = 0; i < ITER_TEST_PARALLELISM; ++i) {
            iterTestSnapshotWait[i] = new OneShotLatch();
            iterTestRestoreWait[i] = new OneShotLatch();
            iterTestCheckpointVerify[i] = 0;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final IntegerStreamSource source = new IntegerStreamSource();
        IterativeStream<Integer> iteration =
                env.addSource(source)
                        .flatMap(
                                new RichFlatMapFunction<Integer, Integer>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void flatMap(Integer in, Collector<Integer> clctr)
                                            throws Exception {
                                        clctr.collect(in);
                                    }
                                })
                        .setParallelism(ITER_TEST_PARALLELISM)
                        .keyBy(
                                new KeySelector<Integer, Object>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Object getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                })
                        .flatMap(new DuplicateFilter())
                        .setParallelism(ITER_TEST_PARALLELISM)
                        .iterate();

        DataStream<Integer> iterationBody =
                iteration
                        .map(
                                new MapFunction<Integer, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer map(Integer value) throws Exception {
                                        return value;
                                    }
                                })
                        .setParallelism(ITER_TEST_PARALLELISM);

        iteration.closeWith(iterationBody);

        StreamGraph streamGraph = env.getStreamGraph();

        JobGraph jobGraph = streamGraph.getJobGraph();

        Configuration config = getFileBasedCheckpointsConfig();
        config.addAll(jobGraph.getJobConfiguration());
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ZERO);

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(2 * jobGraph.getMaximumParallelism())
                                .build());
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        String savepointPath = null;
        try {
            client.submitJob(jobGraph).get();

            waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);

            for (OneShotLatch latch : iterTestSnapshotWait) {
                latch.await();
            }
            savepointPath =
                    client.triggerSavepoint(
                                    jobGraph.getJobID(), null, SavepointFormatType.CANONICAL)
                            .get();

            client.cancel(jobGraph.getJobID()).get();
            while (!client.getJobStatus(jobGraph.getJobID()).get().isGloballyTerminalState()) {
                Thread.sleep(100);
            }

            jobGraph = streamGraph.getJobGraph();
            jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

            client.submitJob(jobGraph).get();
            for (OneShotLatch latch : iterTestRestoreWait) {
                latch.await();
            }

            client.cancel(jobGraph.getJobID()).get();
            while (!client.getJobStatus(jobGraph.getJobID()).get().isGloballyTerminalState()) {
                Thread.sleep(100);
            }
        } finally {
            if (null != savepointPath) {
                client.disposeSavepoint(savepointPath);
            }
            cluster.after();
        }
    }

    private static final class IntegerStreamSource extends RichSourceFunction<Integer>
            implements ListCheckpointed<Integer> {

        private static final long serialVersionUID = 1L;
        private volatile boolean running;
        private volatile boolean isRestored;
        private int emittedCount;

        public IntegerStreamSource() {
            this.running = true;
            this.isRestored = false;
            this.emittedCount = 0;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {

            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(emittedCount);
                }

                if (emittedCount < 100) {
                    ++emittedCount;
                } else {
                    emittedCount = 0;
                }
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            iterTestCheckpointVerify[getRuntimeContext().getIndexOfThisSubtask()] = emittedCount;
            return Collections.singletonList(emittedCount);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (!state.isEmpty()) {
                this.emittedCount = state.get(0);
            }
            Assert.assertEquals(
                    iterTestCheckpointVerify[getRuntimeContext().getIndexOfThisSubtask()],
                    emittedCount);
            iterTestRestoreWait[getRuntimeContext().getIndexOfThisSubtask()].trigger();
        }
    }

    private static class DuplicateFilter extends RichFlatMapFunction<Integer, Integer> {

        static final ValueStateDescriptor<Boolean> DESCRIPTOR =
                new ValueStateDescriptor<>("seen", Boolean.class, false);
        private static final long serialVersionUID = 1L;
        private ValueState<Boolean> operatorState;

        @Override
        public void open(OpenContext openContext) {
            operatorState = this.getRuntimeContext().getState(DESCRIPTOR);
        }

        @Override
        public void flatMap(Integer value, Collector<Integer> out) throws Exception {
            if (!operatorState.value()) {
                out.collect(value);
                operatorState.update(true);
            }

            if (30 == value) {
                iterTestSnapshotWait[getRuntimeContext().getIndexOfThisSubtask()].trigger();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------

    private static class MiniClusterResourceFactory {
        private final int numTaskManagers;
        private final int numSlotsPerTaskManager;
        private final Configuration config;

        private MiniClusterResourceFactory(
                int numTaskManagers, int numSlotsPerTaskManager, Configuration config) {
            this.numTaskManagers = numTaskManagers;
            this.numSlotsPerTaskManager = numSlotsPerTaskManager;
            this.config = config;
        }

        MiniClusterWithClientResource get() {
            return new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(config)
                            .setNumberTaskManagers(numTaskManagers)
                            .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                            .build());
        }
    }

    private Configuration getFileBasedCheckpointsConfig(final String savepointDir) {
        final Configuration config = new Configuration();
        config.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        return config;
    }

    private Configuration getFileBasedCheckpointsConfig() {
        return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
    }

    private static Matcher<File> hasEntropyInFileStateHandlePaths() {
        return new TypeSafeDiagnosingMatcher<File>() {

            @Override
            protected boolean matchesSafely(
                    final File savepointDir, final Description mismatchDescription) {
                if (savepointDir == null) {
                    mismatchDescription.appendText("savepoint dir must not be null");
                    return false;
                }

                final List<Path> filesWithoutEntropy =
                        listRecursively(
                                savepointDir
                                        .toPath()
                                        .resolve(
                                                EntropyInjectingTestFileSystem
                                                        .ENTROPY_INJECTION_KEY));
                final Path savepointDirWithEntropy =
                        savepointDir.toPath().resolve(EntropyInjectingTestFileSystem.ENTROPY);
                final List<Path> filesWithEntropy = listRecursively(savepointDirWithEntropy);

                if (!filesWithoutEntropy.isEmpty()) {
                    mismatchDescription.appendText(
                            "there are savepoint files with unresolved entropy placeholders");
                    return false;
                }

                if (!Files.exists(savepointDirWithEntropy) || filesWithEntropy.isEmpty()) {
                    mismatchDescription.appendText(
                            "there are no savepoint files with added entropy");
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("all savepoint files should have added entropy");
            }
        };
    }

    private static List<Path> listRecursively(final Path dir) {
        try {
            if (!Files.exists(dir)) {
                return Collections.emptyList();
            } else {
                try (Stream<Path> files = Files.walk(dir, FileVisitOption.FOLLOW_LINKS)) {
                    return files.filter(Files::isRegularFile).collect(Collectors.toList());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** A test file system. It will fail when trying to perform actions on a statically set path. */
    public static class PathFailingFileSystem extends LocalFileSystem {

        public static final String SCHEME = "failPath";

        private static String failingPathRegex;

        public static void resetFailingPath(String regex) {
            failingPathRegex = regex;
        }

        @Override
        public FSDataInputStream open(org.apache.flink.core.fs.Path f, int bufferSize)
                throws IOException {
            failPath(f);
            return super.open(f, bufferSize);
        }

        @Override
        public FSDataInputStream open(org.apache.flink.core.fs.Path f) throws IOException {
            failPath(f);
            return super.open(f);
        }

        @Override
        public FSDataOutputStream create(
                final org.apache.flink.core.fs.Path filePath, final WriteMode overwrite)
                throws IOException {
            failPath(filePath);
            return super.create(filePath, overwrite);
        }

        @Override
        public LocalRecoverableWriter createRecoverableWriter() throws IOException {
            throw new UnsupportedOperationException(
                    "This file system does not support recoverable writers.");
        }

        private void failPath(org.apache.flink.core.fs.Path filePath) throws IOException {
            if (filePath.getPath().matches(failingPathRegex)) {
                throw new IOException("Expected IO exception for path: " + failingPathRegex);
            }
        }

        @Override
        public URI getUri() {
            return URI.create(SCHEME + ":///");
        }
    }
    // ------------------------------------------------------------------------

    /**
     * A factory for {@link
     * org.apache.flink.test.checkpointing.SavepointITCase.PathFailingFileSystem}.
     */
    public static final class PathFailingFileSystemFactory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return PathFailingFileSystem.SCHEME;
        }

        @Override
        public FileSystem create(URI fsUri) throws IOException {
            return new PathFailingFileSystem();
        }
    }
}
