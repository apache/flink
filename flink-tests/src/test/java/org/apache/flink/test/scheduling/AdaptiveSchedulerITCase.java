/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory.RootExceptionInfo;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.util.ExceptionUtils.assertThrowable;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/** Integration tests for the adaptive scheduler. */
public class AdaptiveSchedulerITCase extends TestLogger {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;

    private static final Configuration configuration = getConfiguration();

    private static Configuration getConfiguration() {
        final Configuration conf = new Configuration();
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        conf.set(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 1_000L);
        conf.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 5_000L);
        return conf;
    }

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_WITH_CLIENT_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .build());

    @Before
    public void ensureAdaptiveSchedulerEnabled() {
        assumeTrue(ClusterOptions.isAdaptiveSchedulerEnabled(configuration));
    }

    @After
    public void cancelRunningJobs() {
        MINI_CLUSTER_WITH_CLIENT_RESOURCE.cancelAllJobsAndWaitUntilSlotsAreFreed();
    }

    /** Tests that the adaptive scheduler can recover stateful operators. */
    @Test
    public void testGlobalFailoverCanRecoverState() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        env.enableCheckpointing(20L, CheckpointingMode.EXACTLY_ONCE);
        final DataStreamSource<Integer> input = env.addSource(new SimpleSource());

        // TODO replace this by sink v2 after source is ported to FLIP-27.
        input.addSink(new DiscardingSink<>());

        env.execute();
    }

    private enum StopWithSavepointTestBehavior {
        NO_FAILURE,
        FAIL_ON_CHECKPOINT,
        FAIL_ON_CHECKPOINT_COMPLETE,
        FAIL_ON_FIRST_CHECKPOINT_ONLY
    }

    @Test
    public void testStopWithSavepointNoError() throws Exception {
        StreamExecutionEnvironment env = getEnvWithSource(StopWithSavepointTestBehavior.NO_FAILURE);

        DummySource.resetForParallelism(PARALLELISM);

        JobClient client = env.executeAsync();

        DummySource.awaitRunning();

        final File savepointDirectory = tempFolder.newFolder("savepoint");
        final String savepoint =
                client.stopWithSavepoint(
                                false,
                                savepointDirectory.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();
        assertThat(savepoint, containsString(savepointDirectory.getAbsolutePath()));
        assertThat(client.getJobStatus().get(), is(JobStatus.FINISHED));
    }

    @Test
    public void testStopWithSavepointFailOnCheckpoint() throws Exception {
        StreamExecutionEnvironment env =
                getEnvWithSource(StopWithSavepointTestBehavior.FAIL_ON_CHECKPOINT);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

        DummySource.resetForParallelism(PARALLELISM);

        JobClient client = env.executeAsync();

        DummySource.awaitRunning();
        try {
            client.stopWithSavepoint(
                            false,
                            tempFolder.newFolder("savepoint").getAbsolutePath(),
                            SavepointFormatType.CANONICAL)
                    .get();
            fail("Expect exception");
        } catch (ExecutionException e) {
            assertThat(e, containsCause(FlinkException.class));
        }
        // expect job to run again (maybe restart)
        CommonTestUtils.waitUntilCondition(() -> client.getJobStatus().get() == JobStatus.RUNNING);
    }

    @Test
    public void testStopWithSavepointFailOnStop() throws Throwable {
        StreamExecutionEnvironment env =
                getEnvWithSource(StopWithSavepointTestBehavior.FAIL_ON_CHECKPOINT_COMPLETE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

        DummySource.resetForParallelism(PARALLELISM);

        JobClient client = env.executeAsync();

        DummySource.awaitRunning();
        final CompletableFuture<String> savepointCompleted =
                client.stopWithSavepoint(
                        false,
                        tempFolder.newFolder("savepoint").getAbsolutePath(),
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
                client.getJobStatus().get(),
                either(is(JobStatus.FAILED)).or(is(JobStatus.FAILING)));
    }

    @Test
    public void testStopWithSavepointFailOnFirstSavepointSucceedOnSecond() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        env.setParallelism(PARALLELISM);

        env.addSource(new DummySource(StopWithSavepointTestBehavior.FAIL_ON_FIRST_CHECKPOINT_ONLY))
                // TODO replace this by sink v2 after source is ported to FLIP-27.
                .addSink(new DiscardingSink<>());
        DummySource.resetForParallelism(PARALLELISM);

        JobClient client = env.executeAsync();

        DummySource.awaitRunning();
        DummySource.resetForParallelism(PARALLELISM);
        final File savepointDirectory = tempFolder.newFolder("savepoint");
        try {
            client.stopWithSavepoint(
                            false,
                            savepointDirectory.getAbsolutePath(),
                            SavepointFormatType.CANONICAL)
                    .get();
            fail("Expect failure of operation");
        } catch (ExecutionException e) {
            assertThat(e, containsCause(FlinkException.class));
        }

        DummySource.awaitRunning();

        // ensure failed savepoint files have been removed from the directory.
        // We execute this in a retry loop with a timeout, because the savepoint deletion happens
        // asynchronously and is not bound to the job lifecycle. See FLINK-22493 for more details.
        CommonTestUtils.waitUntilCondition(() -> isDirectoryEmpty(savepointDirectory));

        // trigger second savepoint
        final String savepoint =
                client.stopWithSavepoint(
                                false,
                                savepointDirectory.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();
        assertThat(savepoint, containsString(savepointDirectory.getAbsolutePath()));
    }

    @Test
    public void testExceptionHistoryIsRetrievableFromTheRestAPI() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(20L, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new FailOnCompletedCheckpointSource())
                // TODO replace this by sink v2 after source is ported to FLIP-27.
                .addSink(new DiscardingSink<>());
        final JobClient jobClient = env.executeAsync();
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final List<RootExceptionInfo> exceptions =
                            getJobExceptions(
                                            jobClient.getJobID(), MINI_CLUSTER_WITH_CLIENT_RESOURCE)
                                    .get()
                                    .getExceptionHistory()
                                    .getEntries();
                    return !exceptions.isEmpty();
                });
        jobClient.cancel().get();
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.CANCELED));
    }

    @Test
    public void testGlobalFailureOnRestart() throws Exception {
        final MiniCluster miniCluster = MINI_CLUSTER_WITH_CLIENT_RESOURCE.getMiniCluster();

        final JobVertexID jobVertexId = new JobVertexID();
        final JobVertex jobVertex = new JobVertex("jobVertex", jobVertexId);
        jobVertex.setInvokableClass(FailingInvokable.class);
        jobVertex.addOperatorCoordinator(
                new SerializedValue<>(
                        new FailingCoordinatorProvider(OperatorID.fromJobVertexID(jobVertexId))));
        jobVertex.setParallelism(1);

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.hours(1)));

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Collections.singletonList(jobVertex))
                        .setExecutionConfig(executionConfig)
                        .build();
        miniCluster.submitJob(jobGraph).join();

        // We rely on waiting in restarting state (see the restart strategy above)
        CommonTestUtils.waitUntilCondition(
                () -> miniCluster.getJobStatus(jobGraph.getJobID()).join() == JobStatus.RESTARTING);
        FailingCoordinatorProvider.JOB_RESTARTING.countDown();

        // Make sure this doesn't throw an assertion
        getJobExceptions(jobGraph.getJobID(), MINI_CLUSTER_WITH_CLIENT_RESOURCE).get();

        miniCluster.cancelJob(jobGraph.getJobID());
        CommonTestUtils.waitUntilCondition(
                () -> miniCluster.getJobStatus(jobGraph.getJobID()).join() == JobStatus.CANCELED);

        final JobExceptionsInfoWithHistory jobExceptions =
                getJobExceptions(jobGraph.getJobID(), MINI_CLUSTER_WITH_CLIENT_RESOURCE).get();
        // Global failure is ignored while Restarting so only 1
        assertThat(jobExceptions.getExceptionHistory().getEntries().size(), is(1));

        // Make sure history contains only the Local failure
        String allExceptions =
                jobExceptions.getExceptionHistory().getEntries().stream()
                        .map(e -> e.getStacktrace())
                        .collect(Collectors.joining());
        assertThat(
                allExceptions, not(containsString(FailingCoordinatorProvider.globalExceptionMsg)));
        assertThat(allExceptions, containsString(FailingInvokable.localExceptionMsg));
    }

    private boolean isDirectoryEmpty(File directory) {
        File[] files = directory.listFiles();
        if (files.length > 0) {
            log.warn(
                    "There are still unexpected files: {}",
                    Arrays.stream(files)
                            .map(File::getAbsolutePath)
                            .collect(Collectors.joining(", ")));
            return false;
        }
        return true;
    }

    private static StreamExecutionEnvironment getEnvWithSource(
            StopWithSavepointTestBehavior behavior) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.addSource(new DummySource(behavior))
                // TODO replace this by sink v2 after source is ported to FLIP-27.
                .addSink(new DiscardingSink<>());
        return env;
    }

    private static CompletableFuture<JobExceptionsInfoWithHistory> getJobExceptions(
            JobID jobId, MiniClusterWithClientResource minClusterRes) throws Exception {
        final RestClusterClient<?> restClusterClient = minClusterRes.getRestClusterClient();
        final JobExceptionsMessageParameters params = new JobExceptionsMessageParameters();
        params.jobPathParameter.resolve(jobId);
        return restClusterClient.sendRequest(
                JobExceptionsHeaders.getInstance(), params, EmptyRequestBody.getInstance());
    }

    /** Simple invokable which fails immediately after being invoked. */
    public static class FailingInvokable extends AbstractInvokable {
        private static final String localExceptionMsg = "Local exception.";

        public FailingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            throw new Exception(localExceptionMsg);
        }
    }

    private static class FailingCoordinatorProvider implements OperatorCoordinator.Provider {

        private static final CountDownLatch JOB_RESTARTING = new CountDownLatch(1);

        private final OperatorID operatorId;
        private static final String globalExceptionMsg = "Global exception.";

        FailingCoordinatorProvider(OperatorID operatorId) {
            this.operatorId = operatorId;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) {
            return new OperatorCoordinator() {

                @Nullable private Thread thread;

                @Override
                public void start() {
                    thread =
                            new Thread(
                                    () -> {
                                        try {
                                            JOB_RESTARTING.await();
                                            context.failJob(new Exception(globalExceptionMsg));
                                        } catch (InterruptedException e) {
                                            // Ignored.
                                        }
                                    });
                    thread.setName("failing-coordinator");
                    thread.setDaemon(true);
                    thread.start();
                }

                @Override
                public void close() throws Exception {
                    if (thread != null) {
                        thread.interrupt();
                        thread.join();
                    }
                }

                @Override
                public void handleEventFromOperator(
                        int subtask, int attemptNumber, OperatorEvent event) {}

                @Override
                public void checkpointCoordinator(
                        long checkpointId, CompletableFuture<byte[]> resultFuture) {}

                @Override
                public void notifyCheckpointComplete(long checkpointId) {}

                @Override
                public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {}

                @Override
                public void subtaskReset(int subtask, long checkpointId) {}

                @Override
                public void executionAttemptFailed(
                        int subtask, int attemptNumber, @Nullable Throwable reason) {}

                @Override
                public void executionAttemptReady(
                        int subtask, int attemptNumber, SubtaskGateway gateway) {}
            };
        }
    }

    private static final class DummySource extends RichParallelSourceFunction<Integer>
            implements CheckpointedFunction, CheckpointListener {
        private final StopWithSavepointTestBehavior behavior;
        private volatile boolean running = true;
        private static volatile CountDownLatch instancesRunning;

        public DummySource(StopWithSavepointTestBehavior behavior) {
            this.behavior = behavior;
        }

        private static void resetForParallelism(int para) {
            instancesRunning = new CountDownLatch(para);
        }

        private static void awaitRunning() throws InterruptedException {
            Preconditions.checkNotNull(instancesRunning);
            instancesRunning.await();
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            Preconditions.checkNotNull(instancesRunning);
            instancesRunning.countDown();
            int i = Integer.MIN_VALUE;
            while (running) {
                Thread.sleep(10L);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(i++);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (behavior == StopWithSavepointTestBehavior.FAIL_ON_CHECKPOINT) {
                throw new RuntimeException(behavior.name());
            }
            if (behavior == StopWithSavepointTestBehavior.FAIL_ON_FIRST_CHECKPOINT_ONLY
                    && context.getCheckpointId() == 1L) {
                throw new RuntimeException(behavior.name());
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (behavior == StopWithSavepointTestBehavior.FAIL_ON_CHECKPOINT_COMPLETE) {
                throw new RuntimeException(behavior.name());
            }
        }
    }

    /**
     * Simple source which fails once after a successful checkpoint has been taken. Upon recovery
     * the source will immediately terminate.
     */
    public static final class SimpleSource extends RichParallelSourceFunction<Integer>
            implements CheckpointListener, CheckpointedFunction {

        private static final ListStateDescriptor<Boolean> unionStateListDescriptor =
                new ListStateDescriptor<>("state", Boolean.class);

        private volatile boolean running = true;

        @Nullable private ListState<Boolean> unionListState = null;

        private boolean hasFailedBefore = false;

        private boolean fail = false;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running && !hasFailedBefore) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());

                    Thread.sleep(5L);
                }

                if (fail) {
                    throw new FlinkException("Test failure.");
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            fail = true;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            unionListState =
                    context.getOperatorStateStore().getUnionListState(unionStateListDescriptor);

            for (Boolean previousState : unionListState.get()) {
                hasFailedBefore |= previousState;
            }

            unionListState.update(Collections.singletonList(true));
        }
    }

    /** Simple source which fails every time checkpoint is completed. */
    public static final class FailOnCompletedCheckpointSource
            extends RichParallelSourceFunction<Integer> implements CheckpointListener {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
                    Thread.sleep(5L);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            throw new RuntimeException("Test exception.");
        }
    }
}
