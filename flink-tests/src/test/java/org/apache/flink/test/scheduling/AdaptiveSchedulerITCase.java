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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
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
        conf.set(ClusterOptions.ENABLE_DECLARATIVE_RESOURCE_MANAGEMENT, true);

        return conf;
    }

    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER_WITH_CLIENT_RESOURCE =
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
        MINI_CLUSTER_WITH_CLIENT_RESOURCE.cancelAllJobs();
    }

    /** Tests that the adaptive scheduler can recover stateful operators. */
    @Test
    public void testGlobalFailoverCanRecoverState() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        env.enableCheckpointing(20L, CheckpointingMode.EXACTLY_ONCE);
        final DataStreamSource<Integer> input = env.addSource(new SimpleSource());

        input.addSink(new DiscardingSink<>());

        env.execute();
    }

    private enum StopWithSavepointTestBehavior {
        NO_FAILURE,
        FAIL_ON_CHECKPOINT,
        FAIL_ON_STOP,
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
                client.stopWithSavepoint(false, savepointDirectory.getAbsolutePath()).get();
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
            client.stopWithSavepoint(false, tempFolder.newFolder("savepoint").getAbsolutePath())
                    .get();
            fail("Expect exception");
        } catch (ExecutionException e) {
            assertThat(e, containsCause(FlinkException.class));
        }
        // expect job to run again (maybe restart)
        CommonTestUtils.waitUntilCondition(
                () -> client.getJobStatus().get() == JobStatus.RUNNING,
                Deadline.fromNow(Duration.of(1, ChronoUnit.MINUTES)));
    }

    @Test
    public void testStopWithSavepointFailOnStop() throws Exception {
        StreamExecutionEnvironment env =
                getEnvWithSource(StopWithSavepointTestBehavior.FAIL_ON_STOP);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

        DummySource.resetForParallelism(PARALLELISM);

        JobClient client = env.executeAsync();

        DummySource.awaitRunning();
        try {
            client.stopWithSavepoint(false, tempFolder.newFolder("savepoint").getAbsolutePath())
                    .get();
            fail("Expect exception");
        } catch (ExecutionException e) {
            assertThat(e, containsCause(FlinkException.class));
        }
        // expect job to run again (maybe restart)
        CommonTestUtils.waitUntilCondition(
                () -> client.getJobStatus().get() == JobStatus.RUNNING,
                Deadline.fromNow(Duration.of(1, ChronoUnit.MINUTES)));
    }

    @Test
    public void testStopWithSavepointFailOnFirstSavepointSucceedOnSecond() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        env.setParallelism(PARALLELISM);

        env.addSource(new DummySource(StopWithSavepointTestBehavior.FAIL_ON_FIRST_CHECKPOINT_ONLY))
                .addSink(new DiscardingSink<>());
        DummySource.resetForParallelism(PARALLELISM);

        JobClient client = env.executeAsync();

        DummySource.awaitRunning();
        DummySource.resetForParallelism(PARALLELISM);
        final File savepointDirectory = tempFolder.newFolder("savepoint");
        try {
            client.stopWithSavepoint(false, savepointDirectory.getAbsolutePath()).get();
            fail("Expect failure of operation");
        } catch (ExecutionException e) {
            assertThat(e, containsCause(FlinkException.class));
        }

        DummySource.awaitRunning();

        // ensure failed savepoint files have been removed from the directory.
        // We execute this in a retry loop with a timeout, because the savepoint deletion happens
        // asynchronously and is not bound to the job lifecycle. See FLINK-22493 for more details.
        CommonTestUtils.waitUntilCondition(
                () -> isDirectoryEmpty(savepointDirectory),
                Deadline.fromNow(Duration.ofSeconds(10)));

        // trigger second savepoint
        final String savepoint =
                client.stopWithSavepoint(false, savepointDirectory.getAbsolutePath()).get();
        assertThat(savepoint, containsString(savepointDirectory.getAbsolutePath()));
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
        env.addSource(new DummySource(behavior)).addSink(new DiscardingSink<>());
        return env;
    }

    private static final class DummySource extends RichParallelSourceFunction<Integer>
            implements CheckpointedFunction, CheckpointListener {
        private final StopWithSavepointTestBehavior behavior;
        private volatile boolean running = true;
        private static volatile CountDownLatch instancesRunning;
        private volatile boolean checkpointComplete = false;

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
            if (checkpointComplete && behavior == StopWithSavepointTestBehavior.FAIL_ON_STOP) {
                throw new RuntimeException(behavior.name());
            }
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
            checkpointComplete = true;
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
                    ctx.collect(getRuntimeContext().getIndexOfThisSubtask());

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

            unionListState.clear();
            unionListState.add(true);
        }
    }
}
