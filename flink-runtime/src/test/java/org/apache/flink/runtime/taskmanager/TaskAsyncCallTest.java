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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/** Testing asynchronous call of {@link Task}. */
public class TaskAsyncCallTest extends TestLogger {

    /** Number of expected checkpoints. */
    private static int numCalls;

    /** Triggered at the beginning of {@link CheckpointsInOrderInvokable#invoke()}. */
    private static OneShotLatch awaitLatch;

    /**
     * Triggered when {@link CheckpointsInOrderInvokable#triggerCheckpointAsync(CheckpointMetaData,
     * CheckpointOptions)} was called {@link #numCalls} times.
     */
    private static OneShotLatch triggerLatch;

    private ShuffleEnvironment<?, ?> shuffleEnvironment;

    @Before
    public void createQueuesAndActors() {
        numCalls = 1000;

        awaitLatch = new OneShotLatch();
        triggerLatch = new OneShotLatch();

        shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
    }

    @After
    public void teardown() throws Exception {
        if (shuffleEnvironment != null) {
            shuffleEnvironment.close();
        }
    }

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    @Test
    public void testCheckpointCallsInOrder() throws Exception {

        Task task = createTask(CheckpointsInOrderInvokable.class);
        try (TaskCleaner ignored = new TaskCleaner(task)) {
            task.startTaskThread();

            awaitLatch.await();

            for (int i = 1; i <= numCalls; i++) {
                task.triggerCheckpointBarrier(
                        i, 156865867234L, CheckpointOptions.forCheckpointWithDefaultLocation());
            }

            triggerLatch.await();

            assertFalse(task.isCanceledOrFailed());

            ExecutionState currentState = task.getExecutionState();
            assertThat(currentState, isOneOf(ExecutionState.RUNNING, ExecutionState.FINISHED));
        }
    }

    @Test
    public void testMixedAsyncCallsInOrder() throws Exception {

        Task task = createTask(CheckpointsInOrderInvokable.class);
        try (TaskCleaner ignored = new TaskCleaner(task)) {
            task.startTaskThread();

            awaitLatch.await();

            for (int i = 1; i <= numCalls; i++) {
                task.triggerCheckpointBarrier(
                        i, 156865867234L, CheckpointOptions.forCheckpointWithDefaultLocation());
                task.notifyCheckpointComplete(i);
            }

            triggerLatch.await();

            assertFalse(task.isCanceledOrFailed());

            ExecutionState currentState = task.getExecutionState();
            assertThat(currentState, isOneOf(ExecutionState.RUNNING, ExecutionState.FINISHED));
        }
    }

    private Task createTask(Class<? extends AbstractInvokable> invokableClass) throws Exception {
        final TestingClassLoaderLease classLoaderHandle =
                TestingClassLoaderLease.newBuilder()
                        .setGetOrResolveClassLoaderFunction(
                                (permanentBlobKeys, urls) ->
                                        TestingUserCodeClassLoader.newBuilder()
                                                .setClassLoader(new TestUserCodeClassLoader())
                                                .build())
                        .build();

        ResultPartitionConsumableNotifier consumableNotifier =
                new NoOpResultPartitionConsumableNotifier();
        PartitionProducerStateChecker partitionProducerStateChecker =
                mock(PartitionProducerStateChecker.class);
        Executor executor = mock(Executor.class);
        TaskMetricGroup taskMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();

        JobInformation jobInformation =
                new JobInformation(
                        new JobID(),
                        "Job Name",
                        new SerializedValue<>(new ExecutionConfig()),
                        new Configuration(),
                        Collections.emptyList(),
                        Collections.emptyList());

        TaskInformation taskInformation =
                new TaskInformation(
                        new JobVertexID(),
                        "Test Task",
                        1,
                        1,
                        invokableClass.getName(),
                        new Configuration());

        return new Task(
                jobInformation,
                taskInformation,
                new ExecutionAttemptID(),
                new AllocationID(),
                0,
                0,
                Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
                Collections.<InputGateDeploymentDescriptor>emptyList(),
                mock(MemoryManager.class),
                mock(IOManager.class),
                shuffleEnvironment,
                new KvStateService(new KvStateRegistry(), null, null),
                mock(BroadcastVariableManager.class),
                new TaskEventDispatcher(),
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                new TestTaskStateManager(),
                mock(TaskManagerActions.class),
                mock(InputSplitProvider.class),
                mock(CheckpointResponder.class),
                new NoOpTaskOperatorEventGateway(),
                new TestGlobalAggregateManager(),
                classLoaderHandle,
                mock(FileCache.class),
                new TestingTaskManagerRuntimeInfo(),
                taskMetricGroup,
                consumableNotifier,
                partitionProducerStateChecker,
                executor);
    }

    /** Invokable for testing checkpoints. */
    public static class CheckpointsInOrderInvokable extends AbstractInvokable {

        private volatile long lastCheckpointId = 0;

        private volatile Exception error;

        public CheckpointsInOrderInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            awaitLatch.trigger();

            // wait forever (until canceled)
            synchronized (this) {
                while (error == null) {
                    wait();
                }
            }

            if (error != null) {
                // exit method prematurely due to error but make sure that the tests can finish
                triggerLatch.trigger();

                throw error;
            }
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            lastCheckpointId++;
            if (checkpointMetaData.getCheckpointId() == lastCheckpointId) {
                if (lastCheckpointId == numCalls) {
                    triggerLatch.trigger();
                }
            } else if (this.error == null) {
                this.error = new Exception("calls out of order");
                synchronized (this) {
                    notifyAll();
                }
            }
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void triggerCheckpointOnBarrier(
                CheckpointMetaData checkpointMetaData,
                CheckpointOptions checkpointOptions,
                CheckpointMetricsBuilder checkpointMetrics) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            if (checkpointId != lastCheckpointId && this.error == null) {
                this.error = new Exception("calls out of order");
                synchronized (this) {
                    notifyAll();
                }
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * A {@link ClassLoader} that delegates everything to {@link
     * ClassLoader#getSystemClassLoader()}.
     */
    private static class TestUserCodeClassLoader extends ClassLoader {
        TestUserCodeClassLoader() {
            super(ClassLoader.getSystemClassLoader());
        }
    }

    private static class TaskCleaner implements AutoCloseable {

        private final Task task;

        private TaskCleaner(Task task) {
            this.task = task;
        }

        @Override
        public void close() throws Exception {
            task.cancelExecution();
            task.getExecutingThread().join(5000);
        }
    }
}
