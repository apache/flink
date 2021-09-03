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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
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
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.SerializedValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests that the cached thread pool used by the {@link Task} allows synchronous checkpoints to
 * complete successfully.
 */
public class SynchronousCheckpointITCase {

    // A thread-safe queue to "log" and monitor events happening in the task's methods. Also, used
    // by the test thread
    // to synchronize actions with the task's threads.
    private static LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(10);

    @Test
    public void taskDispatcherThreadPoolAllowsForSynchronousCheckpoints() throws Exception {
        final Task task = createTask(SynchronousCheckpointTestingTask.class);

        try (TaskCleaner ignored = new TaskCleaner(task)) {
            task.startTaskThread();

            assertThat(eventQueue.take(), is(Event.TASK_IS_RUNNING));
            assertTrue(eventQueue.isEmpty());

            assertEquals(ExecutionState.RUNNING, task.getExecutionState());

            task.triggerCheckpointBarrier(
                    42,
                    156865867234L,
                    new CheckpointOptions(
                            CheckpointType.SAVEPOINT_SUSPEND,
                            CheckpointStorageLocationReference.getDefault()));

            assertThat(eventQueue.take(), is(Event.PRE_TRIGGER_CHECKPOINT));
            assertThat(eventQueue.take(), is(Event.POST_TRIGGER_CHECKPOINT));
            assertTrue(eventQueue.isEmpty());

            task.notifyCheckpointComplete(42);

            assertThat(eventQueue.take(), is(Event.PRE_NOTIFY_CHECKPOINT_COMPLETE));
            assertThat(eventQueue.take(), is(Event.POST_NOTIFY_CHECKPOINT_COMPLETE));
            assertTrue(eventQueue.isEmpty());

            assertEquals(ExecutionState.RUNNING, task.getExecutionState());
        }
    }

    /**
     * A {@link StreamTask} which makes sure that the different phases of a synchronous checkpoint
     * are reflected in the {@link SynchronousCheckpointITCase#eventQueue}.
     */
    public static class SynchronousCheckpointTestingTask extends StreamTask {
        // Flag to emit the first event only once.
        private boolean isRunning;

        public SynchronousCheckpointTestingTask(Environment environment) throws Exception {
            super(environment);
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            if (!isRunning) {
                isRunning = true;
                eventQueue.put(Event.TASK_IS_RUNNING);
            }
            if (isCanceled()) {
                controller.suspendDefaultAction();
                mailboxProcessor.suspend();
            } else {
                controller.suspendDefaultAction();
            }
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            try {
                eventQueue.put(Event.PRE_TRIGGER_CHECKPOINT);
                CompletableFuture<Boolean> result =
                        super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
                eventQueue.put(Event.POST_TRIGGER_CHECKPOINT);
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            try {
                eventQueue.put(Event.PRE_NOTIFY_CHECKPOINT_COMPLETE);
                Future<Void> result = super.notifyCheckpointCompleteAsync(checkpointId);
                eventQueue.put(Event.POST_NOTIFY_CHECKPOINT_COMPLETE);
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public Future<Void> notifyCheckpointAbortAsync(
                long checkpointId, long latestCompletedCheckpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected void init() {}

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
        protected void cleanUpInternal() {}
    }

    /**
     * The different state transitions during a synchronous checkpoint along with their expected
     * previous state.
     */
    private enum Event {
        TASK_IS_RUNNING,
        PRE_TRIGGER_CHECKPOINT,
        PRE_NOTIFY_CHECKPOINT_COMPLETE,
        POST_NOTIFY_CHECKPOINT_COMPLETE,
        POST_TRIGGER_CHECKPOINT,
    }

    // --------------------------		Boilerplate tools copied from the TaskAsyncCallTest
    //	--------------------------

    private Task createTask(Class<? extends TaskInvokable> invokableClass) throws Exception {

        ResultPartitionConsumableNotifier consumableNotifier =
                new NoOpResultPartitionConsumableNotifier();
        PartitionProducerStateChecker partitionProducerStateChecker =
                mock(PartitionProducerStateChecker.class);
        Executor executor = mock(Executor.class);
        ShuffleEnvironment<?, ?> shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

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
                TestingClassLoaderLease.newBuilder().build(),
                mock(FileCache.class),
                new TestingTaskManagerRuntimeInfo(),
                taskMetricGroup,
                consumableNotifier,
                partitionProducerStateChecker,
                executor);
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
