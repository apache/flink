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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteChannelStateChecker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the Task, which make sure that correct state transitions happen, and failures are
 * correctly handled.
 */
class TaskTest extends TestLogger {
    private static final String RESTORE_EXCEPTION_MSG = "TestExceptionInRestore";

    private ShuffleEnvironment<?, ?> shuffleEnvironment;

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static boolean wasCleanedUp = false;

    @BeforeEach
    void setup() {
        shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
        wasCleanedUp = false;
    }

    @AfterEach
    void teardown() throws Exception {
        if (shuffleEnvironment != null) {
            shuffleEnvironment.close();
        }
    }

    @Test
    void testCleanupWhenRestoreFails() throws Exception {
        createTaskBuilder()
                .setInvokable(InvokableWithExceptionInRestore.class)
                .build(Executors.directExecutor())
                .run();
        assertThat(wasCleanedUp).isTrue();
    }

    @Test
    void testCleanupWhenInvokeFails() throws Exception {
        createTaskBuilder()
                .setInvokable(InvokableWithExceptionInInvoke.class)
                .build(Executors.directExecutor())
                .run();
        assertThat(wasCleanedUp).isTrue();
    }

    @Test
    void testCleanupWhenCancelledAfterRestore() throws Exception {
        Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInRestore.class)
                        .build(Executors.directExecutor());
        task.startTaskThread();
        awaitInvokableLatch(task);
        task.cancelExecution();
        task.getExecutingThread().join();
        assertThat(wasCleanedUp).isTrue();
    }

    @Test
    void testCleanupWhenAfterInvokeSucceeded() throws Exception {
        Task task =
                createTaskBuilder()
                        .setInvokable(TestInvokableCorrect.class)
                        .build(Executors.directExecutor());
        task.run();
        assertThat(wasCleanedUp).isTrue();
        assertThat(task.isCanceledOrFailed()).isFalse();
    }

    @Test
    void testCleanupWhenSwitchToInitializationFails() throws Exception {
        Task task =
                createTaskBuilder()
                        .setInvokable(TestInvokableCorrect.class)
                        .setTaskManagerActions(
                                new NoOpTaskManagerActions() {
                                    @Override
                                    public void updateTaskExecutionState(
                                            TaskExecutionState taskExecutionState) {
                                        if (taskExecutionState.getExecutionState()
                                                == ExecutionState.INITIALIZING) {
                                            throw new ExpectedTestException();
                                        }
                                    }
                                })
                        .build(Executors.directExecutor());
        task.run();
        assertThat(wasCleanedUp).isTrue();
        assertThat(task.isCanceledOrFailed()).isTrue();
    }

    @Test
    void testRegularExecution() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(TestInvokableCorrect.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // task should be new and perfect
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CREATED);
        assertThat(task.isCanceledOrFailed()).isFalse();
        assertThat(task.getFailureCause()).isNull();

        // go into the run method. we should switch to DEPLOYING, RUNNING, then
        // FINISHED, and all should be good
        task.run();

        // verify final state
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FINISHED);
        assertThat(task.isCanceledOrFailed()).isFalse();
        assertThat(task.getFailureCause()).isNull();
        assertThat(task.getInvokable()).isNull();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.FINISHED, task, null);
    }

    @Test
    void testCancelRightAway() throws Exception {
        final Task task = createTaskBuilder().build(Executors.directExecutor());
        task.cancelExecution();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELING);

        task.run();

        // verify final state
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELED);

        assertThat(task.getInvokable()).isNull();
    }

    @Test
    void testFailExternallyRightAway() throws Exception {
        final Task task = createTaskBuilder().build(Executors.directExecutor());
        task.failExternally(new Exception("fail externally"));

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);

        task.run();

        // verify final state
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
    }

    @Test
    void testLibraryCacheRegistrationFailed() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final IOException testException = new IOException("Could not load classloader");
        final Task task =
                createTaskBuilder()
                        .setTaskManagerActions(taskManagerActions)
                        .setClassLoaderHandle(
                                TestingClassLoaderLease.newBuilder()
                                        .setGetOrResolveClassLoaderFunction(
                                                (permanentBlobKeys, urls) -> {
                                                    throw testException;
                                                })
                                        .build())
                        .build(Executors.directExecutor());

        // task should be new and perfect
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CREATED);
        assertThat(task.isCanceledOrFailed()).isFalse();
        assertThat(task.getFailureCause()).isNull();

        // should fail
        task.run();

        // verify final state
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause()).isEqualTo(testException);

        assertThat(task.getInvokable()).isNull();

        taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, testException);
    }

    @Test
    void testExecutionFailsInNetworkRegistrationForPartitions() throws Exception {
        final PartitionDescriptor partitionDescriptor =
                PartitionDescriptorBuilder.newBuilder().build();
        final ShuffleDescriptor shuffleDescriptor =
                NettyShuffleDescriptorBuilder.newBuilder().buildLocal();
        final ResultPartitionDeploymentDescriptor dummyPartition =
                new ResultPartitionDeploymentDescriptor(partitionDescriptor, shuffleDescriptor, 1);
        testExecutionFailsInNetworkRegistration(
                Collections.singletonList(dummyPartition), Collections.emptyList());
    }

    @Test
    void testExecutionFailsInNetworkRegistrationForGates() throws Exception {
        final ShuffleDescriptor dummyChannel =
                NettyShuffleDescriptorBuilder.newBuilder().buildRemote();
        final InputGateDeploymentDescriptor dummyGate =
                new InputGateDeploymentDescriptor(
                        new IntermediateDataSetID(),
                        ResultPartitionType.PIPELINED,
                        0,
                        new ShuffleDescriptorAndIndex[] {
                            new ShuffleDescriptorAndIndex(dummyChannel, 0)
                        });
        testExecutionFailsInNetworkRegistration(
                Collections.emptyList(), Collections.singletonList(dummyGate));
    }

    private void testExecutionFailsInNetworkRegistration(
            List<ResultPartitionDeploymentDescriptor> resultPartitions,
            List<InputGateDeploymentDescriptor> inputGates)
            throws Exception {
        final String errorMessage = "Network buffer pool has already been destroyed.";

        final PartitionProducerStateChecker partitionProducerStateChecker =
                mock(PartitionProducerStateChecker.class);

        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                new TestTaskBuilder(shuffleEnvironment)
                        .setTaskManagerActions(taskManagerActions)
                        .setPartitionProducerStateChecker(partitionProducerStateChecker)
                        .setResultPartitions(resultPartitions)
                        .setInputGates(inputGates)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        // shut down the network to make the following task registration failure
        shuffleEnvironment.close();

        // should fail
        task.run();

        // verify final state
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage().contains(errorMessage)).isTrue();

        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new IllegalStateException(errorMessage));
    }

    @Test
    void testInvokableInstantiationFailed() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setTaskManagerActions(taskManagerActions)
                        .setInvokable(InvokableNonInstantiable.class)
                        .build(Executors.directExecutor());

        // should fail
        task.run();

        // verify final state
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage().contains("instantiate")).isTrue();

        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED,
                task,
                new FlinkException("Could not instantiate the task's invokable class."));
    }

    @Test
    void testExecutionFailsInRestore() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithExceptionInRestore.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        task.run();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause()).isNotNull();
        assertThat(task.getFailureCause().getMessage()).isNotNull();
        assertThat(task.getFailureCause().getMessage()).contains(RESTORE_EXCEPTION_MSG);

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new Exception(RESTORE_EXCEPTION_MSG));
    }

    @Test
    void testExecutionFailsInInvoke() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithExceptionInInvoke.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        task.run();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause()).isNotNull();
        assertThat(task.getFailureCause().getMessage()).isNotNull();
        assertThat(task.getFailureCause().getMessage().contains("test")).isTrue();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new Exception("test"));
    }

    @Test
    void testFailWithWrappedException() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(FailingInvokableWithChainedException.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        task.run();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();

        final Throwable cause = task.getFailureCause();
        assertThat(cause instanceof IOException).isTrue();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new IOException("test"));
    }

    @Test
    void testCancelDuringRestore() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInRestore.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in restore
        awaitInvokableLatch(task);

        task.cancelExecution();
        assertThat(task.getExecutionState())
                .isIn(ExecutionState.CANCELING, ExecutionState.CANCELED);

        task.getExecutingThread().join();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause()).isNull();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.CANCELED, task, null);
    }

    @Test
    void testCancelDuringInvoke() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInInvoke.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in invoke
        awaitInvokableLatch(task);

        task.cancelExecution();

        assertThat(task.getExecutionState())
                .isIn(ExecutionState.CANCELING, ExecutionState.CANCELED);

        task.getExecutingThread().join();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause()).isNull();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.CANCELED, task, null);
    }

    @Test
    void testFailExternallyDuringRestore() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInRestore.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in invoke
        awaitInvokableLatch(task);

        task.failExternally(new Exception(RESTORE_EXCEPTION_MSG));

        task.getExecutingThread().join();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage()).contains(RESTORE_EXCEPTION_MSG);

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new Exception(RESTORE_EXCEPTION_MSG));
    }

    @Test
    void testFailExternallyDuringInvoke() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInInvoke.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in invoke
        awaitInvokableLatch(task);

        task.failExternally(new Exception("test"));

        task.getExecutingThread().join();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage().contains("test")).isTrue();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new Exception("test"));
    }

    @Test
    void testCanceledAfterExecutionFailedInInvoke() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithExceptionInInvoke.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        task.run();

        // this should not overwrite the failure state
        task.cancelExecution();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage().contains("test")).isTrue();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new Exception("test"));
    }

    @Test
    void testExecutionFailsAfterCanceling() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithExceptionOnTrigger.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in invoke
        awaitInvokableLatch(task);

        task.cancelExecution();
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELING);

        // this causes an exception
        triggerInvokableLatch(task);

        task.getExecutingThread().join();

        // we should still be in state canceled
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause()).isNull();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.CANCELED, task, null);
    }

    @Test
    void testExecutionFailsAfterTaskMarkedFailed() throws Exception {
        final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithExceptionOnTrigger.class)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in invoke
        awaitInvokableLatch(task);

        task.failExternally(new Exception("external"));
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);

        // this causes an exception
        triggerInvokableLatch(task);

        task.getExecutingThread().join();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage().contains("external")).isTrue();

        taskManagerActions.validateListenerMessage(ExecutionState.INITIALIZING, task, null);
        taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
        taskManagerActions.validateListenerMessage(
                ExecutionState.FAILED, task, new Exception("external"));
    }

    @Test
    void testCancelTaskException() throws Exception {
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithCancelTaskExceptionInInvoke.class)
                        .build(Executors.directExecutor());

        task.startTaskThread();

        // Cause CancelTaskException.
        triggerInvokableLatch(task);

        task.getExecutingThread().join();
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELED);
    }

    @Test
    void testCancelTaskExceptionAfterTaskMarkedFailed() throws Exception {
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithCancelTaskExceptionInInvoke.class)
                        .build(Executors.directExecutor());

        task.startTaskThread();

        // Wait till the task is in invoke.
        awaitInvokableLatch(task);

        task.failExternally(new Exception("external"));
        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);

        // Either we cause the CancelTaskException or the TaskCanceler
        // by interrupting the invokable.
        triggerInvokableLatch(task);

        task.getExecutingThread().join();

        assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(task.isCanceledOrFailed()).isTrue();
        assertThat(task.getFailureCause().getMessage().contains("external")).isTrue();
    }

    @Test
    void testOnPartitionStateUpdateWhileRunning() throws Exception {
        testOnPartitionStateUpdate(ExecutionState.RUNNING);
    }

    /**
     * Partition state updates can also happen when {@link Task} is in {@link
     * ExecutionState#DEPLOYING} state, because we are requesting for partitions during setting up
     * input gates.
     */
    @Test
    void testOnPartitionStateUpdateWhileDeploying() throws Exception {
        testOnPartitionStateUpdate(ExecutionState.DEPLOYING);
    }

    public void testOnPartitionStateUpdate(ExecutionState initialTaskState) throws Exception {
        final ResultPartitionID partitionId = new ResultPartitionID();

        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInInvoke.class)
                        .build(Executors.directExecutor());

        RemoteChannelStateChecker checker = new RemoteChannelStateChecker(partitionId, "test task");

        // Expected task state for each producer state
        final Map<ExecutionState, ExecutionState> expected =
                new HashMap<>(ExecutionState.values().length);

        // Fail the task for unexpected states
        for (ExecutionState state : ExecutionState.values()) {
            expected.put(state, ExecutionState.FAILED);
        }

        expected.put(ExecutionState.INITIALIZING, initialTaskState);
        expected.put(ExecutionState.RUNNING, initialTaskState);
        expected.put(ExecutionState.SCHEDULED, initialTaskState);
        expected.put(ExecutionState.DEPLOYING, initialTaskState);
        expected.put(ExecutionState.FINISHED, initialTaskState);

        expected.put(ExecutionState.CANCELED, ExecutionState.CANCELING);
        expected.put(ExecutionState.CANCELING, ExecutionState.CANCELING);
        expected.put(ExecutionState.FAILED, ExecutionState.CANCELING);

        int producingStateCounter = 0;
        for (ExecutionState state : ExecutionState.values()) {
            TestTaskBuilder.setTaskState(task, initialTaskState);

            if (checker.isProducerReadyOrAbortConsumption(
                    task.new PartitionProducerStateResponseHandle(state, null))) {
                producingStateCounter++;
            }

            ExecutionState newTaskState = task.getExecutionState();

            assertThat(newTaskState).isEqualTo(expected.get(state));
        }

        assertThat(producingStateCounter).isEqualTo(5);
    }

    /** Tests the trigger partition state update future completions. */
    @Test
    void testTriggerPartitionStateUpdate() throws Exception {
        final IntermediateDataSetID resultId = new IntermediateDataSetID();
        final ResultPartitionID partitionId = new ResultPartitionID();

        final PartitionProducerStateChecker partitionChecker =
                mock(PartitionProducerStateChecker.class);

        AtomicInteger callCount = new AtomicInteger(0);

        RemoteChannelStateChecker remoteChannelStateChecker =
                new RemoteChannelStateChecker(partitionId, "test task");

        // Test all branches of trigger partition state check
        {
            // Reset latches
            setup();

            // PartitionProducerDisposedException
            final Task task =
                    createTaskBuilder()
                            .setInvokable(InvokableBlockingInInvoke.class)
                            .setPartitionProducerStateChecker(partitionChecker)
                            .build(Executors.directExecutor());
            TestTaskBuilder.setTaskState(task, ExecutionState.RUNNING);

            final CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
            when(partitionChecker.requestPartitionProducerState(
                            eq(task.getJobID()), eq(resultId), eq(partitionId)))
                    .thenReturn(promise);

            task.requestPartitionProducerState(
                    resultId,
                    partitionId,
                    checkResult ->
                            assertThat(
                                            remoteChannelStateChecker
                                                    .isProducerReadyOrAbortConsumption(checkResult))
                                    .isFalse());

            promise.completeExceptionally(new PartitionProducerDisposedException(partitionId));
            assertThat(task.getExecutionState()).isEqualTo(ExecutionState.CANCELING);
        }

        {
            // Reset latches
            setup();

            // Any other exception
            final Task task =
                    createTaskBuilder()
                            .setInvokable(InvokableBlockingInInvoke.class)
                            .setPartitionProducerStateChecker(partitionChecker)
                            .build(Executors.directExecutor());
            TestTaskBuilder.setTaskState(task, ExecutionState.RUNNING);

            final CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
            when(partitionChecker.requestPartitionProducerState(
                            eq(task.getJobID()), eq(resultId), eq(partitionId)))
                    .thenReturn(promise);

            task.requestPartitionProducerState(
                    resultId,
                    partitionId,
                    checkResult ->
                            assertThat(
                                            remoteChannelStateChecker
                                                    .isProducerReadyOrAbortConsumption(checkResult))
                                    .isFalse());

            promise.completeExceptionally(new RuntimeException("Any other exception"));

            assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        }

        {
            callCount.set(0);

            // Reset latches
            setup();

            // TimeoutException handled special => retry
            // Any other exception
            final Task task =
                    createTaskBuilder()
                            .setInvokable(InvokableBlockingInInvoke.class)
                            .setPartitionProducerStateChecker(partitionChecker)
                            .build(Executors.directExecutor());

            try {
                task.startTaskThread();
                awaitInvokableLatch(task);

                CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
                when(partitionChecker.requestPartitionProducerState(
                                eq(task.getJobID()), eq(resultId), eq(partitionId)))
                        .thenReturn(promise);

                task.requestPartitionProducerState(
                        resultId,
                        partitionId,
                        checkResult -> {
                            if (remoteChannelStateChecker.isProducerReadyOrAbortConsumption(
                                    checkResult)) {
                                callCount.incrementAndGet();
                            }
                        });

                promise.completeExceptionally(new TimeoutException());

                assertThat(task.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

                assertThat(callCount.get()).isEqualTo(1);
            } finally {
                task.getExecutingThread().interrupt();
                task.getExecutingThread().join();
            }
        }

        {
            callCount.set(0);

            // Reset latches
            setup();

            // Success
            final Task task =
                    createTaskBuilder()
                            .setInvokable(InvokableBlockingInInvoke.class)
                            .setPartitionProducerStateChecker(partitionChecker)
                            .build(Executors.directExecutor());

            try {
                task.startTaskThread();
                awaitInvokableLatch(task);

                CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
                when(partitionChecker.requestPartitionProducerState(
                                eq(task.getJobID()), eq(resultId), eq(partitionId)))
                        .thenReturn(promise);

                task.requestPartitionProducerState(
                        resultId,
                        partitionId,
                        checkResult -> {
                            if (remoteChannelStateChecker.isProducerReadyOrAbortConsumption(
                                    checkResult)) {
                                callCount.incrementAndGet();
                            }
                        });

                promise.complete(ExecutionState.RUNNING);

                assertThat(task.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

                assertThat(callCount.get()).isEqualTo(1);
            } finally {
                task.getExecutingThread().interrupt();
                task.getExecutingThread().join();
            }
        }
    }

    /**
     * Tests that interrupt happens via watch dog if canceller is stuck in cancel. Task cancellation
     * blocks the task canceller. Interrupt after cancel via cancellation watch dog.
     */
    @Test
    void testWatchDogInterruptsTask() throws Exception {
        final TaskManagerActions taskManagerActions = new ProhibitFatalErrorTaskManagerActions();

        final Configuration config = new Configuration();
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL.key(), 5);
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT.key(), 60 * 1000);

        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInCancel.class)
                        .setTaskManagerConfig(config)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        task.startTaskThread();

        awaitInvokableLatch(task);

        task.cancelExecution();
        task.getExecutingThread().join();
    }

    /**
     * The 'invoke' method holds a lock (trigger awaitLatch after acquisition) and cancel cannot
     * complete because it also tries to acquire the same lock. This is resolved by the watch dog,
     * no fatal error.
     */
    @Test
    void testInterruptibleSharedLockInInvokeAndCancel() throws Exception {
        final TaskManagerActions taskManagerActions = new ProhibitFatalErrorTaskManagerActions();

        final Configuration config = new Configuration();
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 5);
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 1000);

        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableInterruptibleSharedLockInInvokeAndCancel.class)
                        .setTaskManagerConfig(config)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        task.startTaskThread();

        awaitInvokableLatch(task);

        task.cancelExecution();
        task.getExecutingThread().join();
    }

    /**
     * The 'invoke' method blocks infinitely, but cancel() does not block. Only resolved by a fatal
     * error.
     */
    @Test
    void testFatalErrorAfterUnInterruptibleInvoke() throws Exception {
        final CompletableFuture<Throwable> fatalErrorFuture = new CompletableFuture<>();
        final TestingTaskManagerActions taskManagerActions =
                TestingTaskManagerActions.newBuilder()
                        .setNotifyFatalErrorConsumer(
                                (s, throwable) -> fatalErrorFuture.complete(throwable))
                        .build();

        final Configuration config = new Configuration();
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 10);

        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableUnInterruptibleBlockingInvoke.class)
                        .setTaskManagerConfig(config)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());

        try {
            task.startTaskThread();

            awaitInvokableLatch(task);

            task.cancelExecution();

            // wait for the notification of notifyFatalError
            final Throwable fatalError = fatalErrorFuture.join();
            assertThat(fatalError).isNotNull();
        } finally {
            // Interrupt again to clean up Thread
            triggerInvokableLatch(task);
            task.getExecutingThread().interrupt();
            task.getExecutingThread().join();
        }
    }

    /** Tests that a fatal error gotten from canceling task is notified. */
    @Test
    void testFatalErrorOnCanceling() throws Exception {
        final CompletableFuture<Throwable> fatalErrorFuture = new CompletableFuture<>();
        final TestingTaskManagerActions taskManagerActions =
                TestingTaskManagerActions.newBuilder()
                        .setNotifyFatalErrorConsumer(
                                (s, throwable) -> fatalErrorFuture.complete(throwable))
                        .build();

        final Configuration config = new Configuration();
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 5);
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 50);

        // We need to remember the original object since all changes in  `startTaskThread` applies
        // to it rather than to spy object.
        Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingWithTrigger.class)
                        .setTaskManagerConfig(config)
                        .setTaskManagerActions(taskManagerActions)
                        .build(Executors.directExecutor());
        final Task spyTask = spy(task);

        final Class<OutOfMemoryError> fatalErrorType = OutOfMemoryError.class;
        doThrow(fatalErrorType)
                .when(spyTask)
                .cancelOrFailAndCancelInvokableInternal(eq(ExecutionState.CANCELING), eq(null));

        try {
            spyTask.startTaskThread();

            awaitInvokableLatch(task);

            spyTask.cancelExecution();

            // wait for the notification of notifyFatalError
            final Throwable fatalError = fatalErrorFuture.join();
            assertThat(fatalError).isInstanceOf(fatalErrorType);
        } finally {
            triggerInvokableLatch(task);
        }
    }

    /** Tests that the task configuration is respected and overwritten by the execution config. */
    @Test
    void testTaskConfig() throws Exception {
        long interval = 28218123;
        long timeout = interval + 19292;

        final Configuration config = new Configuration();
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, interval);
        config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, timeout);

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setTaskCancellationInterval(interval + 1337);
        executionConfig.setTaskCancellationTimeout(timeout - 1337);

        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInInvoke.class)
                        .setTaskManagerConfig(config)
                        .setExecutionConfig(executionConfig)
                        .build(Executors.directExecutor());

        assertThat(task.getTaskCancellationInterval()).isEqualTo(interval);
        assertThat(task.getTaskCancellationTimeout()).isEqualTo(timeout);

        task.startTaskThread();

        awaitInvokableLatch(task);

        assertThat(executionConfig.getTaskCancellationInterval())
                .isEqualTo(task.getTaskCancellationInterval());
        assertThat(executionConfig.getTaskCancellationTimeout())
                .isEqualTo(task.getTaskCancellationTimeout());

        task.getExecutingThread().interrupt();
        task.getExecutingThread().join();
    }

    @Test
    void testTerminationFutureCompletesOnNormalExecution() throws Exception {
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingWithTrigger.class)
                        .setTaskManagerActions(new NoOpTaskManagerActions())
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        // wait till the task is in invoke
        awaitInvokableLatch(task);

        assertThat(task.getTerminationFuture().isDone()).isFalse();

        triggerInvokableLatch(task);

        task.getExecutingThread().join();

        assertThat(task.getTerminationFuture().getNow(null)).isEqualTo(ExecutionState.FINISHED);
    }

    @Test
    void testTerminationFutureCompletesOnImmediateCancellation() throws Exception {
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableBlockingInInvoke.class)
                        .setTaskManagerActions(new NoOpTaskManagerActions())
                        .build(Executors.directExecutor());

        task.cancelExecution();

        assertThat(task.getTerminationFuture().isDone()).isFalse();

        // run the task asynchronous
        task.startTaskThread();

        task.getExecutingThread().join();

        assertThat(task.getTerminationFuture().getNow(null)).isEqualTo(ExecutionState.CANCELED);
    }

    @Test
    void testTerminationFutureCompletesOnErrorInInvoke() throws Exception {
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableWithExceptionInInvoke.class)
                        .setTaskManagerActions(new NoOpTaskManagerActions())
                        .build(Executors.directExecutor());

        // run the task asynchronous
        task.startTaskThread();

        task.getExecutingThread().join();

        assertThat(task.getTerminationFuture().getNow(null)).isEqualTo(ExecutionState.FAILED);
    }

    @Test
    void testNoBackPressureIfTaskNotStarted() throws Exception {
        final Task task = createTaskBuilder().build(Executors.directExecutor());
        assertThat(task.isBackPressured()).isFalse();
    }

    @Test
    void testDeclineCheckpoint() throws Exception {
        TestCheckpointResponder testCheckpointResponder = new TestCheckpointResponder();
        final Task task =
                createTaskBuilder()
                        .setInvokable(InvokableDecliningCheckpoints.class)
                        .setCheckpointResponder(testCheckpointResponder)
                        .build(Executors.directExecutor());
        assertCheckpointDeclined(
                task,
                testCheckpointResponder,
                1,
                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY);

        task.startTaskThread();
        try {
            awaitInvokableLatch(task);
            assertThat(task.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

            assertCheckpointDeclined(
                    task,
                    testCheckpointResponder,
                    InvokableDecliningCheckpoints.REJECTED_EXECUTION_CHECKPOINT_ID,
                    CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING);
            assertCheckpointDeclined(
                    task,
                    testCheckpointResponder,
                    InvokableDecliningCheckpoints.THROWING_CHECKPOINT_ID,
                    CheckpointFailureReason.TASK_FAILURE);
            assertCheckpointDeclined(
                    task,
                    testCheckpointResponder,
                    InvokableDecliningCheckpoints.TRIGGERING_FAILED_CHECKPOINT_ID,
                    CheckpointFailureReason.TASK_FAILURE);
        } finally {
            triggerInvokableLatch(task);
            task.getExecutingThread().join();
        }
        assertThat(task.getTerminationFuture().getNow(null)).isEqualTo(ExecutionState.FINISHED);
    }

    private void assertCheckpointDeclined(
            Task task,
            TestCheckpointResponder testCheckpointResponder,
            long checkpointId,
            CheckpointFailureReason failureReason) {
        CheckpointOptions checkpointOptions =
                CheckpointOptions.alignedNoTimeout(
                        CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
        task.triggerCheckpointBarrier(checkpointId, 1, checkpointOptions);

        assertThat(testCheckpointResponder.getDeclineReports()).hasSize(1);
        assertThat(testCheckpointResponder.getDeclineReports().get(0).getCheckpointId())
                .isEqualTo(checkpointId);
        assertThat(
                        testCheckpointResponder
                                .getDeclineReports()
                                .get(0)
                                .getCause()
                                .getCheckpointFailureReason())
                .isEqualTo(failureReason);

        testCheckpointResponder.clear();
    }

    private TaskInvokable waitForInvokable(Task task) throws Exception {
        waitUntilCondition(() -> task.getInvokable() != null, 10L);

        return task.getInvokable();
    }

    private void awaitInvokableLatch(Task task) throws Exception {
        TaskInvokable taskInvokable = waitForInvokable(task);
        if (!(taskInvokable instanceof AwaitLatchInvokable)) {
            throw new Exception(
                    "Invokable doesn't implement class - " + AwaitLatchInvokable.class.getName());
        }

        ((AwaitLatchInvokable) taskInvokable).await();
    }

    private void triggerInvokableLatch(Task task) throws Exception {
        TaskInvokable taskInvokable = waitForInvokable(task);
        if (!(taskInvokable instanceof TriggerLatchInvokable)) {
            throw new Exception(
                    "Invokable doesn't implement class - " + TriggerLatchInvokable.class.getName());
        }

        ((TriggerLatchInvokable) taskInvokable).trigger();
    }

    // ------------------------------------------------------------------------
    //  customized TaskManagerActions
    // ------------------------------------------------------------------------

    /** Customized TaskManagerActions that queues all calls of updateTaskExecutionState. */
    private static class QueuedNoOpTaskManagerActions extends NoOpTaskManagerActions {
        private final BlockingQueue<TaskExecutionState> queue = new LinkedBlockingDeque<>();

        @Override
        public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
            assertThat(queue.offer(taskExecutionState)).isTrue();
        }

        private void validateListenerMessage(ExecutionState state, Task task, Throwable error) {
            try {
                // we may have to wait for a bit to give the actors time to receive the message
                // and put it into the queue
                final TaskExecutionState taskState = queue.take();
                assertThat(state).as("There is no additional listener message").isNotNull();

                assertThat(taskState.getID()).isEqualTo(task.getExecutionId());
                assertThat(taskState.getExecutionState()).isEqualTo(state);

                final Throwable t = taskState.getError(getClass().getClassLoader());
                if (error == null) {
                    assertThat(t).isNull();
                } else {
                    assertThat(t.toString()).isEqualTo(error.toString());
                }
            } catch (InterruptedException e) {
                fail("interrupted");
            }
        }
    }

    /** Customized TaskManagerActions that ensures no call of notifyFatalError. */
    private static class ProhibitFatalErrorTaskManagerActions extends NoOpTaskManagerActions {
        @Override
        public void notifyFatalError(String message, Throwable cause) {
            throw new RuntimeException("Unexpected FatalError notification");
        }
    }

    // ------------------------------------------------------------------------
    //  helper functions
    // ------------------------------------------------------------------------

    private TestTaskBuilder createTaskBuilder() {
        return new TestTaskBuilder(shuffleEnvironment);
    }

    // ------------------------------------------------------------------------
    //  test task classes
    // ------------------------------------------------------------------------

    private static final class TestInvokableCorrect extends AbstractInvokable {
        public TestInvokableCorrect(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {}

        @Override
        public void cancel() {}

        @Override
        public void cleanUp(Throwable throwable) throws Exception {
            wasCleanedUp = true;
            super.cleanUp(throwable);
        }
    }

    private abstract static class InvokableNonInstantiable extends AbstractInvokable {
        public InvokableNonInstantiable(Environment environment) {
            super(environment);
        }
    }

    private static final class InvokableWithExceptionInInvoke extends AbstractInvokable {
        public InvokableWithExceptionInInvoke(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            throw new Exception("test");
        }

        @Override
        public void cleanUp(Throwable throwable) throws Exception {
            wasCleanedUp = true;
            super.cleanUp(throwable);
        }
    }

    static final class InvokableWithExceptionInRestore extends AbstractInvokable {
        public InvokableWithExceptionInRestore(Environment environment) {
            super(environment);
        }

        @Override
        public void restore() throws Exception {
            throw new Exception(RESTORE_EXCEPTION_MSG);
        }

        @Override
        public void invoke() {}

        @Override
        public void cleanUp(Throwable throwable) throws Exception {
            wasCleanedUp = true;
            super.cleanUp(throwable);
        }
    }

    private static final class FailingInvokableWithChainedException extends AbstractInvokable {
        public FailingInvokableWithChainedException(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {
            throw new TestWrappedException(new IOException("test"));
        }

        @Override
        public void cancel() {}
    }

    private static class InvokableBlockingWithTrigger extends TriggerLatchInvokable {
        public InvokableBlockingWithTrigger(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            awaitLatch.trigger();

            triggerLatch.await();
        }
    }

    private static class InvokableDecliningCheckpoints extends InvokableBlockingWithTrigger {
        public static final int REJECTED_EXECUTION_CHECKPOINT_ID = 2;
        public static final int THROWING_CHECKPOINT_ID = 3;
        public static final int TRIGGERING_FAILED_CHECKPOINT_ID = 4;

        public InvokableDecliningCheckpoints(Environment environment) {
            super(environment);
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            long checkpointId = checkpointMetaData.getCheckpointId();
            switch (Math.toIntExact(checkpointId)) {
                case REJECTED_EXECUTION_CHECKPOINT_ID:
                    throw new RejectedExecutionException();
                case THROWING_CHECKPOINT_ID:
                    CompletableFuture<Boolean> result = new CompletableFuture<>();
                    result.completeExceptionally(new ExpectedTestException());
                    return result;
                case TRIGGERING_FAILED_CHECKPOINT_ID:
                    return CompletableFuture.completedFuture(false);
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported checkpointId: " + checkpointId);
            }
        }
    }

    private static final class InvokableBlockingInInvoke extends AwaitLatchInvokable {
        public InvokableBlockingInInvoke(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            awaitLatch.trigger();

            // block forever
            synchronized (this) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    wait();
                }
            }
        }
    }

    private static final class InvokableBlockingInRestore extends AwaitLatchInvokable {
        public InvokableBlockingInRestore(Environment environment) {
            super(environment);
        }

        @Override
        public void restore() throws Exception {
            awaitLatch.trigger();

            // block forever
            synchronized (this) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    wait();
                }
            }
        }

        @Override
        public void invoke() {}

        @Override
        public void cleanUp(Throwable throwable) throws Exception {
            wasCleanedUp = true;
            super.cleanUp(throwable);
        }
    }

    /** {@link AbstractInvokable} which throws {@link RuntimeException} on invoke. */
    public static final class InvokableWithExceptionOnTrigger extends TriggerLatchInvokable {
        public InvokableWithExceptionOnTrigger(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {
            awaitTriggerLatch();

            throw new RuntimeException("test");
        }
    }

    /** {@link AbstractInvokable} which throws {@link CancelTaskException} on invoke. */
    public static final class InvokableWithCancelTaskExceptionInInvoke
            extends TriggerLatchInvokable {
        public InvokableWithCancelTaskExceptionInInvoke(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {
            awaitTriggerLatch();

            throw new CancelTaskException();
        }
    }

    /** {@link AbstractInvokable} which blocks in cancel. */
    public static final class InvokableBlockingInCancel extends TriggerLatchInvokable {
        public InvokableBlockingInCancel(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {
            awaitLatch.trigger();

            try {
                triggerLatch.await(); // await cancel
                synchronized (this) {
                    wait();
                }
            } catch (InterruptedException ignored) {
                synchronized (this) {
                    notifyAll(); // notify all that are stuck in cancel
                }
            }
        }

        @Override
        public void cancel() throws Exception {
            synchronized (this) {
                triggerLatch.trigger();
                wait();
            }
        }
    }

    /** {@link AbstractInvokable} which blocks in cancel and is interruptible. */
    public static final class InvokableInterruptibleSharedLockInInvokeAndCancel
            extends TriggerLatchInvokable {
        private final Object lock = new Object();

        public InvokableInterruptibleSharedLockInInvokeAndCancel(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            while (!triggerLatch.isTriggered()) {
                synchronized (lock) {
                    awaitLatch.trigger();
                    lock.wait();
                }
            }
        }

        @Override
        public void cancel() {
            synchronized (lock) {
                // do nothing but a placeholder
                triggerLatch.trigger();
            }
        }
    }

    /** {@link AbstractInvokable} which blocks in cancel and is not interruptible. */
    public static final class InvokableUnInterruptibleBlockingInvoke extends TriggerLatchInvokable {
        public InvokableUnInterruptibleBlockingInvoke(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {
            while (!triggerLatch.isTriggered()) {
                try {
                    synchronized (this) {
                        awaitLatch.trigger();
                        wait();
                    }
                } catch (InterruptedException ignored) {
                }
            }
        }

        @Override
        public void cancel() {}
    }

    // ------------------------------------------------------------------------
    //  test exceptions
    // ------------------------------------------------------------------------

    private static class TestWrappedException extends WrappingRuntimeException {
        private static final long serialVersionUID = 1L;

        TestWrappedException(@Nonnull Throwable cause) {
            super(cause);
        }
    }

    private abstract static class AwaitLatchInvokable extends AbstractInvokable {

        final OneShotLatch awaitLatch = new OneShotLatch();

        public AwaitLatchInvokable(Environment environment) {
            super(environment);
        }

        void await() throws InterruptedException {
            awaitLatch.await();
        }
    }

    private abstract static class TriggerLatchInvokable extends AwaitLatchInvokable {

        final OneShotLatch triggerLatch = new OneShotLatch();

        public TriggerLatchInvokable(Environment environment) {
            super(environment);
        }

        void trigger() {
            triggerLatch.trigger();
        }

        void awaitTriggerLatch() {
            awaitLatch.trigger();

            // make sure that the interrupt call does not
            // grab us out of the lock early
            while (true) {
                try {
                    triggerLatch.await();
                    break;
                } catch (InterruptedException e) {
                    // fall through the loop
                }
            }
        }
    }
}
