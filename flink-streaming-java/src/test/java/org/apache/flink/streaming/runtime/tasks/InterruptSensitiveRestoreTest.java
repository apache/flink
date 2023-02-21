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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.VoidPermanentBlobService;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestStreamStateHandle;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * This test checks that task restores that get stuck in the presence of interrupts are handled
 * properly.
 *
 * <p>In practice, reading from HDFS is interrupt sensitive: The HDFS code frequently deadlocks or
 * livelocks if it is interrupted.
 */
public class InterruptSensitiveRestoreTest {

    private static final OneShotLatch IN_RESTORE_LATCH = new OneShotLatch();

    private static final int OPERATOR_MANAGED = 0;
    private static final int OPERATOR_RAW = 1;
    private static final int KEYED_MANAGED = 2;
    private static final int KEYED_RAW = 3;

    @Test
    public void testRestoreWithInterruptOperatorManaged() throws Exception {
        testRestoreWithInterrupt(OPERATOR_MANAGED);
    }

    @Test
    public void testRestoreWithInterruptOperatorRaw() throws Exception {
        testRestoreWithInterrupt(OPERATOR_RAW);
    }

    @Test
    public void testRestoreWithInterruptKeyedManaged() throws Exception {
        testRestoreWithInterrupt(KEYED_MANAGED);
    }

    @Test
    public void testRestoreWithInterruptKeyedRaw() throws Exception {
        testRestoreWithInterrupt(KEYED_RAW);
    }

    private void testRestoreWithInterrupt(int mode) throws Exception {

        IN_RESTORE_LATCH.reset();
        Configuration taskConfig = new Configuration();
        StreamConfig cfg = new StreamConfig(taskConfig);
        cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        switch (mode) {
            case OPERATOR_MANAGED:
            case OPERATOR_RAW:
            case KEYED_MANAGED:
            case KEYED_RAW:
                cfg.setStateKeySerializer(IntSerializer.INSTANCE);
                cfg.setStreamOperator(new StreamSource<>(new TestSource(mode)));
                cfg.serializeAllConfigs();
                break;
            default:
                throw new IllegalArgumentException();
        }

        StreamStateHandle lockingHandle = new InterruptLockingStateHandle();

        Task task = createTask(cfg, taskConfig, lockingHandle, mode);

        // start the task and wait until it is in "restore"
        task.startTaskThread();
        IN_RESTORE_LATCH.await();

        // trigger cancellation and signal to continue
        task.cancelExecution();

        task.getExecutingThread().join(30000);

        if (task.getExecutionState() == ExecutionState.CANCELING) {
            fail("Task is stuck and not canceling");
        }

        assertEquals(ExecutionState.CANCELED, task.getExecutionState());
        assertNull(task.getFailureCause());
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static Task createTask(
            StreamConfig streamConfig, Configuration taskConfig, StreamStateHandle state, int mode)
            throws IOException {

        ShuffleEnvironment<?, ?> shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

        Collection<KeyedStateHandle> keyedStateFromBackend = Collections.emptyList();
        Collection<KeyedStateHandle> keyedStateFromStream = Collections.emptyList();
        Collection<OperatorStateHandle> operatorStateBackend = Collections.emptyList();
        Collection<OperatorStateHandle> operatorStateStream = Collections.emptyList();

        Map<String, OperatorStateHandle.StateMetaInfo> operatorStateMetadata = new HashMap<>(1);
        OperatorStateHandle.StateMetaInfo metaInfo =
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
        operatorStateMetadata.put(
                DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME, metaInfo);

        KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(new KeyGroupRange(0, 0));

        Collection<OperatorStateHandle> operatorStateHandles =
                Collections.singletonList(
                        new OperatorStreamStateHandle(operatorStateMetadata, state));

        List<KeyedStateHandle> keyedStateHandles =
                Collections.singletonList(new KeyGroupsStateHandle(keyGroupRangeOffsets, state));

        switch (mode) {
            case OPERATOR_MANAGED:
                operatorStateBackend = operatorStateHandles;
                break;
            case OPERATOR_RAW:
                operatorStateStream = operatorStateHandles;
                break;
            case KEYED_MANAGED:
                keyedStateFromBackend = keyedStateHandles;
                break;
            case KEYED_RAW:
                keyedStateFromStream = keyedStateHandles;
                break;
            default:
                throw new IllegalArgumentException();
        }

        OperatorSubtaskState operatorSubtaskState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(new StateObjectCollection<>(operatorStateBackend))
                        .setRawOperatorState(new StateObjectCollection<>(operatorStateStream))
                        .setManagedKeyedState(new StateObjectCollection<>(keyedStateFromBackend))
                        .setRawKeyedState(new StateObjectCollection<>(keyedStateFromStream))
                        .build();

        JobVertexID jobVertexID = new JobVertexID();
        OperatorID operatorID = OperatorID.fromJobVertexID(jobVertexID);
        streamConfig.setOperatorID(operatorID);
        TaskStateSnapshot stateSnapshot = new TaskStateSnapshot();
        stateSnapshot.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);

        JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(1L, stateSnapshot);

        JobInformation jobInformation =
                new JobInformation(
                        new JobID(),
                        "test job name",
                        new SerializedValue<>(new ExecutionConfig()),
                        new Configuration(),
                        Collections.emptyList(),
                        Collections.emptyList());

        TaskInformation taskInformation =
                new TaskInformation(
                        jobVertexID,
                        "test task name",
                        1,
                        1,
                        SourceStreamTask.class.getName(),
                        taskConfig);

        TestTaskStateManager taskStateManager =
                TestTaskStateManager.builder()
                        .setReportedCheckpointId(taskRestore.getRestoreCheckpointId())
                        .setJobManagerTaskStateSnapshotsByCheckpointId(
                                Collections.singletonMap(
                                        taskRestore.getRestoreCheckpointId(),
                                        taskRestore.getTaskStateSnapshot()))
                        .build();

        return new Task(
                jobInformation,
                taskInformation,
                createExecutionAttemptId(taskInformation.getJobVertexId()),
                new AllocationID(),
                Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
                Collections.<InputGateDeploymentDescriptor>emptyList(),
                mock(MemoryManager.class),
                new SharedResources(),
                mock(IOManager.class),
                shuffleEnvironment,
                new KvStateService(new KvStateRegistry(), null, null),
                mock(BroadcastVariableManager.class),
                new TaskEventDispatcher(),
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                taskStateManager,
                mock(TaskManagerActions.class),
                mock(InputSplitProvider.class),
                mock(CheckpointResponder.class),
                new NoOpTaskOperatorEventGateway(),
                new TestGlobalAggregateManager(),
                TestingClassLoaderLease.newBuilder().build(),
                new FileCache(
                        new String[] {EnvironmentInformation.getTemporaryFileDirectory()},
                        VoidPermanentBlobService.INSTANCE),
                new TestingTaskManagerRuntimeInfo(),
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                mock(PartitionProducerStateChecker.class),
                mock(Executor.class),
                new ChannelStateWriteRequestExecutorFactory(jobInformation.getJobId()));
    }

    // ------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static class InterruptLockingStateHandle implements TestStreamStateHandle {

        private static final long serialVersionUID = 1L;

        private volatile boolean closed;

        @Override
        public FSDataInputStream openInputStream() throws IOException {

            closed = false;

            FSDataInputStream is =
                    new FSDataInputStream() {

                        @Override
                        public void seek(long desired) {}

                        @Override
                        public long getPos() {
                            return 0;
                        }

                        @Override
                        public int read() throws IOException {
                            block();
                            throw new EOFException();
                        }

                        @Override
                        public void close() throws IOException {
                            super.close();
                            closed = true;
                        }
                    };

            return is;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        private void block() {
            IN_RESTORE_LATCH.trigger();
            // this mimics what happens in the HDFS client code.
            // an interrupt on a waiting object leads to an infinite loop
            try {
                synchronized (this) {
                    //noinspection WaitNotInLoop
                    wait();
                }
            } catch (InterruptedException e) {
                while (!closed) {
                    try {
                        synchronized (this) {
                            wait();
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        @Override
        public void discardState() throws Exception {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    // ------------------------------------------------------------------------

    private static class TestSource implements SourceFunction<Object>, CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private final int testType;

        public TestSource(int testType) {
            this.testType = testType;
        }

        @Override
        public void run(SourceContext<Object> ctx) throws Exception {
            fail("should never be called");
        }

        @Override
        public void cancel() {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            fail("should never be called");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // raw keyed state is already read by timer service, all others to initialize the
            // context...we only need to
            // trigger this manually.
            ((StateInitializationContext) context)
                    .getRawOperatorStateInputs()
                    .iterator()
                    .next()
                    .getStream()
                    .read();
        }
    }
}
