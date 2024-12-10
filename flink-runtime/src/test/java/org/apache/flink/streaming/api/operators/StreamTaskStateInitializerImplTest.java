/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.TestTaskLocalStateStore;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.Collections;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.state.OperatorStateHandle.Mode.SPLIT_DISTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Test for {@link StreamTaskStateInitializerImpl}. */
class StreamTaskStateInitializerImplTest {

    @Test
    void testNoRestore() throws Exception {

        HashMapStateBackend stateBackend = spy(new HashMapStateBackend());

        // No job manager provided state to restore
        StreamTaskStateInitializer streamTaskStateManager =
                streamTaskStateManager(
                        stateBackend,
                        null,
                        new SubTaskInitializationMetricsBuilder(
                                SystemClock.getInstance().absoluteTimeMillis()),
                        true);

        OperatorID operatorID = new OperatorID(47L, 11L);
        AbstractStreamOperator<?> streamOperator = mock(AbstractStreamOperator.class);
        when(streamOperator.getOperatorID()).thenReturn(operatorID);

        TypeSerializer<?> typeSerializer = new IntSerializer();
        CloseableRegistry closeableRegistry = new CloseableRegistry();

        StreamOperatorStateContext stateContext =
                streamTaskStateManager.streamOperatorStateContext(
                        streamOperator.getOperatorID(),
                        streamOperator.getClass().getSimpleName(),
                        new TestProcessingTimeService(),
                        streamOperator,
                        typeSerializer,
                        closeableRegistry,
                        new UnregisteredMetricsGroup(),
                        1.0,
                        false,
                        false);

        OperatorStateBackend operatorStateBackend = stateContext.operatorStateBackend();
        CheckpointableKeyedStateBackend<?> keyedStateBackend = stateContext.keyedStateBackend();
        InternalTimeServiceManager<?> timeServiceManager =
                stateContext.internalTimerServiceManager();
        CloseableIterable<KeyGroupStatePartitionStreamProvider> keyedStateInputs =
                stateContext.rawKeyedStateInputs();
        CloseableIterable<StatePartitionStreamProvider> operatorStateInputs =
                stateContext.rawOperatorStateInputs();

        assertThat(stateContext.isRestored())
                .as("Expected the context to NOT be restored")
                .isFalse();
        assertThat(operatorStateBackend).isNotNull();
        assertThat(keyedStateBackend).isNotNull();
        assertThat(timeServiceManager).isNotNull();
        assertThat(keyedStateInputs).isNotNull();
        assertThat(operatorStateInputs).isNotNull();

        checkCloseablesRegistered(
                closeableRegistry,
                operatorStateBackend,
                keyedStateBackend,
                keyedStateInputs,
                operatorStateInputs);

        assertThat(keyedStateInputs.iterator()).isExhausted();
        assertThat(operatorStateInputs.iterator()).isExhausted();
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithRestore() throws Exception {

        StateBackend mockingBackend =
                spy(
                        new StateBackend() {
                            @Override
                            public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                                    KeyedStateBackendParameters<K> parameters) throws Exception {
                                return mock(AbstractKeyedStateBackend.class);
                            }

                            @Override
                            public OperatorStateBackend createOperatorStateBackend(
                                    OperatorStateBackendParameters parameters) throws Exception {
                                return mock(OperatorStateBackend.class);
                            }
                        });

        OperatorID operatorID = new OperatorID(47L, 11L);
        TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();

        Random random = new Random(0x42);

        OperatorSubtaskState operatorSubtaskState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                new OperatorStreamStateHandle(
                                        Collections.singletonMap(
                                                "a",
                                                new OperatorStateHandle.StateMetaInfo(
                                                        new long[] {0, 10}, SPLIT_DISTRIBUTE)),
                                        CheckpointTestUtils.createDummyStreamStateHandle(
                                                random, null)))
                        .setRawOperatorState(
                                new OperatorStreamStateHandle(
                                        Collections.singletonMap(
                                                "_default_",
                                                new OperatorStateHandle.StateMetaInfo(
                                                        new long[] {0, 20, 30}, SPLIT_DISTRIBUTE)),
                                        CheckpointTestUtils.createDummyStreamStateHandle(
                                                random, null)))
                        .setManagedKeyedState(
                                CheckpointTestUtils.createDummyKeyGroupStateHandle(random, null))
                        .setRawKeyedState(
                                CheckpointTestUtils.createDummyKeyGroupStateHandle(random, null))
                        .setInputChannelState(
                                singleton(createNewInputChannelStateHandle(10, random)))
                        .setResultSubpartitionState(
                                singleton(createNewResultSubpartitionStateHandle(10, random)))
                        .build();

        taskStateSnapshot.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);

        JobManagerTaskRestore jobManagerTaskRestore =
                new JobManagerTaskRestore(42L, taskStateSnapshot);

        SubTaskInitializationMetricsBuilder metricsBuilder =
                new SubTaskInitializationMetricsBuilder(
                        SystemClock.getInstance().absoluteTimeMillis());

        StreamTaskStateInitializer streamTaskStateManager =
                streamTaskStateManager(
                        mockingBackend, jobManagerTaskRestore, metricsBuilder, false);

        AbstractStreamOperator<?> streamOperator = mock(AbstractStreamOperator.class);
        when(streamOperator.getOperatorID()).thenReturn(operatorID);

        TypeSerializer<?> typeSerializer = new IntSerializer();
        CloseableRegistry closeableRegistry = new CloseableRegistry();

        StreamOperatorStateContext stateContext =
                streamTaskStateManager.streamOperatorStateContext(
                        streamOperator.getOperatorID(),
                        streamOperator.getClass().getSimpleName(),
                        new TestProcessingTimeService(),
                        streamOperator,
                        typeSerializer,
                        closeableRegistry,
                        new UnregisteredMetricsGroup(),
                        1.0,
                        false,
                        false);

        OperatorStateBackend operatorStateBackend = stateContext.operatorStateBackend();
        CheckpointableKeyedStateBackend<?> keyedStateBackend = stateContext.keyedStateBackend();
        InternalTimeServiceManager<?> timeServiceManager =
                stateContext.internalTimerServiceManager();
        CloseableIterable<KeyGroupStatePartitionStreamProvider> keyedStateInputs =
                stateContext.rawKeyedStateInputs();
        CloseableIterable<StatePartitionStreamProvider> operatorStateInputs =
                stateContext.rawOperatorStateInputs();

        assertThat(stateContext.isRestored()).as("Expected the context to be restored").isTrue();
        assertThat(stateContext.getRestoredCheckpointId()).hasValue(42L);

        assertThat(operatorStateBackend).isNotNull();
        assertThat(keyedStateBackend).isNotNull();
        // this is deactivated on purpose so that it does not attempt to consume the raw keyed
        // state.
        assertThat(timeServiceManager).isNull();
        assertThat(keyedStateInputs).isNotNull();
        assertThat(operatorStateInputs).isNotNull();

        int count = 0;
        for (KeyGroupStatePartitionStreamProvider keyedStateInput : keyedStateInputs) {
            ++count;
        }
        assertThat(count).isOne();

        count = 0;
        for (StatePartitionStreamProvider operatorStateInput : operatorStateInputs) {
            ++count;
        }
        assertThat(count).isEqualTo(3);

        long expectedSumLocalMemory =
                Stream.of(
                                operatorSubtaskState.getManagedOperatorState().stream(),
                                operatorSubtaskState.getManagedKeyedState().stream(),
                                operatorSubtaskState.getRawKeyedState().stream(),
                                operatorSubtaskState.getRawOperatorState().stream())
                        .flatMap(i -> i)
                        .mapToLong(StateObject::getStateSize)
                        .sum();

        long expectedSumUnknown =
                Stream.concat(
                                operatorSubtaskState.getInputChannelState().stream(),
                                operatorSubtaskState.getResultSubpartitionState().stream())
                        .mapToLong(StateObject::getStateSize)
                        .sum();

        SubTaskInitializationMetrics metrics = metricsBuilder.build();
        assertThat(metrics.getDurationMetrics())
                .hasSize(2)
                .containsEntry(
                        MetricNames.RESTORED_STATE_SIZE
                                + "."
                                + StateObject.StateObjectLocation.LOCAL_MEMORY.name(),
                        expectedSumLocalMemory)
                .containsEntry(
                        MetricNames.RESTORED_STATE_SIZE
                                + "."
                                + StateObject.StateObjectLocation.UNKNOWN.name(),
                        expectedSumUnknown);

        checkCloseablesRegistered(
                closeableRegistry,
                operatorStateBackend,
                keyedStateBackend,
                keyedStateInputs,
                operatorStateInputs);
    }

    private static void checkCloseablesRegistered(
            CloseableRegistry closeableRegistry, Closeable... closeables) {
        for (Closeable closeable : closeables) {
            assertThat(closeableRegistry.unregisterCloseable(closeable)).isTrue();
        }
    }

    private StreamTaskStateInitializer streamTaskStateManager(
            StateBackend stateBackend,
            JobManagerTaskRestore jobManagerTaskRestore,
            SubTaskInitializationMetricsBuilder metricsBuilder,
            boolean createTimerServiceManager) {

        JobID jobID = new JobID(42L, 43L);
        ExecutionAttemptID executionAttemptID = createExecutionAttemptId();
        TestCheckpointResponder checkpointResponderMock = new TestCheckpointResponder();

        TaskLocalStateStore taskLocalStateStore = new TestTaskLocalStateStore();
        InMemoryStateChangelogStorage changelogStorage = new InMemoryStateChangelogStorage();

        TaskStateManager taskStateManager =
                new TaskStateManagerImpl(
                        jobID,
                        executionAttemptID,
                        taskLocalStateStore,
                        null,
                        changelogStorage,
                        new TaskExecutorStateChangelogStoragesManager(),
                        jobManagerTaskRestore,
                        checkpointResponderMock);

        DummyEnvironment dummyEnvironment =
                new DummyEnvironment(
                        "test-task",
                        1,
                        executionAttemptID.getExecutionVertexId().getSubtaskIndex());
        dummyEnvironment.setTaskStateManager(taskStateManager);

        if (createTimerServiceManager) {
            return new StreamTaskStateInitializerImpl(dummyEnvironment, stateBackend);
        } else {
            return new StreamTaskStateInitializerImpl(
                    dummyEnvironment,
                    stateBackend,
                    metricsBuilder,
                    TtlTimeProvider.DEFAULT,
                    new InternalTimeServiceManager.Provider() {
                        @Override
                        public <K> InternalTimeServiceManager<K> create(
                                TaskIOMetricGroup taskIOMetricGroup,
                                PriorityQueueSetFactory factory,
                                KeyGroupRange keyGroupRange,
                                ClassLoader userClassloader,
                                KeyContext keyContext,
                                ProcessingTimeService processingTimeService,
                                Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates,
                                StreamTaskCancellationContext cancellationContext)
                                throws Exception {
                            return null;
                        }
                    },
                    StreamTaskCancellationContext.alwaysRunning());
        }
    }
}
