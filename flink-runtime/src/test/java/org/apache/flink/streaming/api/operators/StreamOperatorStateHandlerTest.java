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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StreamOperatorStateHandlerTest}. */
class StreamOperatorStateHandlerTest {
    /**
     * Tests that a failing snapshot method call to the keyed state backend will trigger the closing
     * of the StateSnapshotContextSynchronousImpl and the cancellation of the
     * OperatorSnapshotResult. The latter is supposed to also cancel all assigned futures.
     */
    @Test
    void testFailingBackendSnapshotMethod() throws Exception {
        final long checkpointId = 42L;
        final long timestamp = 1L;

        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture =
                    new CancelableFuture<>();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture =
                    new CancelableFuture<>();
            RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture =
                    new CancelableFuture<>();
            RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture =
                    new CancelableFuture<>();
            RunnableFuture<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>
                    inputChannelStateFuture = new CancelableFuture<>();
            RunnableFuture<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>
                    resultSubpartitionStateFuture = new CancelableFuture<>();

            OperatorSnapshotFutures operatorSnapshotResult =
                    new OperatorSnapshotFutures(
                            keyedStateManagedFuture,
                            keyedStateRawFuture,
                            operatorStateManagedFuture,
                            operatorStateRawFuture,
                            inputChannelStateFuture,
                            resultSubpartitionStateFuture);

            StateSnapshotContextSynchronousImpl context =
                    new TestStateSnapshotContextSynchronousImpl(
                            checkpointId, timestamp, closeableRegistry);
            context.getRawKeyedOperatorStateOutput();
            context.getRawOperatorStateOutput();

            StreamTaskStateInitializerImpl stateInitializer =
                    new StreamTaskStateInitializerImpl(
                            new MockEnvironmentBuilder().build(), new HashMapStateBackend());
            StreamOperatorStateContext stateContext =
                    stateInitializer.streamOperatorStateContext(
                            new OperatorID(),
                            "whatever",
                            new TestProcessingTimeService(),
                            new UnUsedKeyContext(),
                            IntSerializer.INSTANCE,
                            closeableRegistry,
                            new InterceptingOperatorMetricGroup(),
                            1.0,
                            false,
                            false);
            StreamOperatorStateHandler stateHandler =
                    new StreamOperatorStateHandler(
                            stateContext, new ExecutionConfig(), closeableRegistry);

            final String keyedStateField = "keyedStateField";
            final String operatorStateField = "operatorStateField";

            CheckpointedStreamOperator checkpointedStreamOperator =
                    new CheckpointedStreamOperator() {
                        @Override
                        public void initializeState(StateInitializationContext context)
                                throws Exception {
                            context.getKeyedStateStore()
                                    .getState(
                                            new ValueStateDescriptor<>(
                                                    keyedStateField, LongSerializer.INSTANCE))
                                    .update(42L);
                            context.getOperatorStateStore()
                                    .getListState(
                                            new ListStateDescriptor<>(
                                                    operatorStateField, LongSerializer.INSTANCE))
                                    .add(42L);
                        }

                        @Override
                        public void snapshotState(StateSnapshotContext context) throws Exception {
                            throw new ExpectedTestException();
                        }
                    };

            stateHandler.setCurrentKey("44");
            stateHandler.initializeOperatorState(checkpointedStreamOperator);

            assertThat(stateContext.operatorStateBackend().getRegisteredStateNames()).isNotEmpty();
            assertThat(
                            ((AbstractKeyedStateBackend<?>) stateContext.keyedStateBackend())
                                    .numKeyValueStatesByName())
                    .isOne();

            assertThatThrownBy(
                            () ->
                                    stateHandler.snapshotState(
                                            checkpointedStreamOperator,
                                            Optional.of(stateContext.internalTimerServiceManager()),
                                            "42",
                                            42,
                                            42,
                                            CheckpointOptions.forCheckpointWithDefaultLocation(),
                                            new MemCheckpointStreamFactory(1024),
                                            operatorSnapshotResult,
                                            context,
                                            false,
                                            false))
                    .isInstanceOfSatisfying(
                            CheckpointException.class,
                            // We can not check for ExpectedTestException class directly,
                            // as CheckpointException is wrapping the cause with SerializedThrowable
                            e ->
                                    assertThat(
                                                    ExceptionUtils.findThrowableWithMessage(
                                                            e, ExpectedTestException.MESSAGE))
                                            .isPresent());

            assertThat(keyedStateManagedFuture).isCancelled();
            assertThat(keyedStateRawFuture).isCancelled();
            assertThat(context.getKeyedStateStreamFuture()).isCancelled();
            assertThat(operatorStateManagedFuture).isCancelled();
            assertThat(operatorStateRawFuture).isCancelled();
            assertThat(context.getOperatorStateStreamFuture()).isCancelled();
            assertThat(inputChannelStateFuture).isCancelled();
            assertThat(resultSubpartitionStateFuture).isCancelled();

            stateHandler.dispose();

            assertThat(stateContext.operatorStateBackend().getRegisteredBroadcastStateNames())
                    .isEmpty();
            assertThat(stateContext.operatorStateBackend().getRegisteredStateNames()).isEmpty();
            assertThat(
                            ((AbstractKeyedStateBackend<?>) stateContext.keyedStateBackend())
                                    .numKeyValueStatesByName())
                    .isZero();
        }
    }

    private static class TestStateSnapshotContextSynchronousImpl
            extends StateSnapshotContextSynchronousImpl {
        public TestStateSnapshotContextSynchronousImpl(
                long checkpointId, long timestamp, CloseableRegistry closeableRegistry) {
            super(
                    checkpointId,
                    timestamp,
                    new MemCheckpointStreamFactory(1024),
                    new KeyGroupRange(0, 2),
                    closeableRegistry);
            this.keyedStateCheckpointClosingFuture = new CancelableFuture<>();
            this.operatorStateCheckpointClosingFuture = new CancelableFuture<>();
        }
    }

    private static class CancelableFuture<T> extends FutureTask<T> {
        public CancelableFuture() {
            super(
                    () -> {
                        throw new UnsupportedOperationException();
                    });
        }
    }

    private static class UnUsedKeyContext implements KeyContext {
        @Override
        public void setCurrentKey(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getCurrentKey() {
            throw new UnsupportedOperationException();
        }
    }
}
