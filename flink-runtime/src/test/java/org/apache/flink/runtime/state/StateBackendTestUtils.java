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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.MockStateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.RunnableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** This class contains test utils of {@link StateBackend} */
public class StateBackendTestUtils {

    /** Create an AbstractStateBackend supporting apply the snapshot result. */
    public static AbstractStateBackend wrapStateBackendWithSnapshotFunction(
            AbstractStateBackend delegatedStataBackend,
            SerializableFunctionWithException<RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                    snapshotResultFunction) {
        return new ApplyingSnapshotStateBackend(delegatedStataBackend, snapshotResultFunction);
    }

    public static StateBackend buildAsyncStateBackend(StateBackend delegatedSyncStateBackend) {
        return new TestAsyncStateBackend(delegatedSyncStateBackend)
                .setInnerState(null)
                .setStateExecutor(new MockStateExecutor());
    }

    public static StateBackend buildAsyncStateBackend(
            Supplier<org.apache.flink.api.common.state.v2.State> innerStateSupplier,
            StateExecutor stateExecutor) {
        return new TestAsyncStateBackend(new HashMapStateBackend())
                .setInnerState(innerStateSupplier)
                .setStateExecutor(stateExecutor);
    }

    private static class TestAsyncStateBackend implements StateBackend {

        private final StateBackend delegatedStateBackend;
        private Supplier<org.apache.flink.api.common.state.v2.State> innerStateSupplier;
        private StateExecutor stateExecutor;

        public TestAsyncStateBackend(StateBackend delegatedStateBackend) {
            this.delegatedStateBackend = delegatedStateBackend;
        }

        public TestAsyncStateBackend setInnerState(
                Supplier<org.apache.flink.api.common.state.v2.State> innerStateSupplier) {
            this.innerStateSupplier = innerStateSupplier;
            return this;
        }

        public TestAsyncStateBackend setStateExecutor(StateExecutor stateExecutor) {
            this.stateExecutor = stateExecutor;
            return this;
        }

        @Override
        public boolean supportsAsyncKeyedStateBackend() {
            return true;
        }

        @Override
        public <K> AsyncKeyedStateBackend createAsyncKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) throws Exception {
            return delegatedStateBackend.supportsAsyncKeyedStateBackend()
                    ? delegatedStateBackend.createAsyncKeyedStateBackend(parameters)
                    : new TestAsyncKeyedStateBackend(innerStateSupplier, stateExecutor);
        }

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) throws Exception {
            return delegatedStateBackend.createKeyedStateBackend(parameters);
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            return delegatedStateBackend.createOperatorStateBackend(parameters);
        }
    }

    private static class TestAsyncKeyedStateBackend<K> implements AsyncKeyedStateBackend<K> {

        private final Supplier<org.apache.flink.api.common.state.v2.State> innerStateSupplier;
        private final StateExecutor stateExecutor;
        private final PriorityQueueSetFactory factory;

        public TestAsyncKeyedStateBackend(
                Supplier<org.apache.flink.api.common.state.v2.State> innerStateSupplier,
                StateExecutor stateExecutor) {
            this.innerStateSupplier = innerStateSupplier;
            this.stateExecutor = stateExecutor;
            this.factory = new HeapPriorityQueueSetFactory(new KeyGroupRange(0, 127), 128, 128);
        }

        @Override
        public void setup(@Nonnull StateRequestHandler stateRequestHandler) {
            // do nothing
        }

        @Nonnull
        @Override
        @SuppressWarnings("unchecked")
        public <N, S extends org.apache.flink.api.common.state.v2.State, SV> S createState(
                @Nonnull N defaultNamespace,
                @Nonnull TypeSerializer<N> namespaceSerializer,
                @Nonnull org.apache.flink.runtime.state.v2.StateDescriptor<SV> stateDesc) {
            return (S) innerStateSupplier.get();
        }

        @Nonnull
        @Override
        public <N, S extends InternalKeyedState, SV> S createStateInternal(
                @Nonnull N defaultNamespace,
                @Nonnull TypeSerializer<N> namespaceSerializer,
                @Nonnull org.apache.flink.runtime.state.v2.StateDescriptor<SV> stateDesc)
                throws Exception {
            return (S) innerStateSupplier.get();
        }

        @Nonnull
        @Override
        public StateExecutor createStateExecutor() {
            return stateExecutor;
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return new KeyGroupRange(0, 127);
        }

        @Override
        public void dispose() {
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
            // do nothing
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            // do nothing
        }

        @Override
        public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                CheckpointStreamFactory streamFactory,
                CheckpointOptions checkpointOptions)
                throws Exception {
            // do nothing
            return null;
        }

        @Nonnull
        @Override
        public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @Nonnull String stateName,
                        @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            return factory.create(stateName, byteOrderedElementSerializer);
        }
    }

    /** Wrapper of state backend which supports apply the snapshot result. */
    private static class ApplyingSnapshotStateBackend extends AbstractStateBackend {

        private static final long serialVersionUID = 1L;

        private final AbstractStateBackend delegatedStataBackend;

        private final SerializableFunctionWithException<
                        RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                snapshotResultFunction;

        public ApplyingSnapshotStateBackend(
                AbstractStateBackend delegatedStataBackend,
                SerializableFunctionWithException<RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                        snapshotResultFunction) {
            this.delegatedStataBackend = delegatedStataBackend;
            this.snapshotResultFunction = snapshotResultFunction;
        }

        @Override
        public boolean useManagedMemory() {
            return delegatedStataBackend.useManagedMemory();
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) throws IOException {
            AbstractKeyedStateBackend<K> delegatedKeyedStateBackend =
                    delegatedStataBackend.createKeyedStateBackend(parameters);
            return new AbstractKeyedStateBackend<K>(
                    parameters.getKvStateRegistry(),
                    parameters.getKeySerializer(),
                    parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                    parameters.getEnv().getExecutionConfig(),
                    parameters.getTtlTimeProvider(),
                    delegatedKeyedStateBackend.getLatencyTrackingStateConfig(),
                    parameters.getCancelStreamRegistry(),
                    delegatedKeyedStateBackend.getKeyContext()) {
                @Override
                public void setCurrentKey(K newKey) {
                    delegatedKeyedStateBackend.setCurrentKey(newKey);
                }

                @Override
                public void notifyCheckpointComplete(long checkpointId) throws Exception {
                    delegatedKeyedStateBackend.notifyCheckpointComplete(checkpointId);
                }

                @Nonnull
                @Override
                public SavepointResources<K> savepoint() throws Exception {
                    return delegatedKeyedStateBackend.savepoint();
                }

                @Override
                public int numKeyValueStateEntries() {
                    return delegatedKeyedStateBackend.numKeyValueStateEntries();
                }

                @Override
                public <N> Stream<K> getKeys(String state, N namespace) {
                    return delegatedKeyedStateBackend.getKeys(state, namespace);
                }

                @Override
                public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
                    return delegatedKeyedStateBackend.getKeysAndNamespaces(state);
                }

                @Nonnull
                @Override
                public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
                        @Nonnull TypeSerializer<N> namespaceSerializer,
                        @Nonnull StateDescriptor<S, SV> stateDesc,
                        @Nonnull
                                StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                                        snapshotTransformFactory)
                        throws Exception {
                    return delegatedKeyedStateBackend.createOrUpdateInternalState(
                            namespaceSerializer, stateDesc, snapshotTransformFactory);
                }

                @Nonnull
                @Override
                public <
                                T extends
                                        HeapPriorityQueueElement & PriorityComparable<? super T>
                                                & Keyed<?>>
                        KeyGroupedInternalPriorityQueue<T> create(
                                @Nonnull String stateName,
                                @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
                    return delegatedKeyedStateBackend.create(
                            stateName, byteOrderedElementSerializer);
                }

                @Nonnull
                @Override
                public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                        long checkpointId,
                        long timestamp,
                        @Nonnull CheckpointStreamFactory streamFactory,
                        @Nonnull CheckpointOptions checkpointOptions)
                        throws Exception {
                    RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotResultRunnableFuture =
                            delegatedKeyedStateBackend.snapshot(
                                    checkpointId, timestamp, streamFactory, checkpointOptions);
                    return snapshotResultFunction.apply(snapshotResultRunnableFuture);
                }

                @Override
                public void dispose() {
                    super.dispose();
                    delegatedKeyedStateBackend.dispose();
                }

                @Override
                public void close() throws IOException {
                    super.close();
                    delegatedKeyedStateBackend.close();
                }
            };
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            return delegatedStataBackend.createOperatorStateBackend(parameters);
        }
    }

    /** A FunctionWithException supports serialization. */
    @FunctionalInterface
    public interface SerializableFunctionWithException<T>
            extends FunctionWithException<T, T, Exception>, Serializable {}

    // ------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private StateBackendTestUtils() {}

    // ------------------------------------------------------------------------

}
