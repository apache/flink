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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.RunnableFuture;
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
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws IOException {
            AbstractKeyedStateBackend<K> delegatedKeyedStateBackend =
                    delegatedStataBackend.createKeyedStateBackend(
                            env,
                            jobID,
                            operatorIdentifier,
                            keySerializer,
                            numberOfKeyGroups,
                            keyGroupRange,
                            kvStateRegistry,
                            ttlTimeProvider,
                            metricGroup,
                            stateHandles,
                            cancelStreamRegistry);
            return new AbstractKeyedStateBackend<K>(
                    kvStateRegistry,
                    keySerializer,
                    env.getUserCodeClassLoader().asClassLoader(),
                    env.getExecutionConfig(),
                    ttlTimeProvider,
                    delegatedKeyedStateBackend.getLatencyTrackingStateConfig(),
                    cancelStreamRegistry,
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
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return delegatedStataBackend.createOperatorStateBackend(
                    env, operatorIdentifier, stateHandles, cancelStreamRegistry);
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
