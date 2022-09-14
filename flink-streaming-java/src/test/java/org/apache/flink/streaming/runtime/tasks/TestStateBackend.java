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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/** Testable state backend. */
public class TestStateBackend extends AbstractStateBackend {
    private static final long serialVersionUID = 1L;

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
        return new TestKeyedStateBackend<>(
                kvStateRegistry,
                keySerializer,
                Thread.currentThread().getContextClassLoader(),
                env.getExecutionConfig(),
                ttlTimeProvider,
                LatencyTrackingStateConfig.newBuilder().build(),
                cancelStreamRegistry,
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups));
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return new DefaultOperatorStateBackend(
                env.getExecutionConfig(),
                cancelStreamRegistry,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                new SnapshotStrategyRunner<>(
                        "Async Failure State Backend",
                        new SnapshotStrategy<OperatorStateHandle, SnapshotResources>() {

                            @Override
                            public SnapshotResources syncPrepareResources(long checkpointId) {
                                return null;
                            }

                            @Override
                            public SnapshotResultSupplier<OperatorStateHandle> asyncSnapshot(
                                    SnapshotResources syncPartResource,
                                    long checkpointId,
                                    long timestamp,
                                    @Nonnull CheckpointStreamFactory streamFactory,
                                    @Nonnull CheckpointOptions checkpointOptions) {
                                return null;
                            }
                        },
                        new CloseableRegistry(),
                        SnapshotExecutionType.ASYNCHRONOUS));
    }

    /**
     * Testable keyed state backend for {@link
     * SubtaskCheckpointCoordinatorTest#testNotifyCheckpointSubsumed()}.
     */
    static class TestKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
        private long subsumeCheckpointId = -1L;

        public TestKeyedStateBackend(
                TaskKvStateRegistry kvStateRegistry,
                TypeSerializer<K> keySerializer,
                ClassLoader userCodeClassLoader,
                ExecutionConfig executionConfig,
                TtlTimeProvider ttlTimeProvider,
                LatencyTrackingStateConfig latencyTrackingStateConfig,
                CloseableRegistry cancelStreamRegistry,
                InternalKeyContext<K> keyContext) {
            super(
                    kvStateRegistry,
                    keySerializer,
                    userCodeClassLoader,
                    executionConfig,
                    ttlTimeProvider,
                    latencyTrackingStateConfig,
                    cancelStreamRegistry,
                    keyContext);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointSubsumed(long checkpointId) {
            subsumeCheckpointId = checkpointId;
        }

        public long getSubsumeCheckpointId() {
            return subsumeCheckpointId;
        }

        @Nonnull
        @Override
        public SavepointResources<K> savepoint() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N> Stream<K> getKeys(String state, N namespace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @Nonnull String stateName,
                        @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                @Nonnull CheckpointStreamFactory streamFactory,
                @Nonnull CheckpointOptions checkpointOptions)
                throws Exception {
            return new FutureTask<>(SnapshotResult::empty);
        }

        @Override
        public int numKeyValueStateEntries() {
            return 0;
        }
    }
}
