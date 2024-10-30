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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.StateDescriptorUtils;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;

/**
 * A adaptor that transforms {@link KeyedStateBackend} into {@link AsyncKeyedStateBackend}.
 *
 * @param <K> The key by which state is keyed.
 */
public class AsyncKeyedStateBackendAdaptor<K> implements AsyncKeyedStateBackend<K> {
    private final CheckpointableKeyedStateBackend<K> keyedStateBackend;

    public AsyncKeyedStateBackendAdaptor(CheckpointableKeyedStateBackend<K> keyedStateBackend) {
        this.keyedStateBackend = keyedStateBackend;
    }

    @Override
    public void setup(@Nonnull StateRequestHandler stateRequestHandler) {}

    @Nonnull
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <N, S extends State, SV> S createState(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<SV> stateDesc)
            throws Exception {
        return createStateInternal(defaultNamespace, namespaceSerializer, stateDesc);
    }

    @Nonnull
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <N, S extends InternalKeyedState, SV> S createStateInternal(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<SV> stateDesc)
            throws Exception {
        org.apache.flink.api.common.state.StateDescriptor rawStateDesc =
                StateDescriptorUtils.transformFromV2ToV1(stateDesc);
        org.apache.flink.api.common.state.State rawState =
                keyedStateBackend.getPartitionedState(
                        defaultNamespace, namespaceSerializer, rawStateDesc);
        switch (rawStateDesc.getType()) {
            case VALUE:
                return (S) new ValueStateAdaptor((InternalValueState) rawState);
            case LIST:
                return (S) new ListStateAdaptor<>((InternalListState) rawState);
            case REDUCING:
                return (S) new ReducingStateAdaptor<>((InternalReducingState) rawState);
            case AGGREGATING:
                return (S) new AggregatingStateAdaptor<>((InternalAggregatingState) rawState);
            case MAP:
                return (S) new MapStateAdaptor<>((InternalMapState) rawState);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported state type: %s", rawStateDesc.getType()));
        }
    }

    @Nonnull
    @Override
    public StateExecutor createStateExecutor() {
        return null;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyedStateBackend.getKeyGroupRange();
    }

    @Override
    public void switchContext(RecordContext<K> context) {
        keyedStateBackend.setCurrentKeyAndKeyGroup(context.getKey(), context.getKeyGroup());
    }

    @Override
    public void dispose() {}

    @Override
    public void close() throws IOException {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (keyedStateBackend instanceof CheckpointListener) {
            ((CheckpointListener) keyedStateBackend).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (keyedStateBackend instanceof CheckpointListener) {
            ((CheckpointListener) keyedStateBackend).notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        if (keyedStateBackend instanceof InternalCheckpointListener) {
            ((InternalCheckpointListener) keyedStateBackend).notifyCheckpointSubsumed(checkpointId);
        }
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        return keyedStateBackend.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        return keyedStateBackend.create(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        if (keyedStateBackend instanceof AbstractKeyedStateBackend) {
            return ((AbstractKeyedStateBackend) keyedStateBackend)
                    .requiresLegacySynchronousTimerSnapshots(checkpointType);
        }
        return false;
    }
}
