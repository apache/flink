/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog.restore;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.changelog.ChangelogKeyGroupedPriorityQueue;
import org.apache.flink.state.changelog.ChangelogState;
import org.apache.flink.state.changelog.ChangelogStateFactory;
import org.apache.flink.state.changelog.KvStateChangeLogger;
import org.apache.flink.state.changelog.StateChangeLogger;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform;

/** A {@link ChangelogRestoreTarget} supports to migrate to the delegated keyed state backend. */
public class ChangelogMigrationRestoreTarget<K> implements ChangelogRestoreTarget<K> {

    private final AbstractKeyedStateBackend<K> keyedStateBackend;

    private final ChangelogStateFactory changelogStateFactory;

    private final FunctionDelegationHelper functionDelegationHelper =
            new FunctionDelegationHelper();

    public ChangelogMigrationRestoreTarget(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            ChangelogStateFactory changelogStateFactory) {
        this.keyedStateBackend = keyedStateBackend;
        this.changelogStateFactory = changelogStateFactory;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyedStateBackend.getKeyGroupRange();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, V> S createKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        InternalKvState<K, N, V> kvState =
                keyedStateBackend.createOrUpdateInternalState(
                        namespaceSerializer, stateDescriptor, noTransform(), true);
        ChangelogState changelogState =
                changelogStateFactory.getExistingState(
                        stateDescriptor.getName(),
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
        if (changelogState == null) {
            changelogState =
                    changelogStateFactory.create(
                            stateDescriptor,
                            kvState,
                            VoidStateChangeLogger.getInstance(),
                            keyedStateBackend /* pass the nested backend as key context so that it get key updates on recovery*/);
        } else {
            changelogState.setDelegatedState(kvState);
        }
        functionDelegationHelper.addOrUpdate(stateDescriptor);
        return (S) changelogState;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> createPqState(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        KeyGroupedInternalPriorityQueue<T> internalPriorityQueue =
                keyedStateBackend.create(stateName, byteOrderedElementSerializer, true);
        ChangelogKeyGroupedPriorityQueue<T> queue =
                (ChangelogKeyGroupedPriorityQueue<T>)
                        changelogStateFactory.getExistingState(
                                stateName, StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);
        if (queue == null) {
            queue =
                    changelogStateFactory.create(
                            stateName,
                            internalPriorityQueue,
                            VoidStateChangeLogger.getInstance(),
                            byteOrderedElementSerializer);
        } else {
            queue.setDelegatedState(internalPriorityQueue);
        }
        return queue;
    }

    @Override
    public ChangelogState getExistingState(
            String name, StateMetaInfoSnapshot.BackendStateType type) {
        return changelogStateFactory.getExistingState(name, type);
    }

    @Override
    public CheckpointableKeyedStateBackend<K> getRestoredKeyedStateBackend() {
        return wrapKeyedStateBackend(
                keyedStateBackend, changelogStateFactory, functionDelegationHelper);
    }

    private static class VoidStateChangeLogger<Value, Namespace>
            implements KvStateChangeLogger<Value, Namespace>, StateChangeLogger<Value, Namespace> {

        private static final VoidStateChangeLogger<Object, Object> INSTANCE =
                new VoidStateChangeLogger<>();

        @SuppressWarnings("unchecked")
        public static <Value, Namespace> VoidStateChangeLogger<Value, Namespace> getInstance() {
            return (VoidStateChangeLogger<Value, Namespace>) INSTANCE;
        }

        private VoidStateChangeLogger() {}

        @Override
        public void namespacesMerged(Namespace target, Collection<Namespace> sources)
                throws IOException {}

        @Override
        public void valueUpdated(Value newValue, Namespace ns) throws IOException {}

        @Override
        public void valueUpdatedInternal(Value newValue, Namespace ns) throws IOException {}

        @Override
        public void valueAdded(Value addedValue, Namespace ns) throws IOException {}

        @Override
        public void valueCleared(Namespace ns) throws IOException {}

        @Override
        public void valueElementAdded(
                ThrowingConsumer<DataOutputView, IOException> dataSerializer, Namespace ns)
                throws IOException {}

        @Override
        public void valueElementAddedOrUpdated(
                ThrowingConsumer<DataOutputView, IOException> dataSerializer, Namespace ns)
                throws IOException {}

        @Override
        public void valueElementRemoved(
                ThrowingConsumer<DataOutputView, IOException> dataSerializer, Namespace ns)
                throws IOException {}

        @Override
        public void resetWritingMetaFlag() {}

        @Override
        public void close() throws IOException {}
    }

    private static <K> AbstractKeyedStateBackend<K> wrapKeyedStateBackend(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            ChangelogStateFactory changelogStateFactory,
            FunctionDelegationHelper functionDelegationHelper) {
        return new AbstractKeyedStateBackend<K>(keyedStateBackend) {

            @Override
            public void setCurrentKey(K newKey) {
                keyedStateBackend.setCurrentKey(newKey);
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) throws Exception {
                keyedStateBackend.notifyCheckpointComplete(checkpointId);
            }

            @Nonnull
            @Override
            public SavepointResources<K> savepoint() throws Exception {
                return keyedStateBackend.savepoint();
            }

            @Override
            public int numKeyValueStateEntries() {
                return keyedStateBackend.numKeyValueStateEntries();
            }

            @Override
            public <N> Stream<K> getKeys(String state, N namespace) {
                return keyedStateBackend.getKeys(state, namespace);
            }

            @Override
            public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
                return keyedStateBackend.getKeysAndNamespaces(state);
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
                return keyedStateBackend.createOrUpdateInternalState(
                        namespaceSerializer, stateDesc, snapshotTransformFactory);
            }

            @Override
            public <N, S extends State> S getPartitionedState(
                    N namespace,
                    TypeSerializer<N> namespaceSerializer,
                    StateDescriptor<S, ?> stateDescriptor)
                    throws Exception {
                S partitionedState =
                        super.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
                functionDelegationHelper.addOrUpdate(stateDescriptor);
                return partitionedState;
            }

            @Override
            public <N, S extends State, V> S getOrCreateKeyedState(
                    TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
                    throws Exception {
                S keyedState = super.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
                functionDelegationHelper.addOrUpdate(stateDescriptor);
                return keyedState;
            }

            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                    KeyGroupedInternalPriorityQueue<T> create(
                            @Nonnull String stateName,
                            @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
                return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
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

            @Override
            public void dispose() {
                keyedStateBackend.dispose();
                changelogStateFactory.dispose();
            }
        };
    }
}
