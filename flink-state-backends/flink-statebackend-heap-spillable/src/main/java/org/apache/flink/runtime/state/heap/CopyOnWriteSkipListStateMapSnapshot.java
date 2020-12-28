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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.ResourceGuard;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.flink.runtime.state.heap.SkipListUtils.HEAD_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_VALUE_POINTER;

/**
 * This class represents the snapshot of a {@link CopyOnWriteSkipListStateMap}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class CopyOnWriteSkipListStateMapSnapshot<K, N, S>
        extends StateMapSnapshot<K, N, S, CopyOnWriteSkipListStateMap<K, N, S>> {

    /**
     * Version of the {@link CopyOnWriteSkipListStateMap} when this snapshot was created. This can
     * be used to release the snapshot.
     */
    private final int snapshotVersion;

    /** The number of (non-null) entries in snapshotData. */
    @Nonnegative private final int numberOfEntriesInSnapshotData;

    /** This lease protects the state map resources. */
    private final ResourceGuard.Lease lease;

    /**
     * Creates a new {@link CopyOnWriteSkipListStateMap}.
     *
     * @param owningStateMap the {@link CopyOnWriteSkipListStateMap} for which this object
     *     represents a snapshot.
     * @param lease the lease protects the state map resources.
     */
    CopyOnWriteSkipListStateMapSnapshot(
            CopyOnWriteSkipListStateMap<K, N, S> owningStateMap, ResourceGuard.Lease lease) {
        super(owningStateMap);

        this.snapshotVersion = owningStateMap.getStateMapVersion();
        this.numberOfEntriesInSnapshotData = owningStateMap.size();
        this.lease = lease;
    }

    /** Returns the internal version of the when this snapshot was created. */
    int getSnapshotVersion() {
        return snapshotVersion;
    }

    @Override
    public void release() {
        owningStateMap.releaseSnapshot(this);
        lease.close();
    }

    @Override
    public void writeState(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer,
            @Nonnull DataOutputView dov,
            @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer)
            throws IOException {
        if (stateSnapshotTransformer == null) {
            writeStateWithNoTransform(dov);
        } else {
            writeStateWithTransform(stateSerializer, dov, stateSnapshotTransformer);
        }
    }

    private void writeStateWithNoTransform(@Nonnull DataOutputView dov) throws IOException {
        dov.writeInt(numberOfEntriesInSnapshotData);
        SnapshotNodeIterator nodeIterator = new SnapshotNodeIterator(true);
        while (nodeIterator.hasNext()) {
            Tuple2<Long, Long> tuple = nodeIterator.next();
            writeKeyAndNamespace(tuple.f0, dov);
            writeValue(tuple.f1, dov);
        }
    }

    private void writeStateWithTransform(
            TypeSerializer<S> stateSerializer,
            @Nonnull DataOutputView dov,
            @Nonnull StateSnapshotTransformer<S> stateSnapshotTransformer)
            throws IOException {
        SkipListValueSerializer<S> skipListValueSerializer =
                new SkipListValueSerializer<>(stateSerializer);

        // 1. iterates nodes to get size after transform
        SnapshotNodeIterator transformNodeIterator = new SnapshotNodeIterator(true);
        int size = 0;
        while (transformNodeIterator.hasNext()) {
            Tuple2<Long, Long> tuple = transformNodeIterator.next();
            S oldState = owningStateMap.helpGetState(tuple.f1, skipListValueSerializer);
            S newState = stateSnapshotTransformer.filterOrTransform(oldState);
            if (newState != null) {
                size++;
            }
        }

        dov.writeInt(size);

        // 2. iterates nodes again to write them to output, and there is no need to prune
        SnapshotNodeIterator writeNodeIterator = new SnapshotNodeIterator(false);
        while (writeNodeIterator.hasNext()) {
            Tuple2<Long, Long> tuple = writeNodeIterator.next();
            S oldState = owningStateMap.helpGetState(tuple.f1, skipListValueSerializer);
            S newState = stateSnapshotTransformer.filterOrTransform(oldState);
            if (newState != null) {
                writeKeyAndNamespace(tuple.f0, dov);
                stateSerializer.serialize(newState, dov);
            }
        }
    }

    /** Write key and namespace from bytes. */
    private void writeKeyAndNamespace(long nodeId, DataOutputView outputView) throws IOException {
        // tuple of byte arrays for key and namespace
        Tuple2<byte[], byte[]> tuple = owningStateMap.helpGetBytesForKeyAndNamespace(nodeId);
        // write namespace first
        outputView.write(tuple.f1);
        outputView.write(tuple.f0);
    }

    /** Write value from bytes. */
    private void writeValue(long valuePointer, DataOutputView outputView) throws IOException {
        outputView.write(owningStateMap.helpGetBytesForState(valuePointer));
    }

    /**
     * Iterates over all nodes used by this snapshot. The iterator will return a tuple, and f0 is
     * the node and f1 is the value pointer.
     */
    class SnapshotNodeIterator implements Iterator<Tuple2<Long, Long>> {

        /** Whether to prune values during iteration. */
        private boolean isPrune;

        private long nextNode;
        private long nextValuePointer;

        SnapshotNodeIterator(boolean isPrune) {
            this.isPrune = isPrune;
            this.nextNode = HEAD_NODE;
            advance();
        }

        private void advance() {
            if (nextNode == NIL_NODE) {
                return;
            }

            long node = owningStateMap.helpGetNextNode(nextNode, 0);
            long valuePointer = NIL_VALUE_POINTER;
            while (node != NIL_NODE) {
                valuePointer =
                        isPrune
                                ? owningStateMap.getAndPruneValueForSnapshot(node, snapshotVersion)
                                : owningStateMap.getValueForSnapshot(node, snapshotVersion);
                int valueLen =
                        valuePointer == NIL_VALUE_POINTER
                                ? 0
                                : owningStateMap.helpGetValueLen(valuePointer);
                // for a logically removed node, it's value length will be 0
                if (valueLen != 0) {
                    break;
                }
                node = owningStateMap.helpGetNextNode(node, 0);
            }

            nextNode = node;
            nextValuePointer = valuePointer;
        }

        @Override
        public boolean hasNext() {
            return nextNode != NIL_NODE;
        }

        @Override
        public Tuple2<Long, Long> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            long node = nextNode;
            long valuePointer = nextValuePointer;
            advance();

            return Tuple2.of(node, valuePointer);
        }
    }
}
