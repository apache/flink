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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.runtime.state.KeyValueStateIterator} over Heap backend snapshot
 * resources.
 */
@Internal
@NotThreadSafe
public final class HeapKeyValueStateIterator implements KeyValueStateIterator {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final Map<StateUID, Integer> stateNamesToId;
    private final Map<StateUID, StateSnapshot> stateStableSnapshots;
    private final int keyGroupPrefixBytes;

    private boolean isValid;
    private boolean newKeyGroup;
    private boolean newKVState;
    private byte[] currentKey;
    private byte[] currentValue;

    /** Iterator over the key groups of the corresponding key group range. */
    private final Iterator<Integer> keyGroupIterator;
    /** The current value of the keyGroupIterator. */
    private int currentKeyGroup;

    /** Iterator over all states present in the snapshots. */
    private Iterator<StateUID> statesIterator;
    /** The current value of the statesIterator. */
    private StateUID currentState;
    /**
     * An iterator over the values of the current state. It can be one of three:
     *
     * <ul>
     *   <li>{@link QueueIterator} for iterating over entries in a priority queue
     *   <li>{@link StateTableIterator} for iterating over entries in a StateTable
     *   <li>{@link MapStateIterator} for iterating over entries in a user map, this one falls back
     *       to the upper one automatically if exhausted
     * </ul>
     */
    private SingleStateIterator currentStateIterator;
    /** Helpers for serializing state into the unified format. */
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);

    private final ListDelimitedSerializer listDelimitedSerializer = new ListDelimitedSerializer();
    private final SerializedCompositeKeyBuilder<Object> compositeKeyBuilder;

    public HeapKeyValueStateIterator(
            @Nonnull final KeyGroupRange keyGroupRange,
            @Nonnull final TypeSerializer<?> keySerializer,
            @Nonnegative final int totalKeyGroups,
            @Nonnull final Map<StateUID, Integer> stateNamesToId,
            @Nonnull final Map<StateUID, StateSnapshot> stateSnapshots)
            throws IOException {
        checkNotNull(keyGroupRange);
        checkNotNull(keySerializer);
        this.stateNamesToId = checkNotNull(stateNamesToId);
        this.stateStableSnapshots = checkNotNull(stateSnapshots);

        this.statesIterator = stateSnapshots.keySet().iterator();
        this.keyGroupIterator = keyGroupRange.iterator();

        this.keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
        this.compositeKeyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        castToType(keySerializer), keyGroupPrefixBytes, 32);

        if (!keyGroupIterator.hasNext() || !statesIterator.hasNext()) {
            // stop early, no key groups or states
            isValid = false;
        } else {
            currentKeyGroup = keyGroupIterator.next();
            next();
            this.newKeyGroup = true;
        }
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public boolean isNewKeyValueState() {
        return this.newKVState;
    }

    @Override
    public boolean isNewKeyGroup() {
        return this.newKeyGroup;
    }

    @Override
    public int keyGroup() {
        return currentKeyGroup;
    }

    @Override
    public int kvStateId() {
        return stateNamesToId.get(currentState);
    }

    @Override
    public void next() throws IOException {
        this.newKVState = false;
        this.newKeyGroup = false;

        boolean nextElementSet = false;
        do {
            if (currentState == null) {
                boolean hasNextState = moveToNextState();
                if (!hasNextState) {
                    isValid = false;
                    return;
                }
            }

            boolean hasStateEntry = currentStateIterator != null && currentStateIterator.hasNext();
            if (!hasStateEntry) {
                this.currentState = null;
            }

            if (hasStateEntry) {
                nextElementSet = currentStateIterator.writeOutNext();
            }
        } while (!nextElementSet);
        isValid = true;
    }

    private boolean moveToNextState() throws IOException {
        if (statesIterator.hasNext()) {
            this.currentState = statesIterator.next();
            this.newKVState = true;
        } else if (keyGroupIterator.hasNext()) {
            this.currentKeyGroup = keyGroupIterator.next();
            resetStates();
            this.newKeyGroup = true;
            this.newKVState = true;
        } else {
            return false;
        }

        StateSnapshot stateSnapshot = this.stateStableSnapshots.get(currentState);
        setCurrentStateIterator(stateSnapshot);

        // set to a valid entry
        return true;
    }

    private void resetStates() {
        this.statesIterator = stateStableSnapshots.keySet().iterator();
        this.currentState = statesIterator.next();
    }

    @SuppressWarnings("unchecked")
    private void setCurrentStateIterator(StateSnapshot stateSnapshot) throws IOException {
        if (stateSnapshot instanceof IterableStateSnapshot) {
            RegisteredKeyValueStateBackendMetaInfo<Object, Object> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateSnapshot.getMetaInfoSnapshot());
            Iterator<? extends StateEntry<?, ?, ?>> snapshotIterator =
                    ((IterableStateSnapshot<?, ?, ?>) stateSnapshot).getIterator(currentKeyGroup);
            this.currentStateIterator = new StateTableIterator(snapshotIterator, metaInfo);
        } else if (stateSnapshot instanceof HeapPriorityQueueStateSnapshot) {
            Iterator<Object> snapshotIterator =
                    ((HeapPriorityQueueStateSnapshot<Object>) stateSnapshot)
                            .getIteratorForKeyGroup(currentKeyGroup);
            RegisteredPriorityQueueStateBackendMetaInfo<Object> metaInfo =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(
                            stateSnapshot.getMetaInfoSnapshot());
            this.currentStateIterator = new QueueIterator<>(snapshotIterator, metaInfo);
        } else {
            throw new IllegalStateException("Unknown snapshot type: " + stateSnapshot);
        }
    }

    /** A common interface for writing out a single entry in a state. */
    private interface SingleStateIterator {

        boolean hasNext();

        /**
         * Sets the {@link #currentKey} and {@link #currentValue} to the value of the next entry in
         * the state.
         *
         * @return false if an entry was empty. It can be the case if we try to serialize an empty
         *     Map or List. In that case we should skip to a next entry.
         */
        boolean writeOutNext() throws IOException;
    }

    private final class StateTableIterator implements SingleStateIterator {

        private final Iterator<? extends StateEntry<?, ?, ?>> entriesIterator;
        private final RegisteredKeyValueStateBackendMetaInfo<?, ?> stateSnapshot;

        private StateTableIterator(
                Iterator<? extends StateEntry<?, ?, ?>> entriesIterator,
                RegisteredKeyValueStateBackendMetaInfo<?, ?> stateSnapshot) {
            this.entriesIterator = entriesIterator;
            this.stateSnapshot = stateSnapshot;
        }

        @Override
        public boolean hasNext() {
            return entriesIterator.hasNext();
        }

        @Override
        public boolean writeOutNext() throws IOException {
            StateEntry<?, ?, ?> currentEntry = entriesIterator.next();
            valueOut.clear();
            compositeKeyBuilder.setKeyAndKeyGroup(currentEntry.getKey(), keyGroup());
            compositeKeyBuilder.setNamespace(
                    currentEntry.getNamespace(),
                    castToType(stateSnapshot.getNamespaceSerializer()));
            TypeSerializer<?> stateSerializer = stateSnapshot.getStateSerializer();
            switch (stateSnapshot.getStateType()) {
                case AGGREGATING:
                case REDUCING:
                case FOLDING:
                case VALUE:
                    return writeOutValue(currentEntry, stateSerializer);
                case LIST:
                    return writeOutList(currentEntry, stateSerializer);
                case MAP:
                    return writeOutMap(currentEntry, stateSerializer);
                default:
                    throw new IllegalStateException("");
            }
        }

        private boolean writeOutValue(
                StateEntry<?, ?, ?> currentEntry, TypeSerializer<?> stateSerializer)
                throws IOException {
            currentKey = compositeKeyBuilder.build();
            castToType(stateSerializer).serialize(currentEntry.getState(), valueOut);
            currentValue = valueOut.getCopyOfBuffer();
            return true;
        }

        @SuppressWarnings("unchecked")
        private boolean writeOutList(
                StateEntry<?, ?, ?> currentEntry, TypeSerializer<?> stateSerializer)
                throws IOException {
            List<Object> state = (List<Object>) currentEntry.getState();
            if (state.isEmpty()) {
                return false;
            }
            ListSerializer<Object> listSerializer = (ListSerializer<Object>) stateSerializer;
            currentKey = compositeKeyBuilder.build();
            currentValue =
                    listDelimitedSerializer.serializeList(
                            state, listSerializer.getElementSerializer());
            return true;
        }

        @SuppressWarnings("unchecked")
        private boolean writeOutMap(
                StateEntry<?, ?, ?> currentEntry, TypeSerializer<?> stateSerializer)
                throws IOException {
            Map<Object, Object> state = (Map<Object, Object>) currentEntry.getState();
            if (state.isEmpty()) {
                return false;
            }
            MapSerializer<Object, Object> mapSerializer =
                    (MapSerializer<Object, Object>) stateSerializer;
            currentStateIterator =
                    new MapStateIterator(
                            state,
                            mapSerializer.getKeySerializer(),
                            mapSerializer.getValueSerializer(),
                            this);
            return currentStateIterator.writeOutNext();
        }
    }

    private final class MapStateIterator implements SingleStateIterator {

        private final Iterator<Map.Entry<Object, Object>> mapEntries;
        private final TypeSerializer<Object> userKeySerializer;
        private final TypeSerializer<Object> userValueSerializer;
        private final StateTableIterator parentIterator;

        private MapStateIterator(
                Map<Object, Object> mapEntries,
                TypeSerializer<Object> userKeySerializer,
                TypeSerializer<Object> userValueSerializer,
                StateTableIterator parentIterator) {
            assert !mapEntries.isEmpty();
            this.mapEntries = mapEntries.entrySet().iterator();
            this.userKeySerializer = userKeySerializer;
            this.userValueSerializer = userValueSerializer;
            this.parentIterator = parentIterator;
        }

        @Override
        public boolean hasNext() {
            // we should never end up here with an exhausted map iterator
            // if an iterator is exhausted in the writeOutNext we switch back to
            // the originating StateTableIterator
            assert mapEntries.hasNext();
            return true;
        }

        @Override
        public boolean writeOutNext() throws IOException {
            Map.Entry<Object, Object> entry = mapEntries.next();
            valueOut.clear();
            currentKey =
                    compositeKeyBuilder.buildCompositeKeyUserKey(entry.getKey(), userKeySerializer);
            Object userValue = entry.getValue();
            valueOut.writeBoolean(userValue == null);
            userValueSerializer.serialize(userValue, valueOut);
            currentValue = valueOut.getCopyOfBuffer();

            if (!mapEntries.hasNext()) {
                currentStateIterator = parentIterator;
            }
            return true;
        }
    }

    private final class QueueIterator<T> implements SingleStateIterator {
        private final Iterator<T> elementsForKeyGroup;
        private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;
        private final DataOutputSerializer keyOut = new DataOutputSerializer(128);
        private final int afterKeyMark;

        public QueueIterator(
                Iterator<T> elementsForKeyGroup,
                RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo)
                throws IOException {
            this.elementsForKeyGroup = elementsForKeyGroup;
            this.metaInfo = metaInfo;
            CompositeKeySerializationUtils.writeKeyGroup(keyGroup(), keyGroupPrefixBytes, keyOut);
            afterKeyMark = keyOut.length();
        }

        @Override
        public boolean hasNext() {
            return elementsForKeyGroup.hasNext();
        }

        @Override
        public boolean writeOutNext() throws IOException {
            currentValue = EMPTY_BYTE_ARRAY;
            keyOut.setPosition(afterKeyMark);
            T next = elementsForKeyGroup.next();
            metaInfo.getElementSerializer().serialize(next, keyOut);
            currentKey = keyOut.getCopyOfBuffer();
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private static <T> TypeSerializer<T> castToType(@Nonnull TypeSerializer<?> serializer) {
        return (TypeSerializer<T>) serializer;
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return currentValue;
    }

    @Override
    public void close() {}
}
