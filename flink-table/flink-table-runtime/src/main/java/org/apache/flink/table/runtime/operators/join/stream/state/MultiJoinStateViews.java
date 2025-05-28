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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.IterableIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Factory class to create different implementations of {@link MultiJoinStateView} based on the
 * characteristics described in {@link JoinInputSideSpec}.
 *
 * <p>Each state view uses a {@link MapState} where the primary key is the `joinKey` derived from
 * the join conditions (via {@link
 * org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor}). The value
 * stored within this map depends on whether the input side has a unique key and how it relates to
 * the join key, optimizing storage and access patterns.
 */
public final class MultiJoinStateViews {

    /** Creates a {@link MultiJoinStateView} depends on {@link JoinInputSideSpec}. */
    public static MultiJoinStateView create(
            RuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            @Nullable
                    RowType
                            joinKeyType, /* joinKeyType is null for inputId = 0, see {@link InputSideHasUniqueKey}*/
            RowType recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);

        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey() && joinKeyType != null) {
                return new JoinKeyContainsUniqueKey(
                        ctx, stateName, joinKeyType, recordType, ttlConfig);
            } else {
                return new InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        joinKeyType,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new InputSideHasNoUniqueKey(ctx, stateName, joinKeyType, recordType, ttlConfig);
        }
    }

    /**
     * Creates a {@link MapStateDescriptor} with the given parameters and applies TTL configuration.
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param stateName Unique name for the state
     * @param keyTypeInfo Type information for the key
     * @param valueTypeInfo Type information for the value
     * @param ttlConfig State TTL configuration
     * @return Configured MapStateDescriptor
     */
    private static <K, V> MapStateDescriptor<K, V> createStateDescriptor(
            String stateName,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo,
            StateTtlConfig ttlConfig) {
        MapStateDescriptor<K, V> descriptor =
                new MapStateDescriptor<>(stateName, keyTypeInfo, valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        return descriptor;
    }

    // ------------------------------------------------------------------------------------
    // Multi Join State View Implementations
    // ------------------------------------------------------------------------------------

    /**
     * State view for input sides where the unique key is fully contained within the join key.
     *
     * <p>Stores data as {@code MapState<JoinKey, Record>}.
     */
    private static final class JoinKeyContainsUniqueKey implements MultiJoinStateView {

        // stores record in the mapping <JoinKey, Record>
        private final MapState<RowData, RowData> recordState;
        private final List<RowData> reusedList;

        private JoinKeyContainsUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final RowType joinKeyType,
                final RowType recordType,
                final StateTtlConfig ttlConfig) {
            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    createStateDescriptor(
                            stateName,
                            InternalTypeInfo.of(joinKeyType),
                            InternalTypeInfo.of(recordType),
                            ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
            // the result records always not more than 1 per joinKey
            this.reusedList = new ArrayList<>(1);
        }

        @Override
        public void addRecord(RowData joinKey, RowData record) throws Exception {
            recordState.put(joinKey, record);
        }

        @Override
        public void retractRecord(RowData joinKey, RowData record) throws Exception {
            recordState.remove(joinKey);
        }

        @Override
        public Iterable<RowData> getRecords(RowData joinKey) throws Exception {
            reusedList.clear();
            RowData record = recordState.get(joinKey);
            if (record != null) {
                reusedList.add(record);
            }
            return reusedList;
        }
    }

    /**
     * State view for input sides that have a unique key, but it differs from the join key.
     *
     * <p>Stores data as {@code MapState<CompositeKey<JoinKey, UK>, Record>}. The composite key is a
     * RowData with 2 fields: joinKey and uniqueKey.
     *
     * <p>For Input 0, stores data as {@code MapState<UK, Record>}. Necessary since we don't have
     * the join key value when iterating through input 0 to use it, since there is no left side. For
     * the next inputs, currentRecords can be used to derive the join key.
     */
    private static final class InputSideHasUniqueKey implements MultiJoinStateView {
        private final MapState<RowData, RowData> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;
        private RowDataSerializer joinKeySerializer; // Null if joinKeyType is null
        private int joinKeyFieldCount = 0; // 0 if joinKeyType is null

        private InputSideHasUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                @Nullable final RowType joinKeyType, // Can be null
                final RowType recordType,
                final InternalTypeInfo<RowData> uniqueKeyType,
                final KeySelector<RowData, RowData> uniqueKeySelector,
                final StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            this.uniqueKeySelector = uniqueKeySelector;

            // If joinKeyType is null, state will be keyed by UniqueKey.
            // Otherwise, it's keyed by CompositeKey<JoinKey, UniqueKey>.
            InternalTypeInfo<RowData> keyStateType;
            if (joinKeyType == null) {
                // State key is just the unique key
                keyStateType = uniqueKeyType;
            } else {
                this.joinKeySerializer = new RowDataSerializer(joinKeyType);
                this.joinKeyFieldCount = joinKeyType.getFieldCount();
                // Composite key type: RowData with 2 fields (joinKey, uniqueKey)
                // The composite key is a RowData with joinKey at index 0 and uniqueKey at index 1.
                final RowType keyRowType = RowType.of(joinKeyType, uniqueKeyType.toRowType());
                keyStateType = InternalTypeInfo.of(keyRowType);
            }

            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    createStateDescriptor(
                            stateName, keyStateType, InternalTypeInfo.of(recordType), ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        private boolean joinKeysEqual(RowData joinKey, RowData currentJoinKeyInState) {
            BinaryRowData binaryJoinKey = joinKeySerializer.toBinaryRow(joinKey);
            BinaryRowData binaryCurrJoinKey = joinKeySerializer.toBinaryRow(currentJoinKeyInState);
            return binaryJoinKey.equals(binaryCurrJoinKey);
        }

        private RowData getStateKey(RowData joinKey, RowData uniqueKey) {
            if (joinKey == null) {
                return uniqueKey;
            } else {
                GenericRowData compositeKey = new GenericRowData(2);
                compositeKey.setField(0, joinKey);
                compositeKey.setField(1, uniqueKey);
                return compositeKey;
            }
        }

        @Override
        public void addRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.put(stateKey, record);
        }

        @Override
        public void retractRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.remove(stateKey);
        }

        @Override
        public Iterable<RowData> getRecords(@Nullable RowData joinKey) throws Exception {
            if (joinKey == null || this.joinKeySerializer == null) {
                return recordState.values();
            }

            Iterator<Map.Entry<RowData, RowData>> stateIterator = recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            return new IterableIterator<>() {
                private RowData nextRecord = null;

                @Override
                public boolean hasNext() {
                    if (nextRecord != null) {
                        return true;
                    }
                    while (stateIterator.hasNext()) {
                        Map.Entry<RowData, RowData> entry = stateIterator.next();
                        RowData compositeKey = entry.getKey();
                        RowData currentJoinKey = compositeKey.getRow(0, joinKeyFieldCount);

                        if (joinKeysEqual(joinKey, currentJoinKey)) {
                            nextRecord = entry.getValue();
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public RowData next() {
                    if (hasNext()) {
                        RowData recordToReturn = nextRecord;
                        nextRecord = null; // Consume the record
                        return recordToReturn;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                @Nonnull
                public Iterator<RowData> iterator() {
                    // This iterator is stateful and intended for single use per call to getRecords.
                    return this;
                }
            };
        }
    }

    /**
     * State view for input sides that do not have a unique key (multi-set semantics).
     *
     * <p>Stores data as {@code MapState<CompositeKey<JoinKey, Record>, Count>}. The composite key
     * is a RowData with 2 fields: joinKey and record.
     *
     * <p>For Input 0, stores data as {@code MapState<Record, Count>}. The reason for that is that
     * we don't have the join key value when iterating through input 0 to use it here.
     */
    private static final class InputSideHasNoUniqueKey implements MultiJoinStateView {
        private final MapState<RowData, Integer> recordState;
        private RowDataSerializer joinKeySerializer; // Null if joinKeyType is null
        private int joinKeyFieldCount; // 0 if joinKeyType is null
        private final int recordFieldCount;
        @Nullable private final RowType joinKeyType; // Store to check for null

        private InputSideHasNoUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                @Nullable final RowType joinKeyType, // Can be null
                final RowType recordType,
                final StateTtlConfig ttlConfig) {
            this.joinKeyType = joinKeyType;
            this.recordFieldCount = recordType.getFieldCount();

            InternalTypeInfo<RowData> keyStateType;
            if (this.joinKeyType == null) {
                // State key is just the record itself
                keyStateType = InternalTypeInfo.of(recordType);
            } else {
                this.joinKeySerializer = new RowDataSerializer(this.joinKeyType);
                this.joinKeyFieldCount = this.joinKeyType.getFieldCount();
                // Composite key type: RowData with 2 fields (joinKey, record)
                final RowType keyRowType = RowType.of(this.joinKeyType, recordType);
                keyStateType = InternalTypeInfo.of(keyRowType);
            }

            MapStateDescriptor<RowData, Integer> recordStateDesc =
                    createStateDescriptor(stateName, keyStateType, Types.INT, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        private boolean joinKeysEqual(RowData joinKeyToLookup, RowData currentJoinKeyInState) {
            BinaryRowData binaryJoinKey = joinKeySerializer.toBinaryRow(joinKeyToLookup);
            BinaryRowData binaryCurrJoinKey = joinKeySerializer.toBinaryRow(currentJoinKeyInState);
            return binaryJoinKey.equals(binaryCurrJoinKey);
        }

        private RowData getStateKey(@Nullable RowData joinKey, RowData record) {
            if (joinKey == null) {
                return record; // Key is just the record
            } else {
                GenericRowData compositeKey = new GenericRowData(2);
                compositeKey.setField(0, joinKey);
                compositeKey.setField(1, record);
                return compositeKey;
            }
        }

        @Override
        public void addRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            // Normalize RowKind for consistent state representation
            RowKind originalKind = record.getRowKind();
            record.setRowKind(RowKind.INSERT); // Normalize for key creation
            RowData stateKey = getStateKey(joinKey, record);

            Integer currentCount = recordState.get(stateKey);
            if (currentCount == null) {
                currentCount = 0;
            }
            recordState.put(stateKey, currentCount + 1);
            record.setRowKind(originalKind); // Restore original RowKind
        }

        @Override
        public void retractRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            // Normalize RowKind for consistent state representation and lookup
            RowKind originalKind = record.getRowKind();
            record.setRowKind(RowKind.INSERT); // Normalize for key lookup
            RowData stateKey = getStateKey(joinKey, record);

            Integer currentCount = recordState.get(stateKey);
            if (currentCount != null) {
                if (currentCount > 1) {
                    recordState.put(stateKey, currentCount - 1);
                } else {
                    recordState.remove(stateKey);
                }
            }
            record.setRowKind(originalKind); // Restore original RowKind
        }

        @Override
        public Iterable<RowData> getRecords(@Nullable RowData joinKey) throws Exception {
            if (joinKey == null) {
                // For joinKey ull (input0), iterate all records in state. Key is Record, Value is
                // Count.
                Iterator<Map.Entry<RowData, Integer>> stateIterator = recordState.iterator();
                if (stateIterator == null) {
                    return Collections.emptyList();
                }
                return new IterableIterator<>() {
                    private RowData currentRecordFromStateKey = null;
                    private int remainingTimes = 0;

                    @Override
                    public boolean hasNext() {
                        if (remainingTimes > 0) {
                            return true;
                        }
                        if (stateIterator.hasNext()) {
                            Map.Entry<RowData, Integer> entry = stateIterator.next();
                            currentRecordFromStateKey = entry.getKey(); // Key is the record
                            remainingTimes = entry.getValue();
                            return remainingTimes > 0;
                        }
                        return false;
                    }

                    @Override
                    public RowData next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        remainingTimes--;
                        return currentRecordFromStateKey;
                    }

                    @Override
                    @Nonnull
                    public Iterator<RowData> iterator() {
                        return this;
                    }
                };
            }

            // Our stateKey is CompositeKey<JoinKey, Record> when joinKey is not null
            Iterator<Map.Entry<RowData, Integer>> stateIterator = recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            return new IterableIterator<>() {
                private RowData currentRecord = null;
                private int remainingTimes = 0;

                @Override
                public boolean hasNext() {
                    if (remainingTimes > 0) {
                        return true;
                    }
                    while (stateIterator.hasNext()) {
                        Map.Entry<RowData, Integer> currentEntry = stateIterator.next();
                        RowData compositeKey = currentEntry.getKey();
                        // For joinKeyType not null, compositeKey is <JoinKey, Record>
                        RowData currentJoinKeyInState = compositeKey.getRow(0, joinKeyFieldCount);
                        if (joinKeysEqual(joinKey, currentJoinKeyInState)) {
                            // The record is the second part of the composite key
                            currentRecord = compositeKey.getRow(1, recordFieldCount);
                            remainingTimes = currentEntry.getValue();
                            if (remainingTimes > 0) {
                                return true;
                            }
                        }
                    }
                    return false;
                }

                @Override
                public RowData next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    remainingTimes--;

                    return currentRecord;
                }

                @Override
                @Nonnull
                public Iterator<RowData> iterator() {
                    return this;
                }
            };
        }
    }
}
