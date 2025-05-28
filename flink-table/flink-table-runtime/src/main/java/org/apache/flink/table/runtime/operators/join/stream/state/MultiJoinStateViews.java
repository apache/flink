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
            InternalTypeInfo<RowData> joinKeyType, // Type info for the outer map key
            InternalTypeInfo<RowData> recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);

        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
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
                final InternalTypeInfo<RowData> joinKeyType,
                final InternalTypeInfo<RowData> recordType,
                final StateTtlConfig ttlConfig) {

            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    createStateDescriptor(stateName, joinKeyType, recordType, ttlConfig);

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

        @Override
        public void cleanup(RowData joinKey) throws Exception {
            recordState.remove(joinKey);
        }
    }

    /**
     * State view for input sides that have a unique key, but it differs from the join key.
     *
     * <p>Stores data as {@code MapState<CompositeKey<JoinKey, UK>, Record>}. The composite key is a
     * RowData with 2 fields: joinKey and uniqueKey.
     */
    private static final class InputSideHasUniqueKey implements MultiJoinStateView {

        // stores record in the mapping <(JoinKey, UK), Record>
        private final MapState<RowData, RowData> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;
        private final InternalTypeInfo<RowData> joinKeyType;
        private final RowDataSerializer joinKeySerializer;
        private final int joinKeyFieldCount;

        private InputSideHasUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final InternalTypeInfo<RowData> joinKeyType,
                final InternalTypeInfo<RowData> recordType,
                final InternalTypeInfo<RowData> uniqueKeyType,
                final KeySelector<RowData, RowData> uniqueKeySelector,
                final StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            this.uniqueKeySelector = uniqueKeySelector;
            this.joinKeyType = joinKeyType;
            this.joinKeySerializer = new RowDataSerializer(joinKeyType.toRowType());
            this.joinKeyFieldCount = joinKeyType.toRowType().getFieldCount();

            // Composite key type: RowData with 2 fields (joinKey, uniqueKey)
            // The composite key is a RowData with joinKey at index 0 and uniqueKey at index 1.
            // TODO Gustavo refactor so we only use row type when possible
            final RowType keyRowType =
                    RowType.of(joinKeyType.toRowType(), uniqueKeyType.toRowType());
            InternalTypeInfo<RowData> compositeKeyType = InternalTypeInfo.of(keyRowType);

            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    createStateDescriptor(stateName, compositeKeyType, recordType, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        private boolean joinKeysEqual(RowData joinKey, RowData currentJoinKey) {
            BinaryRowData binaryJoinKey = joinKeySerializer.toBinaryRow(joinKey);
            BinaryRowData binaryCurrJoinKey = joinKeySerializer.toBinaryRow(currentJoinKey);
            return binaryJoinKey.equals(binaryCurrJoinKey);
        }

        // TODO Gustavo We want to drop the default key and only store uniquekey when there is no
        // join key
        // We will use null in the code
        private RowData createCompositeKey(RowData joinKey, RowData uniqueKey) {
            GenericRowData compositeKey = new GenericRowData(2);
            compositeKey.setField(0, joinKey);
            compositeKey.setField(1, uniqueKey);
            return compositeKey;
        }

        @Override
        public void addRecord(RowData joinKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData compositeKey = createCompositeKey(joinKey, uniqueKey);
            recordState.put(compositeKey, record);
        }

        @Override
        public void retractRecord(RowData joinKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData compositeKey = createCompositeKey(joinKey, uniqueKey);
            recordState.remove(compositeKey);
        }

        @Override
        public Iterable<RowData> getRecords(RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, RowData>> stateIterator = recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            // Consume the record
            // This iterator is stateful and intended for single use per call to getRecords.

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

        @Override
        public void cleanup(RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, RowData>> iterator = recordState.iterator();
            if (iterator == null) {
                return;
            }
            List<RowData> keysToRemove = new ArrayList<>();
            while (iterator.hasNext()) {
                Map.Entry<RowData, RowData> entry = iterator.next();
                RowData compositeKey = entry.getKey();
                RowData currentJoinKey = compositeKey.getRow(0, joinKeyFieldCount);
                if (joinKeysEqual(joinKey, currentJoinKey)) {
                    keysToRemove.add(compositeKey);
                }
            }
            for (RowData keyToRemove : keysToRemove) {
                recordState.remove(keyToRemove);
            }
        }
    }

    /**
     * State view for input sides that do not have a unique key (multi-set semantics).
     *
     * <p>Stores data as {@code MapState<CompositeKey<JoinKey, Record>, Count>}. The composite key
     * is a RowData with 2 fields: joinKey and record.
     */
    private static final class InputSideHasNoUniqueKey implements MultiJoinStateView {

        // stores count in the mapping <(JoinKey, Record), Count>
        private final MapState<RowData, Integer> recordState;
        private final InternalTypeInfo<RowData> joinKeyType;
        private final InternalTypeInfo<RowData> recordType; // Needed for composite key construction
        private final RowDataSerializer joinKeySerializer;
        private final int joinKeyFieldCount;
        private final int recordFieldCount;

        private InputSideHasNoUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final InternalTypeInfo<RowData> joinKeyType,
                final InternalTypeInfo<RowData> recordType,
                final StateTtlConfig ttlConfig) {
            this.joinKeyType = joinKeyType;
            this.recordType = recordType;
            this.joinKeySerializer = new RowDataSerializer(joinKeyType.toRowType());
            this.joinKeyFieldCount = joinKeyType.toRowType().getFieldCount();
            this.recordFieldCount = recordType.toRowType().getFieldCount();

            // Composite key type: RowData with 2 fields (joinKey, record)
            // TODO Gustavo: there is probably a cleaner way of instantiating the type
            final RowType keyRowType = RowType.of(joinKeyType.toRowType(), recordType.toRowType());
            InternalTypeInfo<RowData> compositeKeyType = InternalTypeInfo.of(keyRowType);

            MapStateDescriptor<RowData, Integer> recordStateDesc =
                    createStateDescriptor(stateName, compositeKeyType, Types.INT, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        private boolean joinKeysEqual(RowData joinKey, RowData currentJoinKey) {
            BinaryRowData binaryJoinKey = joinKeySerializer.toBinaryRow(joinKey);
            BinaryRowData binaryCurrJoinKey = joinKeySerializer.toBinaryRow(currentJoinKey);
            return binaryJoinKey.equals(binaryCurrJoinKey);
        }

        private RowData createCompositeKey(RowData joinKey, RowData record) {
            GenericRowData compositeKey = new GenericRowData(2);
            compositeKey.setField(0, joinKey);
            compositeKey.setField(1, record);
            return compositeKey;
        }

        @Override
        public void addRecord(RowData joinKey, RowData record) throws Exception {
            // Normalize RowKind for consistent state representation
            RowKind originalKind = record.getRowKind();
            record.setRowKind(RowKind.INSERT);
            RowData compositeKey = createCompositeKey(joinKey, record);

            Integer currentCount = recordState.get(compositeKey);
            if (currentCount == null) {
                currentCount = 0;
            }
            recordState.put(compositeKey, currentCount + 1);
            record.setRowKind(originalKind); // Restore original RowKind after key creation
        }

        @Override
        public void retractRecord(RowData joinKey, RowData record) throws Exception {
            // Normalize RowKind for consistent state representation and lookup
            RowKind originalKind = record.getRowKind();
            record.setRowKind(RowKind.INSERT);
            RowData compositeKey = createCompositeKey(joinKey, record);

            Integer currentCount = recordState.get(compositeKey);
            if (currentCount != null) {
                if (currentCount > 1) {
                    recordState.put(compositeKey, currentCount - 1);
                } else {
                    recordState.remove(compositeKey);
                }
            }
            record.setRowKind(originalKind); // Restore original RowKind
        }

        @Override
        public Iterable<RowData> getRecords(RowData joinKey) throws Exception {
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
                        RowData currentJoinKey = compositeKey.getRow(0, joinKeyFieldCount);
                        if (joinKeysEqual(joinKey, currentJoinKey)) {
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
                    checkNotNull(currentRecord);
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

        @Override
        public void cleanup(RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, Integer>> iterator = recordState.iterator();
            if (iterator == null) {
                return;
            }
            List<RowData> keysToRemove = new ArrayList<>();
            while (iterator.hasNext()) {
                Map.Entry<RowData, Integer> entry = iterator.next();
                RowData compositeKey = entry.getKey();
                RowData currentJoinKey = compositeKey.getRow(0, joinKeyFieldCount);
                if (joinKeysEqual(joinKey, currentJoinKey)) {
                    keysToRemove.add(compositeKey);
                }
            }
            for (RowData keyToRemove : keysToRemove) {
                recordState.remove(keyToRemove);
            }
        }
    }
}
