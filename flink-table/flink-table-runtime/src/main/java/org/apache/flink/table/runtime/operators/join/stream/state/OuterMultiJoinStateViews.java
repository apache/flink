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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
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

public final class OuterMultiJoinStateViews {

    /** Creates a {@link OuterMultiJoinStateView} depends on {@link JoinInputSideSpec}. */
    public static OuterMultiJoinStateView create(
            RuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            @Nullable RowType joinKeyType,
            RowType recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);

        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey() && joinKeyType != null) {
                return new OuterMultiJoinStateViews.JoinKeyContainsUniqueKey(
                        ctx, stateName, joinKeyType, recordType, ttlConfig);
            } else {
                return new OuterMultiJoinStateViews.InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        joinKeyType,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new OuterMultiJoinStateViews.InputSideHasNoUniqueKey(
                    ctx, stateName, joinKeyType, recordType, ttlConfig);
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
    // Outer Multi Join State View Implementations
    // ------------------------------------------------------------------------------------

    /**
     * State view for input sides where the unique key is fully contained within the join key.
     *
     * <p>Stores data as {@code MapState<JoinKey, Tuple2<Integer,Record>>} where Integer value is
     * the number of associations with another side of a join.
     */
    private static final class JoinKeyContainsUniqueKey implements OuterMultiJoinStateView {

        private final MapState<RowData, Tuple2<RowData, Integer>> recordState;
        private final List<RowData> reusedRecordList;
        private final List<Tuple2<RowData, Integer>> reusedTupleList;
        private final List<Tuple2<RowData, Tuple2<Integer, Integer>>> reusedDataList;

        private JoinKeyContainsUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final RowType joinKeyType,
                final RowType recordType,
                final StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(InternalTypeInfo.of(recordType), Types.INT);
            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> recordStateDesc =
                    createStateDescriptor(
                            stateName, InternalTypeInfo.of(joinKeyType), valueTypeInfo, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
            // the result records always not more than 1 per joinKey
            this.reusedRecordList = new ArrayList<>(1);
            this.reusedTupleList = new ArrayList<>(1);
            this.reusedDataList = new ArrayList<>(1);
        }

        @Override
        public void addRecord(RowData joinKey, RowData record) throws Exception {
            recordState.put(joinKey, new Tuple2<>(record, 0));
        }

        @Override
        public void retractRecord(RowData joinKey, RowData record) throws Exception {
            recordState.remove(joinKey);
        }

        @Override
        public Iterable<RowData> getRecords(RowData joinKey) throws Exception {
            reusedRecordList.clear();
            Tuple2<RowData, Integer> record = recordState.get(joinKey);
            if (record != null) {
                reusedRecordList.add(record.f0);
            }
            return reusedRecordList;
        }

        @Override
        public void addRecord(RowData joinKey, RowData record, int numOfAssociations)
                throws Exception {
            recordState.put(joinKey, new Tuple2<>(record, numOfAssociations));
        }

        @Override
        public void addRecord(RowData joinKey, RowData record, Tuple2<Integer, Integer> value)
                throws Exception {
            recordState.put(joinKey, new Tuple2<>(record, value.f1));
        }

        @Override
        public void updateNumOfAssociations(RowData joinKey, RowData record, int numOfAssociations)
                throws Exception {
            recordState.put(joinKey, new Tuple2<>(record, numOfAssociations));
        }

        @Override
        public Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations(RowData joinKey)
                throws Exception {
            if (joinKey == null) {
                return recordState.values();
            }
            reusedTupleList.clear();
            Tuple2<RowData, Integer> record = recordState.get(joinKey);
            if (record != null) {
                reusedTupleList.add(record);
            }
            return reusedTupleList;
        }

        @Override
        public Iterable<Tuple2<RowData, Tuple2<Integer, Integer>>>
                getRecordsCountAndNumOfAssociations(RowData joinKey) throws Exception {
            reusedDataList.clear();
            Tuple2<RowData, Integer> record = recordState.get(joinKey);
            if (record != null) {
                reusedDataList.add(new Tuple2<>(record.f0, new Tuple2<>(1, record.f1)));
            }
            return reusedDataList;
        }
    }

    /**
     * State view for input sides that have a unique key, but it differs from the join key.
     *
     * <p>Stores data as {@code MapState<CompositeKey<JoinKey, UK>, Tuple2<Integer, Record>>} where
     * Integer value is the number of associations with another side of a join. The composite key is
     * a RowData with 2 fields: joinKey and uniqueKey.
     */
    private static final class InputSideHasUniqueKey implements OuterMultiJoinStateView {
        private final MapState<RowData, Tuple2<RowData, Integer>> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;
        private RowDataSerializer joinKeySerializer;
        private int joinKeyFieldCount = 0;

        private InputSideHasUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                @Nullable final RowType joinKeyType,
                final RowType recordType,
                final InternalTypeInfo<RowData> uniqueKeyType,
                final KeySelector<RowData, RowData> uniqueKeySelector,
                final StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            this.uniqueKeySelector = uniqueKeySelector;

            InternalTypeInfo<RowData> keyStateType;

            this.joinKeySerializer = new RowDataSerializer(joinKeyType);
            this.joinKeyFieldCount = joinKeyType.getFieldCount();

            // Composite key type: RowData with 2 fields (joinKey, uniqueKey)
            // The composite key is a RowData with joinKey at index 0 and uniqueKey at index 1.
            final RowType keyRowType = RowType.of(joinKeyType, uniqueKeyType.toRowType());
            keyStateType = InternalTypeInfo.of(keyRowType);

            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(InternalTypeInfo.of(recordType), Types.INT);
            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> recordStateDesc =
                    createStateDescriptor(stateName, keyStateType, valueTypeInfo, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        private boolean joinKeysEqual(RowData joinKey, RowData currentJoinKeyInState) {
            BinaryRowData binaryJoinKey = joinKeySerializer.toBinaryRow(joinKey);
            BinaryRowData binaryCurrJoinKey = joinKeySerializer.toBinaryRow(currentJoinKeyInState);
            return binaryJoinKey.equals(binaryCurrJoinKey);
        }

        private RowData getStateKey(RowData joinKey, RowData uniqueKey) {
            GenericRowData compositeKey = new GenericRowData(2);
            compositeKey.setField(0, joinKey);
            compositeKey.setField(1, uniqueKey);
            return compositeKey;
        }

        @Override
        public void addRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.put(stateKey, new Tuple2<>(record, 0));
        }

        @Override
        public void retractRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.remove(stateKey);
        }

        @Override
        public Iterable<RowData> getRecords(@Nullable RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> stateIterator =
                    recordState.iterator();
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
                        Map.Entry<RowData, Tuple2<RowData, Integer>> entry = stateIterator.next();
                        RowData compositeKey = entry.getKey();
                        RowData currentJoinKey = compositeKey.getRow(0, joinKeyFieldCount);

                        if (joinKeysEqual(joinKey, currentJoinKey)) {
                            nextRecord = entry.getValue().f0;
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public RowData next() {
                    if (hasNext()) {
                        RowData recordToReturn = nextRecord;
                        nextRecord = null;
                        return recordToReturn;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                @Nonnull
                public Iterator<RowData> iterator() {
                    return this;
                }
            };
        }

        @Override
        public void addRecord(RowData joinKey, RowData record, int numOfAssociations)
                throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.put(stateKey, new Tuple2<>(record, numOfAssociations));
        }

        @Override
        public void addRecord(RowData joinKey, RowData record, Tuple2<Integer, Integer> value)
                throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.put(stateKey, new Tuple2<>(record, value.f1));
        }

        @Override
        public void updateNumOfAssociations(RowData joinKey, RowData record, int numOfAssociations)
                throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            RowData stateKey = getStateKey(joinKey, uniqueKey);
            recordState.put(stateKey, new Tuple2<>(record, numOfAssociations));
        }

        @Override
        public Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations(RowData joinKey)
                throws Exception {
            if (joinKey == null) {
                return recordState.values();
            }
            Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> stateIterator =
                    recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            return new IterableIterator<>() {
                private Tuple2<RowData, Integer> nextRecord = null;

                @Override
                public boolean hasNext() {
                    if (nextRecord != null) {
                        return true;
                    }
                    while (stateIterator.hasNext()) {
                        Map.Entry<RowData, Tuple2<RowData, Integer>> entry = stateIterator.next();
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
                public Tuple2<RowData, Integer> next() {
                    if (hasNext()) {
                        Tuple2<RowData, Integer> recordToReturn = nextRecord;
                        nextRecord = null;
                        return recordToReturn;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                @Nonnull
                public Iterator<Tuple2<RowData, Integer>> iterator() {
                    return this;
                }
            };
        }

        @Override
        public Iterable<Tuple2<RowData, Tuple2<Integer, Integer>>>
                getRecordsCountAndNumOfAssociations(RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> stateIterator =
                    recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            return new IterableIterator<>() {
                private Tuple2<RowData, Tuple2<Integer, Integer>> nextRecord = null;

                @Override
                public boolean hasNext() {
                    if (nextRecord != null) {
                        return true;
                    }
                    while (stateIterator.hasNext()) {
                        Map.Entry<RowData, Tuple2<RowData, Integer>> entry = stateIterator.next();
                        RowData compositeKey = entry.getKey();
                        RowData currentJoinKey = compositeKey.getRow(0, joinKeyFieldCount);

                        if (joinKeysEqual(joinKey, currentJoinKey)) {
                            nextRecord =
                                    new Tuple2<>(
                                            entry.getValue().f0,
                                            new Tuple2<>(1, entry.getValue().f1));
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Tuple2<RowData, Tuple2<Integer, Integer>> next() {
                    if (hasNext()) {
                        Tuple2<RowData, Tuple2<Integer, Integer>> recordToReturn = nextRecord;
                        nextRecord = null;
                        return recordToReturn;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                @Nonnull
                public Iterator<Tuple2<RowData, Tuple2<Integer, Integer>>> iterator() {
                    return this;
                }
            };
        }
    }

    /**
     * State view for input sides that do not have a unique key (multi-set semantics).
     *
     * <p>Stores data as {@code MapState<CompositeKey<JoinKey, Record>, Tuple2<Integer, Count>>}
     * where Integer value is the number of associations with another side of a join. The composite
     * key is a RowData with 2 fields: joinKey and record.
     */
    private static final class InputSideHasNoUniqueKey implements OuterMultiJoinStateView {
        private final MapState<RowData, Tuple2<Integer, Integer>> recordState;
        private RowDataSerializer joinKeySerializer;
        private int joinKeyFieldCount;
        private final int recordFieldCount;
        @Nullable private final RowType joinKeyType;

        private InputSideHasNoUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                @Nullable final RowType joinKeyType, // Can be null
                final RowType recordType,
                final StateTtlConfig ttlConfig) {
            this.joinKeyType = joinKeyType;
            this.recordFieldCount = recordType.getFieldCount();

            InternalTypeInfo<RowData> keyStateType;

            this.joinKeySerializer = new RowDataSerializer(this.joinKeyType);
            this.joinKeyFieldCount = this.joinKeyType.getFieldCount();
            // Composite key type: RowData with 2 fields (joinKey, record)
            final RowType keyRowType = RowType.of(this.joinKeyType, recordType);
            keyStateType = InternalTypeInfo.of(keyRowType);
            TupleTypeInfo<Tuple2<Integer, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(Types.INT, Types.INT);

            MapStateDescriptor<RowData, Tuple2<Integer, Integer>> recordStateDesc =
                    createStateDescriptor(stateName, keyStateType, valueTypeInfo, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        private boolean joinKeysEqual(RowData joinKeyToLookup, RowData currentJoinKeyInState) {
            BinaryRowData binaryJoinKey = joinKeySerializer.toBinaryRow(joinKeyToLookup);
            BinaryRowData binaryCurrJoinKey = joinKeySerializer.toBinaryRow(currentJoinKeyInState);
            return binaryJoinKey.equals(binaryCurrJoinKey);
        }

        private RowData getStateKey(@Nullable RowData joinKey, RowData record) {
            GenericRowData compositeKey = new GenericRowData(2);
            compositeKey.setField(0, joinKey);
            compositeKey.setField(1, record);
            return compositeKey;
        }

        @Override
        public void addRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            RowData stateKey = getStateKey(joinKey, record);

            Tuple2<Integer, Integer> value = recordState.get(stateKey);
            Integer currentCount;
            if (value == null) {
                currentCount = 0;
            } else {
                currentCount = value.f0;
            }
            recordState.put(stateKey, new Tuple2<>(currentCount + 1, 0));
        }

        @Override
        public void retractRecord(@Nullable RowData joinKey, RowData record) throws Exception {
            RowData stateKey = getStateKey(joinKey, record);

            Tuple2<Integer, Integer> value = recordState.get(stateKey);
            Integer currentCount;
            Integer currentAssocCount;

            if (value != null) {
                currentCount = value.f0;
                currentAssocCount = value.f1;
                if (currentCount > 1) {
                    recordState.put(stateKey, new Tuple2<>(currentCount - 1, currentAssocCount));
                } else {
                    recordState.remove(stateKey);
                }
            }
        }

        @Override
        public Iterable<RowData> getRecords(@Nullable RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, Tuple2<Integer, Integer>>> stateIterator =
                    recordState.iterator();
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
                        Map.Entry<RowData, Tuple2<Integer, Integer>> currentEntry =
                                stateIterator.next();
                        RowData compositeKey = currentEntry.getKey();
                        RowData currentJoinKeyInState = compositeKey.getRow(0, joinKeyFieldCount);
                        if (joinKeysEqual(joinKey, currentJoinKeyInState)) {
                            currentRecord = compositeKey.getRow(1, recordFieldCount);
                            remainingTimes = currentEntry.getValue().f0;
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

        @Override
        public void addRecord(RowData joinKey, RowData record, int numOfAssociations)
                throws Exception {
            RowData stateKey = getStateKey(joinKey, record);

            Tuple2<Integer, Integer> value = recordState.get(stateKey);
            Integer currentCount;
            if (value == null) {
                currentCount = 0;
            } else {
                currentCount = value.f0;
            }
            recordState.put(stateKey, new Tuple2<>(currentCount + 1, numOfAssociations));
        }

        @Override
        public void addRecord(RowData joinKey, RowData record, Tuple2<Integer, Integer> value)
                throws Exception {
            RowData stateKey = getStateKey(joinKey, record);

            recordState.put(stateKey, value);
        }

        @Override
        public void updateNumOfAssociations(RowData joinKey, RowData record, int numOfAssociations)
                throws Exception {
            RowData stateKey = getStateKey(joinKey, record);

            Tuple2<Integer, Integer> value = recordState.get(stateKey);
            Integer currentCount;
            if (value == null) {
                currentCount = 0;
            } else {
                currentCount = value.f0;
            }
            recordState.put(stateKey, new Tuple2<>(currentCount, numOfAssociations));
        }

        @Override
        public Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations(RowData joinKey)
                throws Exception {
            Iterator<Map.Entry<RowData, Tuple2<Integer, Integer>>> stateIterator =
                    recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            return new IterableIterator<>() {
                private Tuple2<RowData, Integer> currentRecord = null;
                private int remainingTimes = 0;

                @Override
                public boolean hasNext() {
                    if (remainingTimes > 0) {
                        return true;
                    }
                    while (stateIterator.hasNext()) {
                        Map.Entry<RowData, Tuple2<Integer, Integer>> currentEntry =
                                stateIterator.next();
                        RowData compositeKey = currentEntry.getKey();
                        // For joinKeyType not null, compositeKey is <JoinKey, Record>
                        RowData currentJoinKeyInState = compositeKey.getRow(0, joinKeyFieldCount);
                        if (joinKey == null || joinKeysEqual(joinKey, currentJoinKeyInState)) {
                            // The record is the second part of the composite key
                            remainingTimes = currentEntry.getValue().f0;
                            int assocCount = currentEntry.getValue().f1;
                            currentRecord =
                                    new Tuple2<>(
                                            compositeKey.getRow(1, recordFieldCount), assocCount);
                            if (remainingTimes > 0) {
                                return true;
                            }
                        }
                    }
                    return false;
                }

                @Override
                public Tuple2<RowData, Integer> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    remainingTimes--;

                    return currentRecord;
                }

                @Override
                @Nonnull
                public Iterator<Tuple2<RowData, Integer>> iterator() {
                    return this;
                }
            };
        }

        @Override
        public Iterable<Tuple2<RowData, Tuple2<Integer, Integer>>>
                getRecordsCountAndNumOfAssociations(RowData joinKey) throws Exception {
            Iterator<Map.Entry<RowData, Tuple2<Integer, Integer>>> stateIterator =
                    recordState.iterator();
            if (stateIterator == null) {
                return Collections.emptyList();
            }

            return new IterableIterator<>() {
                private Tuple2<RowData, Tuple2<Integer, Integer>> currentRecord = null;
                private int remainingTimes = 0;

                @Override
                public boolean hasNext() {
                    if (remainingTimes > 0) {
                        return true;
                    }
                    while (stateIterator.hasNext()) {
                        Map.Entry<RowData, Tuple2<Integer, Integer>> currentEntry =
                                stateIterator.next();
                        RowData compositeKey = currentEntry.getKey();
                        RowData currentJoinKeyInState = compositeKey.getRow(0, joinKeyFieldCount);
                        if (joinKeysEqual(joinKey, currentJoinKeyInState)) {
                            remainingTimes = currentEntry.getValue().f0;
                            currentRecord =
                                    new Tuple2<>(
                                            compositeKey.getRow(1, recordFieldCount),
                                            currentEntry.getValue());
                            if (remainingTimes > 0) {
                                return true;
                            }
                        }
                    }
                    return false;
                }

                @Override
                public Tuple2<RowData, Tuple2<Integer, Integer>> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    remainingTimes--;

                    return currentRecord;
                }

                @Override
                @Nonnull
                public Iterator<Tuple2<RowData, Tuple2<Integer, Integer>>> iterator() {
                    return this;
                }
            };
        }
    }
}
