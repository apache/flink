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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
 * <p>Each state view uses a {@link MapState} where the primary key is the `mapKey` derived from the
 * join conditions (via {@link
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
            InternalTypeInfo<RowData> mapKeyType, // Type info for the outer map key
            InternalTypeInfo<RowData> recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);

        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new JoinKeyContainsUniqueKey(
                        ctx, stateName, mapKeyType, recordType, ttlConfig);
            } else {
                return new InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        mapKeyType,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new InputSideHasNoUniqueKey(ctx, stateName, mapKeyType, recordType, ttlConfig);
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
     * <p>Stores data as {@code MapState<MapKey, Record>}.
     */
    private static final class JoinKeyContainsUniqueKey implements MultiJoinStateView {

        // stores record in the mapping <MapKey, Record>
        private final MapState<RowData, RowData> recordState;
        private final List<RowData> reusedList;

        private JoinKeyContainsUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final InternalTypeInfo<RowData> mapKeyType,
                final InternalTypeInfo<RowData> recordType,
                final StateTtlConfig ttlConfig) {

            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    createStateDescriptor(stateName, mapKeyType, recordType, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
            // the result records always not more than 1 per mapKey
            this.reusedList = new ArrayList<>(1);
        }

        @Override
        public void addRecord(RowData mapKey, RowData record) throws Exception {
            recordState.put(mapKey, record);
        }

        @Override
        public void retractRecord(RowData mapKey, RowData record) throws Exception {
            recordState.remove(mapKey);
        }

        @Override
        public Iterable<RowData> getRecords(RowData mapKey) throws Exception {
            reusedList.clear();
            RowData record = recordState.get(mapKey);
            if (record != null) {
                reusedList.add(record);
            }
            return reusedList;
        }

        @Override
        public void cleanup(RowData mapKey) throws Exception {
            recordState.remove(mapKey);
        }
    }

    /**
     * State view for input sides that have a unique key, but it differs from the join key.
     *
     * <p>Stores data as {@code MapState<MapKey, Map<UK, Record>>}.
     */
    private static final class InputSideHasUniqueKey implements MultiJoinStateView {

        // stores map in the mapping <MapKey, Map<UK, Record>>
        private final MapState<RowData, Map<RowData, RowData>> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;

        private InputSideHasUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final InternalTypeInfo<RowData> mapKeyType,
                final InternalTypeInfo<RowData> recordType,
                final InternalTypeInfo<RowData> uniqueKeyType,
                final KeySelector<RowData, RowData> uniqueKeySelector,
                final StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            this.uniqueKeySelector = uniqueKeySelector;

            TypeInformation<Map<RowData, RowData>> mapValueTypeInfo =
                    Types.MAP(uniqueKeyType, recordType); // UK is the key in the inner map

            MapStateDescriptor<RowData, Map<RowData, RowData>> recordStateDesc =
                    createStateDescriptor(stateName, mapKeyType, mapValueTypeInfo, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        @Override
        public void addRecord(RowData mapKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            Map<RowData, RowData> uniqueKeyToRecordMap = recordState.get(mapKey);
            if (uniqueKeyToRecordMap == null) {
                uniqueKeyToRecordMap = new HashMap<>();
            }
            uniqueKeyToRecordMap.put(uniqueKey, record);
            recordState.put(mapKey, uniqueKeyToRecordMap);
        }

        @Override
        public void retractRecord(RowData mapKey, RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            Map<RowData, RowData> uniqueKeyToRecordMap = recordState.get(mapKey);
            if (uniqueKeyToRecordMap != null) {
                uniqueKeyToRecordMap.remove(uniqueKey);
                if (uniqueKeyToRecordMap.isEmpty()) {
                    // Clean up the entry for mapKey if the inner map becomes empty
                    recordState.remove(mapKey);
                } else {
                    recordState.put(mapKey, uniqueKeyToRecordMap);
                }
            }
        }

        @Override
        public Iterable<RowData> getRecords(RowData mapKey) throws Exception {
            Map<RowData, RowData> uniqueKeyToRecordMap = recordState.get(mapKey);
            if (uniqueKeyToRecordMap == null) {
                return Collections.emptyList();
            } else {
                // Return the values (records) from the inner map
                return uniqueKeyToRecordMap.values();
            }
        }

        @Override
        public void cleanup(RowData mapKey) throws Exception {
            recordState.remove(mapKey);
        }
    }

    /**
     * State view for input sides that do not have a unique key (multi-set semantics).
     *
     * <p>Stores data as {@code MapState<MapKey, Map<Record, Count>>}.
     */
    private static final class InputSideHasNoUniqueKey implements MultiJoinStateView {

        // stores map in the mapping <MapKey, Map<Record, Count>>
        private final MapState<RowData, Map<RowData, Integer>> recordState;

        private InputSideHasNoUniqueKey(
                RuntimeContext ctx,
                final String stateName,
                final InternalTypeInfo<RowData> mapKeyType,
                final InternalTypeInfo<RowData> recordType,
                final StateTtlConfig ttlConfig) {

            TypeInformation<Map<RowData, Integer>> mapValueTypeInfo =
                    Types.MAP(recordType, Types.INT);

            MapStateDescriptor<RowData, Map<RowData, Integer>> recordStateDesc =
                    createStateDescriptor(stateName, mapKeyType, mapValueTypeInfo, ttlConfig);

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        @Override
        public void addRecord(RowData mapKey, RowData record) throws Exception {
            Map<RowData, Integer> recordToCountMap = recordState.get(mapKey);
            if (recordToCountMap == null) {
                recordToCountMap = new HashMap<>();
            }
            recordToCountMap.merge(record, 1, Integer::sum); // Increment count or set to 1
            recordState.put(mapKey, recordToCountMap);
        }

        @Override
        public void retractRecord(RowData mapKey, RowData record) throws Exception {
            Map<RowData, Integer> recordToCountMap = recordState.get(mapKey);

            // When storing and retrieving records from state, we need to ensure consistent RowKind
            // handling because records are stored in state with RowKind.INSERT or else we'll not
            // find them.
            var origKind = record.getRowKind();
            record.setRowKind(RowKind.INSERT);
            if (recordToCountMap != null) {
                Integer cnt = recordToCountMap.get(record);
                if (cnt != null) {
                    if (cnt > 1) {
                        recordToCountMap.put(record, cnt - 1);
                    } else {
                        // remove the record entry if count reaches 0
                        recordToCountMap.remove(record);
                    }

                    if (recordToCountMap.isEmpty()) {
                        // Clean up the entry for mapKey if the inner map becomes empty
                        recordState.remove(mapKey);
                    } else {
                        recordState.put(mapKey, recordToCountMap);
                    }
                }
            }
            record.setRowKind(origKind);
        }

        @Override
        public Iterable<RowData> getRecords(RowData mapKey) throws Exception {
            Map<RowData, Integer> recordToCountMap = recordState.get(mapKey);
            if (recordToCountMap == null || recordToCountMap.isEmpty()) {
                return Collections.emptyList();
            }

            // Return an Iterable that respects the counts
            return new IterableIterator<RowData>() {
                private final Iterator<Map.Entry<RowData, Integer>> backingIterator =
                        recordToCountMap.entrySet().iterator();
                private RowData currentRecord;
                private int remainingTimes = 0;

                @Override
                public boolean hasNext() {
                    return remainingTimes > 0 || backingIterator.hasNext();
                }

                @Override
                public RowData next() {
                    if (remainingTimes > 0) {
                        checkNotNull(currentRecord);
                        remainingTimes--;
                        return currentRecord;
                    } else {
                        if (!backingIterator.hasNext()) {
                            throw new NoSuchElementException("No more elements");
                        }
                        Map.Entry<RowData, Integer> entry = backingIterator.next();
                        currentRecord = entry.getKey();
                        remainingTimes = entry.getValue() - 1; // We return one now
                        if (remainingTimes < 0) {
                            throw new IllegalStateException(
                                    "Invalid state: count <= 0 for record: " + currentRecord);
                        }
                        return currentRecord;
                    }
                }

                @Override
                public Iterator<RowData> iterator() {
                    // This iterator is stateful and intended for single use per call to getRecords.
                    return this;
                }
            };
        }

        @Override
        public void cleanup(RowData mapKey) throws Exception {
            recordState.remove(mapKey);
        }
    }
}
