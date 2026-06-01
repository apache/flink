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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * State manager for {@link ProcessTableFunctionTestHarness}.
 *
 * <p>Handles state storage, lifecycle, and conversion between external and internal storage
 * formats. Supports TTL-based eviction.
 */
@Internal
class TestHarnessStateManager {

    private final Map<Row, Map<String, StateEntry>> stateByKey = new HashMap<>();
    private final List<ProcessTableFunctionTestHarness.StateArgumentInfo> stateArguments;
    private final Map<String, StateConverter> stateConverters;
    private final PartitionKeyInfo partitionKeyInfo;
    private final LongSupplier systemClock;

    private static class StateEntry {
        final Object internalData;
        @Nullable final Long lastWriteTime;

        StateEntry(Object internalData, @Nullable Long lastWriteTime) {
            this.internalData = internalData;
            this.lastWriteTime = lastWriteTime;
        }
    }

    TestHarnessStateManager(
            List<ProcessTableFunctionTestHarness.StateArgumentInfo> stateArguments,
            Map<String, StateConverter> stateConverters,
            PartitionKeyInfo partitionKeyInfo,
            LongSupplier systemClock) {
        this.stateArguments = stateArguments;
        this.stateConverters = stateConverters;
        this.partitionKeyInfo = partitionKeyInfo;
        this.systemClock = systemClock;
    }

    static class PartitionKeyInfo {
        final int arity;
        @Nullable final String[] columnNames;
        @Nullable final Class<?>[] columnTypes;

        PartitionKeyInfo(
                int arity,
                @Nullable String[] columnNames,
                @Nullable LogicalType[] columnLogicalTypes) {
            this.arity = arity;
            this.columnNames = columnNames;
            this.columnTypes =
                    columnLogicalTypes != null
                            ? Arrays.stream(columnLogicalTypes)
                                    .map(LogicalType::getDefaultConversion)
                                    .toArray(Class<?>[]::new)
                            : null;
        }

        void validate(Row key) {
            if (key.getArity() != arity) {
                throw new IllegalArgumentException(
                        String.format(
                                "Partition key has arity %d, but expected arity %d.",
                                key.getArity(), arity));
            }
            if (columnTypes == null) {
                return;
            }
            for (int i = 0; i < arity; i++) {
                Object value = key.getField(i);
                if (value != null && !columnTypes[i].isInstance(value)) {
                    String columnName = columnNames != null ? columnNames[i] : "position " + i;
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partition key has type %s at position %d, "
                                            + "but partition column '%s' expects %s.",
                                    value.getClass().getSimpleName(),
                                    i,
                                    columnName,
                                    columnTypes[i].getSimpleName()));
                }
            }
        }
    }

    /**
     * Load state for a partition key. Creates new state instances if none exist. Converts internal
     * storage to external objects (value state, ListView, MapView).
     */
    Map<String, Object> loadStateForKey(Row key) {
        Map<String, StateEntry> entries =
                stateByKey.computeIfAbsent(key, k -> createEmptyKeyState());

        Map<String, Object> externalState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            StateEntry entry = entries.get(stateArg.name);
            StateConverter converter = stateConverters.get(stateArg.name);
            externalState.put(stateArg.name, converter.toExternal(entry.internalData));
        }
        return externalState;
    }

    /**
     * Update mutated state after eval() invocation. Converts external state back to internal format
     * and detects value state writes for TTL tracking.
     */
    void updateStateForKey(Row key, Map<String, Object> externalState) throws Exception {
        Map<String, StateEntry> oldEntries = stateByKey.get(key);
        Map<String, StateEntry> newEntries = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            Object external = externalState.get(stateArg.name);
            StateEntry oldEntry = oldEntries != null ? oldEntries.get(stateArg.name) : null;

            StateConverter converter = stateConverters.get(stateArg.name);
            Object internalData = converter.toInternal(external);

            Long lastWriteTime = oldEntry != null ? oldEntry.lastWriteTime : null;
            if (stateArg.hasNonZeroTtl() && stateArg.isValueState()) {
                Object oldInternal = oldEntry != null ? oldEntry.internalData : null;
                if (!Objects.equals(oldInternal, internalData)) {
                    lastWriteTime = systemClock.getAsLong();
                }
            }

            newEntries.put(stateArg.name, new StateEntry(internalData, lastWriteTime));
        }
        stateByKey.put(key, newEntries);
    }

    /** Clear all state for a partition key. */
    void clearAllStatesForKey(Row key) {
        partitionKeyInfo.validate(key);
        stateByKey.remove(key);
    }

    /** Clear specific state entry for a given partition key, resetting it to its default value. */
    void clearStateForKey(String stateName, Row key) {
        partitionKeyInfo.validate(key);
        Map<String, StateEntry> entries = stateByKey.get(key);
        if (entries != null) {
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg =
                    findStateArgument(stateName);
            entries.put(stateName, new StateEntry(createNewStateInternalData(stateArg), null));
        }
    }

    /** Sets the state for a given partition key. */
    void setStateForKey(String stateName, Row key, Object externalState) throws Exception {
        partitionKeyInfo.validate(key);
        ProcessTableFunctionTestHarness.StateArgumentInfo stateArg = findStateArgument(stateName);
        StateConverter converter = stateConverters.get(stateName);
        Object internalData = converter.toInternal(wrapIfPlainDataView(externalState));

        Map<String, StateEntry> entries =
                stateByKey.computeIfAbsent(key, k -> createEmptyKeyState());

        Long lastWriteTime = null;
        if (stateArg.hasNonZeroTtl() && stateArg.isValueState()) {
            lastWriteTime = systemClock.getAsLong();
        }

        entries.put(stateName, new StateEntry(internalData, lastWriteTime));
    }

    /** Get the state for a given partition key. */
    @SuppressWarnings("unchecked")
    <T> T getStateForKey(String stateName, Row key) {
        partitionKeyInfo.validate(key);
        Map<String, StateEntry> entries = stateByKey.get(key);
        if (entries == null) {
            return null;
        }
        StateEntry entry = entries.get(stateName);
        if (entry == null) {
            return null;
        }
        return (T) convertToExternal(entry.internalData, findStateArgument(stateName));
    }

    /** Get all partition keys that have a specific state entry. */
    Set<Row> getKeysForState(String stateName) {
        return stateByKey.entrySet().stream()
                .filter(entry -> entry.getValue().containsKey(stateName))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /** Get all state values for a state name across all partition keys. */
    @SuppressWarnings("unchecked")
    <T> Map<Row, T> getStateForAllKeys(String stateName) {
        ProcessTableFunctionTestHarness.StateArgumentInfo stateArg = findStateArgument(stateName);
        Map<Row, T> result = new HashMap<>();
        for (Map.Entry<Row, Map<String, StateEntry>> entry : stateByKey.entrySet()) {
            StateEntry stateEntry = entry.getValue().get(stateName);
            if (stateEntry != null) {
                result.put(
                        entry.getKey(), (T) convertToExternal(stateEntry.internalData, stateArg));
            }
        }
        return result;
    }

    void evictExpiredState() {
        long now = systemClock.getAsLong();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            if (!stateArg.hasNonZeroTtl()) {
                continue;
            }
            long ttlMillis = stateArg.ttl.toMillis();

            for (Row key : new ArrayList<>(stateByKey.keySet())) {
                Map<String, StateEntry> entries = stateByKey.get(key);
                if (entries == null) {
                    continue;
                }

                StateEntry entry = entries.get(stateArg.name);
                if (entry == null) {
                    continue;
                }

                if (stateArg.isValueState()) {
                    evictValueState(stateArg, entry, entries, now, ttlMillis);
                } else {
                    evictDataViewState(stateArg, entry, now, ttlMillis);
                }
            }
        }
    }

    private Map<String, StateEntry> createEmptyKeyState() {
        Map<String, StateEntry> newState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            newState.put(stateArg.name, new StateEntry(createNewStateInternalData(stateArg), null));
        }
        return newState;
    }

    private Object createNewStateInternalData(
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg) {
        return stateConverters.get(stateArg.name).createNewInternalState();
    }

    private Object convertToExternal(
            Object internalData, ProcessTableFunctionTestHarness.StateArgumentInfo stateArg) {
        return stateConverters.get(stateArg.name).toExternal(internalData);
    }

    private ProcessTableFunctionTestHarness.StateArgumentInfo findStateArgument(String stateName) {
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            if (stateArg.name.equals(stateName)) {
                return stateArg;
            }
        }
        String available =
                stateArguments.stream().map(arg -> arg.name).collect(Collectors.joining(", "));
        throw new IllegalArgumentException(
                "Unknown state: '" + stateName + "'. Available states: [" + available + "]");
    }

    @SuppressWarnings("unchecked")
    private Object wrapIfPlainDataView(Object external) {
        if (external instanceof ListView && !(external instanceof TtlAwareListView)) {
            TtlAwareListView<Object> wrapped = new TtlAwareListView<>(systemClock);
            wrapped.setList((List<Object>) ((ListView<?>) external).getList());
            return wrapped;
        }
        if (external instanceof MapView && !(external instanceof TtlAwareMapView)) {
            TtlAwareMapView<Object, Object> wrapped = new TtlAwareMapView<>(systemClock);
            wrapped.setMap((Map<Object, Object>) ((MapView<?, ?>) external).getMap());
            return wrapped;
        }
        return external;
    }

    private void evictValueState(
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            StateEntry entry,
            Map<String, StateEntry> entries,
            long now,
            long ttlMillis) {
        if (entry.lastWriteTime != null && now - entry.lastWriteTime >= ttlMillis) {
            entries.put(stateArg.name, new StateEntry(createNewStateInternalData(stateArg), null));
        }
    }

    private void evictDataViewState(
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            StateEntry entry,
            long now,
            long ttlMillis) {
        stateConverters.get(stateArg.name).evictExpired(entry.internalData, now, ttlMillis);
    }
}
