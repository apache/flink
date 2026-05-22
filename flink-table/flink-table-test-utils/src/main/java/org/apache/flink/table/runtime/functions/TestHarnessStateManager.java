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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * State manager for {@link ProcessTableFunctionTestHarness}.
 *
 * <p>Handles state storage, lifecycle, and conversion between external and internal storage
 * formats.
 */
@Internal
class TestHarnessStateManager {

    private final Map<Row, Map<String, Object>> stateByKey = new HashMap<>();
    private final List<ProcessTableFunctionTestHarness.StateArgumentInfo> stateArguments;
    private final Map<String, StateConverter> stateConverters;
    private final PartitionKeyInfo partitionKeyInfo;

    TestHarnessStateManager(
            List<ProcessTableFunctionTestHarness.StateArgumentInfo> stateArguments,
            Map<String, StateConverter> stateConverters,
            PartitionKeyInfo partitionKeyInfo) {
        this.stateArguments = stateArguments;
        this.stateConverters = stateConverters;
        this.partitionKeyInfo = partitionKeyInfo;
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
     * storage to external objects (POJOs, ListView, MapView).
     */
    Map<String, Object> loadStateForKey(Row key) {
        Map<String, Object> internalState =
                stateByKey.computeIfAbsent(key, k -> createEmptyKeyState());

        Map<String, Object> externalState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            Object internalData = internalState.get(stateArg.name);
            Object external = convertToExternal(internalData, stateArg);
            externalState.put(stateArg.name, external);
        }
        return externalState;
    }

    /**
     * Update mutated state after eval() invocation. Converts external objects to internal format.
     */
    void updateStateForKey(Row key, Map<String, Object> externalState) throws Exception {
        Map<String, Object> internalState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            Object external = externalState.get(stateArg.name);
            Object internalData = convertToInternal(external, stateArg);
            internalState.put(stateArg.name, internalData);
        }
        stateByKey.put(key, internalState);
    }

    /** Clear all state for a partition key. */
    void clearStateForKey(Row key) {
        partitionKeyInfo.validate(key);
        stateByKey.remove(key);
    }

    /** Clear specific state entry for a given partition key, resetting it to its default value. */
    void clearStateEntryForKey(String stateName, Row key) {
        partitionKeyInfo.validate(key);
        Map<String, Object> internalState = stateByKey.get(key);
        if (internalState != null) {
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg =
                    findStateArgument(stateName);
            internalState.put(stateName, createNewStateInternalData(stateArg));
        }
    }

    /** Sets the state for a given partition key. */
    void setStateForKey(String stateName, Row key, Object externalState) throws Exception {
        partitionKeyInfo.validate(key);
        ProcessTableFunctionTestHarness.StateArgumentInfo stateArg = findStateArgument(stateName);
        Object internalData = convertToInternal(externalState, stateArg);

        Map<String, Object> internalState =
                stateByKey.computeIfAbsent(key, k -> createEmptyKeyState());
        internalState.put(stateName, internalData);
    }

    /** Get the state for a given partition key. */
    @SuppressWarnings("unchecked")
    <T> T getStateForKey(String stateName, Row key) {
        partitionKeyInfo.validate(key);
        Map<String, Object> internalState = stateByKey.get(key);
        if (internalState == null) {
            return null;
        }
        Object internalData = internalState.get(stateName);
        if (internalData == null) {
            return null;
        }
        return (T) convertToExternal(internalData, findStateArgument(stateName));
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
        for (Map.Entry<Row, Map<String, Object>> entry : stateByKey.entrySet()) {
            Object internalData = entry.getValue().get(stateName);
            if (internalData != null) {
                result.put(entry.getKey(), (T) convertToExternal(internalData, stateArg));
            }
        }
        return result;
    }

    private Map<String, Object> createEmptyKeyState() {
        Map<String, Object> newState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            newState.put(stateArg.name, createNewStateInternalData(stateArg));
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

    private Object convertToInternal(
            Object external, ProcessTableFunctionTestHarness.StateArgumentInfo stateArg)
            throws Exception {
        return stateConverters.get(stateArg.name).toInternal(external);
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
}
