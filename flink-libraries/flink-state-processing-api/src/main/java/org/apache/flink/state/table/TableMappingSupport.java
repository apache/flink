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

package org.apache.flink.state.table;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.runtime.SavepointLoader.OperatorStateMetadata;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.state.table.SavepointConnectorOptions.FIELDS;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME;

/**
 * Shared static helpers for the table mapping classes. These helpers are pure schema/metadata
 * plumbing with no dependency on any single mapping class's field layout, so they are factored out
 * here rather than duplicated (or hung off one mapping class for the others to reach into).
 */
final class TableMappingSupport {

    private TableMappingSupport() {}

    /** Returns a {@link ConfigOptions.OptionBuilder} for a per-field connector option. */
    static ConfigOptions.OptionBuilder fieldOption(String fieldName, String suffix) {
        return ConfigOptions.key(String.format("%s.%s.%s", FIELDS, fieldName, suffix));
    }

    /** Returns the index of {@code fieldName} in the physical row data type, or {@code -1}. */
    static int columnIndex(DataType physicalDataType, String fieldName) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        return LogicalTypeChecks.getFieldNames(physicalType).indexOf(fieldName);
    }

    /** Returns the indices of all columns except {@code excludedIndices}. */
    static int[] valueColumnIndices(DataType physicalDataType, int... excludedIndices) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int fieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        return IntStream.range(0, fieldCount)
                .filter(pos -> IntStream.of(excludedIndices).noneMatch(excluded -> excluded == pos))
                .toArray();
    }

    /**
     * Remaps a list of value columns' indices under {@code projectedFields}, dropping any column
     * that was projected away.
     */
    static List<StateValueColumnConfiguration> remapValueColumns(
            int[][] projectedFields, List<StateValueColumnConfiguration> valueColumns) {
        List<StateValueColumnConfiguration> newValueColumns = new ArrayList<>();
        for (StateValueColumnConfiguration col : valueColumns) {
            int newColumnIndex = remapColumnIndex(projectedFields, col.getColumnIndex());
            if (newColumnIndex >= 0) {
                newValueColumns.add(col.withColumnIndex(newColumnIndex));
            }
        }
        return newValueColumns;
    }

    /**
     * Returns the output-row index that {@code sourceIndex} is remapped to under {@code
     * projectedFields} (see {@link
     * org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown}), or {@code -1}
     * if {@code sourceIndex} was projected away.
     *
     * <p>Shared by the mappings' {@code project(...)} and the table sources' {@code
     * applyProjection(...)}, so both sides compute "where does this column end up after projection"
     * the same way.
     */
    static int remapColumnIndex(int[][] projectedFields, int sourceIndex) {
        for (int outputIdx = 0; outputIdx < projectedFields.length; outputIdx++) {
            Preconditions.checkArgument(
                    projectedFields[outputIdx].length == 1,
                    "Only flat (non-nested) projections are supported.");
            if (projectedFields[outputIdx][0] == sourceIndex) {
                return outputIdx;
            }
        }
        return -1;
    }

    /** Infers the state type from its SQL logical type. */
    static SavepointConnectorOptions.StateType inferStateType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                return SavepointConnectorOptions.StateType.LIST;
            case MAP:
                return SavepointConnectorOptions.StateType.MAP;
            default:
                return SavepointConnectorOptions.StateType.VALUE;
        }
    }

    /**
     * Preloads keyed operator metadata (state serializer snapshots + backend key serializer
     * snapshot) and builds the {@link SavepointTypeInfoResolver} derived from it, in a single I/O
     * operation. Shared by all keyed mapping classes' {@code from(...)} factories.
     */
    static SavepointTypeInfoResolver createTypeResolver(
            String statePath,
            OperatorIdentifier operatorIdentifier,
            SerializerConfig serializerConfig) {
        OperatorStateMetadata operatorMetadata;
        try {
            operatorMetadata = SavepointLoader.loadOperatorMetadata(statePath, operatorIdentifier);
        } catch (Exception e) {
            throw metadataLoadFailure(statePath, operatorIdentifier, e);
        }
        return new SavepointTypeInfoResolver(
                operatorMetadata.stateSnapshots,
                serializerConfig,
                operatorMetadata.keySerializerSnapshot);
    }

    private static RuntimeException metadataLoadFailure(
            String statePath, OperatorIdentifier operatorIdentifier, Exception cause) {
        return new RuntimeException(
                String.format(
                        "Failed to load state metadata from savepoint '%s' for operator '%s'. "
                                + "Ensure the savepoint path is valid and the operator exists in the savepoint. ",
                        statePath, operatorIdentifier),
                cause);
    }

    @SuppressWarnings("rawtypes")
    static StateValueColumnConfiguration createValueColumnConfig(
            int columnIndex,
            RowType rowType,
            Configuration options,
            SavepointTypeInfoResolver typeResolver) {

        RowType.RowField valueRowField = rowType.getFields().get(columnIndex);

        ConfigOption<String> stateNameOption =
                fieldOption(valueRowField.getName(), STATE_NAME).stringType().noDefaultValue();
        String stateName = options.getOptional(stateNameOption).orElse(valueRowField.getName());

        SavepointConnectorOptions.StateType stateType = inferStateType(valueRowField.getType());

        TypeSerializer mapKeyTypeSerializer =
                typeResolver.resolveMapKeySerializer(
                        valueRowField, stateType == SavepointConnectorOptions.StateType.MAP);
        TypeSerializer valueTypeSerializer = typeResolver.resolveValueSerializer(valueRowField);

        return new StateValueColumnConfiguration(
                columnIndex,
                stateName,
                stateType,
                typeResolver.resolveStateKind(stateName),
                mapKeyTypeSerializer,
                valueTypeSerializer);
    }
}
