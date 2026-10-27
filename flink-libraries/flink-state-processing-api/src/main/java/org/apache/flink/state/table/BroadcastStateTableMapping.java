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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.SavepointLoader.NonKeyedOperatorStateMetadata;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.List;

/**
 * Maps the fixed 2-column schema of a {@code BroadcastState} table: {@code (map_key <key-type> NOT
 * NULL, map_value)}, with a primary key on {@code map_key}.
 *
 * <p>The second column has a fixed name ({@code map_value}) rather than being named after the state
 * itself, to avoid collisions with other (reserved) column names; the true state name is instead
 * resolved from {@link SavepointConnectorOptions#FLATTENED_STATE_NAME}.
 *
 * <p>Both the map key and value serializers are stored flat/unwrapped in the savepoint metadata
 * (under {@code KEY_SERIALIZER}/{@code VALUE_SERIALIZER}) — unlike a keyed {@code MapState}, there
 * is no wrapping {@code MapSerializer} to unwrap.
 */
@Internal
public class BroadcastStateTableMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int MAP_KEY_COLUMN_INDEX = 0;
    public static final int VALUE_COLUMN_INDEX = 1;

    private final String stateName;
    private final TypeSerializer<?> mapKeyTypeSerializer;
    private final TypeSerializer<?> valueTypeSerializer;

    public BroadcastStateTableMapping(
            String stateName,
            TypeSerializer<?> mapKeyTypeSerializer,
            TypeSerializer<?> valueTypeSerializer) {
        this.stateName = stateName;
        this.mapKeyTypeSerializer = mapKeyTypeSerializer;
        this.valueTypeSerializer = valueTypeSerializer;
    }

    public String getStateName() {
        return stateName;
    }

    public TypeSerializer<?> getMapKeyTypeSerializer() {
        return mapKeyTypeSerializer;
    }

    public TypeSerializer<?> getValueTypeSerializer() {
        return valueTypeSerializer;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Validates that the table schema matches the fixed 2-column {@code (map_key, map_value)}
     * layout with a primary key on {@code map_key}. This is a purely structural check; it performs
     * no I/O or class loading.
     */
    public static void validateSchema(ResolvedCatalogTable catalogTable) {
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        List<Column> columns = schema.getColumns();
        if (columns.size() != 2) {
            throw new ValidationException(
                    "BROADCAST state tables must have exactly 2 columns "
                            + "(map_key, map_value), but found "
                            + columns.size()
                            + ".");
        }
        String mapKeyColumnName = columns.get(MAP_KEY_COLUMN_INDEX).getName();
        if (!"map_key".equals(mapKeyColumnName)) {
            throw new ValidationException(
                    "BROADCAST state tables must name their first column 'map_key', "
                            + "but found '"
                            + mapKeyColumnName
                            + "'.");
        }

        String valueColumnName = columns.get(VALUE_COLUMN_INDEX).getName();
        if (!"map_value".equals(valueColumnName)) {
            throw new ValidationException(
                    "BROADCAST state tables must name their second column 'map_value', "
                            + "but found '"
                            + valueColumnName
                            + "'.");
        }

        List<String> primaryKeyColumns =
                schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(List.of());
        if (!primaryKeyColumns.equals(List.of(mapKeyColumnName))) {
            throw new ValidationException(
                    "BROADCAST state tables must declare a primary key on '"
                            + mapKeyColumnName
                            + "', but found: "
                            + (primaryKeyColumns.isEmpty() ? "none" : primaryKeyColumns)
                            + ".");
        }
    }

    /**
     * Builds a complete {@link BroadcastStateTableMapping}, loading non-keyed operator state
     * metadata from the savepoint and resolving the key and value serializers from it.
     *
     * <p>Assumes {@link #validateSchema} has already been called. This performs I/O; callers should
     * invoke it lazily, deferred to scan time, to keep planning free of savepoint access.
     */
    public static BroadcastStateTableMapping from(
            ResolvedCatalogTable catalogTable,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier,
            SerializerConfig serializerConfig) {

        NonKeyedOperatorStateMetadata operatorMetadata =
                TableMappingSupport.loadNonKeyedOperatorMetadata(statePath, operatorIdentifier);

        SavepointTypeInfoResolver typeResolver =
                new SavepointTypeInfoResolver(
                        operatorMetadata.broadcastStateSnapshots, serializerConfig, null);

        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        LogicalType valueLogicalType = rowType.getFields().get(VALUE_COLUMN_INDEX).getType();
        // Synthetic RowField: name == actual state name (so metadata lookup succeeds), since the
        // value column itself is named after a fixed literal (map_value), not the state.
        RowType.RowField valueRowField = new RowType.RowField(stateName, valueLogicalType);

        TypeSerializer<?> mapKeyTypeSerializer = typeResolver.resolveKeySerializer(valueRowField);
        TypeSerializer<?> valueTypeSerializer =
                typeResolver.resolveFlatValueSerializer(valueRowField);

        return new BroadcastStateTableMapping(stateName, mapKeyTypeSerializer, valueTypeSerializer);
    }
}
