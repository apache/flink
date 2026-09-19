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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Maps the fixed 3-column schema of a flattened keyed list/map state table:
 *
 * <ul>
 *   <li>LIST: {@code (state_key, list_index, list_value)}
 *   <li>MAP: {@code (state_key, map_key, map_value)}
 * </ul>
 *
 * <p>The third column has a fixed name ({@code list_value}/{@code map_value}) rather than being
 * named after the flattened state itself, to avoid collisions with other (reserved) column names;
 * the true state name is instead resolved from {@link
 * SavepointConnectorOptions#FLATTENED_STATE_NAME}.
 *
 * <p>A flattened table always exposes exactly one keyed state and emits one row per list element /
 * map entry (as opposed to one row per key), so unlike {@link StateTableMapping} there is no
 * per-column projection bookkeeping: column indices in the (fixed) output row are always {@code
 * 0=state_key}, {@code 1=list_index/map_key}, {@code 2=list_value/map_value}.
 */
@Internal
public class FlattenedStateTableMapping implements Serializable, SingleColumnStateMapping {

    private static final long serialVersionUID = 1L;

    public static final int STATE_KEY_COLUMN_INDEX = 0;
    public static final int SUB_KEY_COLUMN_INDEX = 1;
    public static final int VALUE_COLUMN_INDEX = 2;

    private final String stateName;
    private final SavepointConnectorOptions.StateType stateType;
    private final TypeInformation<?> keyTypeInfo;
    @Nullable private final TypeSerializer<?> mapKeyTypeSerializer;
    private final TypeSerializer<?> valueTypeSerializer;
    @Nullable private StateDescriptor stateDescriptor;

    public FlattenedStateTableMapping(
            String stateName,
            SavepointConnectorOptions.StateType stateType,
            TypeInformation<?> keyTypeInfo,
            @Nullable TypeSerializer<?> mapKeyTypeSerializer,
            TypeSerializer<?> valueTypeSerializer) {
        Preconditions.checkArgument(
                stateType == SavepointConnectorOptions.StateType.LIST
                        || stateType == SavepointConnectorOptions.StateType.MAP,
                "Flattened state tables only support LIST and MAP states, got: " + stateType);
        this.stateName = stateName;
        this.stateType = stateType;
        this.keyTypeInfo = keyTypeInfo;
        this.mapKeyTypeSerializer = mapKeyTypeSerializer;
        this.valueTypeSerializer = valueTypeSerializer;
    }

    @Override
    public String getStateName() {
        return stateName;
    }

    @Override
    public SavepointConnectorOptions.StateType getStateType() {
        return stateType;
    }

    @Override
    public TypeInformation<?> getKeyTypeInfo() {
        return keyTypeInfo;
    }

    @Override
    @Nullable
    public TypeSerializer<?> getMapKeyTypeSerializer() {
        return mapKeyTypeSerializer;
    }

    @Override
    public TypeSerializer<?> getValueTypeSerializer() {
        return valueTypeSerializer;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void setStateDescriptor(StateDescriptor stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    public StateDescriptor getStateDescriptor() {
        return stateDescriptor;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Validates that the table schema matches the fixed 3-column flattened layout with a composite
     * primary key on {@code (state_key, list_index/map_key)}, and returns the state type (LIST or
     * MAP), inferred from whether the second column is named {@code list_index} or {@code map_key}.
     * This is a purely structural check; it performs no I/O or class loading.
     */
    public static SavepointConnectorOptions.StateType validateFlattenedSchema(
            ResolvedCatalogTable catalogTable) {
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        List<Column> columns = schema.getColumns();
        if (columns.size() != 3) {
            throw new ValidationException(
                    "Flattened keyed state tables must have exactly 3 columns "
                            + "(state_key, list_index/map_key, list_value/map_value), but found "
                            + columns.size()
                            + ".");
        }
        DataType physicalDataType = schema.toPhysicalRowDataType();
        Preconditions.checkArgument(
                physicalDataType.getLogicalType().is(LogicalTypeRoot.ROW),
                "Row data type expected.");

        String stateKeyColumnName = columns.get(STATE_KEY_COLUMN_INDEX).getName();
        String subKeyColumnName = columns.get(SUB_KEY_COLUMN_INDEX).getName();
        String valueColumnName = columns.get(VALUE_COLUMN_INDEX).getName();
        SavepointConnectorOptions.StateType stateType =
                TableMappingSupport.inferFlattenedStateTypeAndValidateValueColumn(
                        "Flattened keyed state tables",
                        "second",
                        "third",
                        subKeyColumnName,
                        valueColumnName);

        List<String> expectedKeyColumns = List.of(stateKeyColumnName, subKeyColumnName);
        List<String> primaryKeyColumns =
                schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(List.of());
        if (!primaryKeyColumns.equals(expectedKeyColumns)) {
            throw new ValidationException(
                    "Flattened keyed state tables must declare a composite primary key on ("
                            + stateKeyColumnName
                            + ", "
                            + subKeyColumnName
                            + "), but found: "
                            + (primaryKeyColumns.isEmpty() ? "none" : primaryKeyColumns)
                            + ".");
        }

        return stateType;
    }

    /**
     * Builds a complete {@link FlattenedStateTableMapping}, loading operator state metadata from
     * the savepoint and resolving serializers and key type from it.
     *
     * <p>Assumes {@link #validateFlattenedSchema} has already been called for this table.
     *
     * <p>This performs I/O (savepoint metadata loading); callers should invoke it lazily, deferred
     * to scan time, to keep planning free of savepoint access.
     *
     * @param catalogTable the resolved table whose schema drives the mapping
     * @param stateName the name of the flattened LIST/MAP state, resolved from {@link
     *     SavepointConnectorOptions#FLATTENED_STATE_NAME}
     * @param statePath path to the savepoint containing the operator state metadata
     * @param operatorIdentifier identifies the operator whose state metadata is loaded
     * @param serializerConfig serializer config used when creating serializers from resolved types
     * @param stateType {@code LIST} or {@code MAP}
     */
    public static FlattenedStateTableMapping from(
            ResolvedCatalogTable catalogTable,
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier,
            SerializerConfig serializerConfig,
            SavepointConnectorOptions.StateType stateType) {

        SavepointTypeInfoResolver typeResolver =
                TableMappingSupport.createTypeResolver(
                        statePath, operatorIdentifier, serializerConfig);

        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        RowType rowType = (RowType) physicalDataType.getLogicalType();

        TableMappingSupport.FlattenedSerializers serializers =
                TableMappingSupport.resolveFlattenedSerializers(
                        rowType,
                        typeResolver,
                        stateName,
                        stateType,
                        STATE_KEY_COLUMN_INDEX,
                        SUB_KEY_COLUMN_INDEX,
                        VALUE_COLUMN_INDEX);

        return new FlattenedStateTableMapping(
                stateName,
                stateType,
                serializers.keyTypeInfo,
                serializers.mapKeyTypeSerializer,
                serializers.valueTypeSerializer);
    }
}
