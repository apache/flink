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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Maps the fixed 4-column schema of a flattened namespaced (e.g. window-scoped) keyed list/map
 * state table:
 *
 * <ul>
 *   <li>LIST: {@code (state_key, state_window, list_index, list_value)}
 *   <li>MAP: {@code (state_key, state_window, map_key, map_value)}
 * </ul>
 *
 * <p>The fourth column has a fixed name ({@code list_value}/{@code map_value}) rather than being
 * named after the flattened state itself, to avoid collisions with other (reserved) column names;
 * the true state name is instead resolved from {@link
 * SavepointConnectorOptions#FLATTENED_STATE_NAME}.
 *
 * <p>Mirrors {@link FlattenedStateTableMapping}, shifted by one column to make room for {@code
 * state_window}. As with {@link WindowStateTableMapping}, no primary key is declared on the table
 * (Flink's primary-key validation for arbitrary {@code ROW}-typed columns such as {@code
 * state_window} is not reliable), so columns are identified by fixed position, exactly as in {@link
 * FlattenedStateTableMapping}.
 */
@Internal
public class WindowFlattenedStateTableMapping implements Serializable, SingleColumnStateMapping {

    private static final long serialVersionUID = 1L;

    public static final int STATE_KEY_COLUMN_INDEX = 0;
    public static final int WINDOW_COLUMN_INDEX = 1;
    public static final int SUB_KEY_COLUMN_INDEX = 2;
    public static final int VALUE_COLUMN_INDEX = 3;

    private final String stateName;
    private final SavepointConnectorOptions.StateType stateType;
    private final TypeInformation<?> keyTypeInfo;
    @Nullable private final TypeSerializer<?> mapKeyTypeSerializer;
    private final TypeSerializer<?> valueTypeSerializer;
    @Nullable private final TypeSerializer<Object> namespaceSerializer;
    @Nullable private StateDescriptor stateDescriptor;

    public WindowFlattenedStateTableMapping(
            String stateName,
            SavepointConnectorOptions.StateType stateType,
            TypeInformation<?> keyTypeInfo,
            @Nullable TypeSerializer<?> mapKeyTypeSerializer,
            TypeSerializer<?> valueTypeSerializer,
            @Nullable TypeSerializer<Object> namespaceSerializer) {
        Preconditions.checkArgument(
                stateType == SavepointConnectorOptions.StateType.LIST
                        || stateType == SavepointConnectorOptions.StateType.MAP,
                "Flattened state tables only support LIST and MAP states, got: " + stateType);
        this.stateName = stateName;
        this.stateType = stateType;
        this.keyTypeInfo = keyTypeInfo;
        this.mapKeyTypeSerializer = mapKeyTypeSerializer;
        this.valueTypeSerializer = valueTypeSerializer;
        this.namespaceSerializer = namespaceSerializer;
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

    @Nullable
    public TypeSerializer<Object> getNamespaceSerializer() {
        return namespaceSerializer;
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
     * Validates that the table schema matches the fixed 4-column flattened window layout, and
     * returns the state type (LIST or MAP), inferred from whether the third column is named {@code
     * list_index} or {@code map_key}. This is a purely structural check; it performs no I/O or
     * class loading.
     */
    public static SavepointConnectorOptions.StateType validateFlattenedSchema(
            ResolvedCatalogTable catalogTable) {
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        List<Column> columns = schema.getColumns();
        if (columns.size() != 4) {
            throw new ValidationException(
                    "Flattened namespaced (window) keyed state tables must have exactly 4 columns "
                            + "(state_key, state_window, list_index/map_key, list_value/map_value), "
                            + "but found "
                            + columns.size()
                            + ".");
        }
        DataType physicalDataType = schema.toPhysicalRowDataType();
        Preconditions.checkArgument(
                physicalDataType.getLogicalType().is(LogicalTypeRoot.ROW),
                "Row data type expected.");

        String windowColumnName = columns.get(WINDOW_COLUMN_INDEX).getName();
        if (!windowColumnName.equals(WindowStateTableMapping.WINDOW_COLUMN_NAME)) {
            throw new ValidationException(
                    "Flattened namespaced (window) keyed state tables must name their second "
                            + "column '"
                            + WindowStateTableMapping.WINDOW_COLUMN_NAME
                            + "', but found '"
                            + windowColumnName
                            + "'.");
        }

        String subKeyColumnName = columns.get(SUB_KEY_COLUMN_INDEX).getName();
        String valueColumnName = columns.get(VALUE_COLUMN_INDEX).getName();
        return TableMappingSupport.inferFlattenedStateTypeAndValidateValueColumn(
                "Flattened namespaced (window) keyed state tables",
                "third",
                "fourth",
                subKeyColumnName,
                valueColumnName);
    }

    /**
     * Builds a complete {@link WindowFlattenedStateTableMapping}, loading operator state metadata
     * from the savepoint and resolving serializers, key type and namespace serializer from it.
     *
     * <p>Assumes {@link #validateFlattenedSchema} has already been called for this table.
     *
     * <p>This performs I/O (savepoint metadata loading); callers should invoke it lazily, deferred
     * to scan time, to keep planning free of savepoint access.
     *
     * @param stateName the name of the flattened LIST/MAP state, resolved from {@link
     *     SavepointConnectorOptions#FLATTENED_STATE_NAME}
     */
    public static WindowFlattenedStateTableMapping from(
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

        @SuppressWarnings("unchecked")
        TypeSerializer<Object> namespaceSerializer =
                (TypeSerializer<Object>) typeResolver.resolveNamespaceSerializer(stateName);

        return new WindowFlattenedStateTableMapping(
                stateName,
                stateType,
                serializers.keyTypeInfo,
                serializers.mapKeyTypeSerializer,
                serializers.valueTypeSerializer,
                namespaceSerializer);
    }
}
