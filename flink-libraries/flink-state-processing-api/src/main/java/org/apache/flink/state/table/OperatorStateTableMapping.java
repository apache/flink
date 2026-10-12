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
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.SavepointLoader.NonKeyedOperatorStateMetadata;
import org.apache.flink.state.api.schema.SerializerSnapshotToLogicalTypeConverter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Maps a {@code ListState}/{@code UnionState} table's schema, in which the state's value is
 * flattened directly into the table's columns: one column per field for a structured (ROW-typed)
 * value, or a single value column for a scalar one. There is no synthetic ordering column.
 *
 * <p>Unlike keyed LIST/MAP state (see {@link FlattenedStateTableMapping}), the value serializer is
 * stored flat/unwrapped in the savepoint metadata — there is no wrapping {@code ListSerializer} to
 * unwrap.
 */
@Internal
public class OperatorStateTableMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String stateName;
    private final SavepointConnectorOptions.StateReaderMode kind;
    private final LogicalType valueLogicalType;
    private final TypeSerializer<?> valueTypeSerializer;

    public OperatorStateTableMapping(
            String stateName,
            SavepointConnectorOptions.StateReaderMode kind,
            LogicalType valueLogicalType,
            TypeSerializer<?> valueTypeSerializer) {
        Preconditions.checkArgument(
                kind == SavepointConnectorOptions.StateReaderMode.LIST
                        || kind == SavepointConnectorOptions.StateReaderMode.UNION,
                "Operator state tables only support LIST and UNION states, got: " + kind);
        this.stateName = stateName;
        this.kind = kind;
        this.valueLogicalType = valueLogicalType;
        this.valueTypeSerializer = valueTypeSerializer;
    }

    public String getStateName() {
        return stateName;
    }

    public SavepointConnectorOptions.StateReaderMode getKind() {
        return kind;
    }

    public LogicalType getValueLogicalType() {
        return valueLogicalType;
    }

    public TypeSerializer<?> getValueTypeSerializer() {
        return valueTypeSerializer;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Validates that the table schema has at least one physical column, matching the flattened (one
     * column per value field, or a single value column for a scalar value) LIST/UNION table shape.
     * This is a purely structural check; it performs no I/O or class loading.
     */
    public static void validateSchema(ResolvedCatalogTable catalogTable) {
        if (catalogTable.getResolvedSchema().getColumns().isEmpty()) {
            throw new ValidationException(
                    "LIST/UNION state tables must have at least 1 column, but found none.");
        }
    }

    /**
     * Builds a complete {@link OperatorStateTableMapping}, loading non-keyed operator state
     * metadata from the savepoint and resolving the value's logical type and serializer from it.
     *
     * <p>Assumes {@link #validateSchema} has already been called. This performs I/O; callers should
     * invoke it lazily, deferred to scan time, to keep planning free of savepoint access.
     */
    public static OperatorStateTableMapping from(
            String stateName,
            String statePath,
            OperatorIdentifier operatorIdentifier,
            SerializerConfig serializerConfig,
            SavepointConnectorOptions.StateReaderMode kind) {

        NonKeyedOperatorStateMetadata operatorMetadata =
                TableMappingSupport.loadNonKeyedOperatorMetadata(statePath, operatorIdentifier);

        StateMetaInfoSnapshot stateMetaInfo =
                operatorMetadata.operatorStateSnapshots.get(stateName);
        if (stateMetaInfo == null) {
            throw new IllegalArgumentException(
                    "State '"
                            + stateName
                            + "' not found in savepoint metadata for operator '"
                            + operatorIdentifier
                            + "'.");
        }
        LogicalType valueLogicalType =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        stateMetaInfo.getTypeSerializerSnapshot(
                                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));

        SavepointTypeInfoResolver typeResolver =
                new SavepointTypeInfoResolver(
                        operatorMetadata.operatorStateSnapshots, serializerConfig, null);

        TypeSerializer<?> valueTypeSerializer =
                typeResolver.resolveFlatValueSerializer(
                        new RowType.RowField(stateName, valueLogicalType));

        return new OperatorStateTableMapping(
                stateName, kind, valueLogicalType, valueTypeSerializer);
    }
}
