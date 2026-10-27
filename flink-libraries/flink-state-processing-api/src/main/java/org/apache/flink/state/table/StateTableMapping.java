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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME;

/**
 * Maps the key column and state value columns to their positions in the output row.
 *
 * <p>After projection push-down, column indices reflect positions in the projected output row
 * rather than the original table schema. The {@link #allValueColumns} list always contains the full
 * original set of value columns (needed for key enumeration), while {@link #valueColumns} contains
 * only the projected subset that is written to the output row.
 */
@Internal
public class StateTableMapping implements Serializable, MultiColumnStateMapping {

    private static final long serialVersionUID = 1L;

    private final int keyColumnIndex;
    private final List<StateValueColumnConfiguration> valueColumns;

    /**
     * Full original value columns, preserved across projections for state-descriptor registration.
     * Key enumeration in {@code KeyedStateReaderOperator} requires at least one registered state;
     * this list guarantees that even when all value columns are projected out.
     */
    private final List<StateValueColumnConfiguration> allValueColumns;

    /** Resolved key type info; {@code null} when the mapping was not created via {@link #from}. */
    @Nullable private final TypeInformation<?> keyTypeInfo;

    public StateTableMapping(int keyColumnIndex, List<StateValueColumnConfiguration> valueColumns) {
        this(keyColumnIndex, null, valueColumns, valueColumns);
    }

    private StateTableMapping(
            int keyColumnIndex,
            @Nullable TypeInformation<?> keyTypeInfo,
            List<StateValueColumnConfiguration> valueColumns,
            List<StateValueColumnConfiguration> allValueColumns) {
        this.keyColumnIndex = keyColumnIndex;
        this.keyTypeInfo = keyTypeInfo;
        this.valueColumns = valueColumns;
        this.allValueColumns = allValueColumns;
    }

    public int getKeyColumnIndex() {
        return keyColumnIndex;
    }

    /** Columns to write to the output row (may be a projected subset). */
    public List<StateValueColumnConfiguration> getValueColumns() {
        return valueColumns;
    }

    @Override
    public List<StateValueColumnConfiguration> getAllValueColumns() {
        return allValueColumns;
    }

    @Override
    @Nullable
    public TypeInformation<?> getKeyTypeInfo() {
        return keyTypeInfo;
    }

    /**
     * Creates a new {@link StateTableMapping} with column indices remapped to the projected output.
     * Only flat projections ({@code projectedFields[i].length == 1}) are supported; a key that was
     * projected out (e.g. after constant folding with filter push-down) maps to {@code -1}, meaning
     * it is not written to the output row.
     */
    @Override
    public StateTableMapping project(int[][] projectedFields) {
        return new StateTableMapping(
                TableMappingSupport.remapColumnIndex(projectedFields, this.keyColumnIndex),
                keyTypeInfo,
                TableMappingSupport.remapValueColumns(projectedFields, this.valueColumns),
                allValueColumns);
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Validates the table schema, registers all per-field connector {@link ConfigOption}s into
     * {@code optionalOptions} for option validation, and returns the index of the primary-key
     * column in the physical row data type.
     *
     * <p>This is a purely structural operation: it analyses the table schema but performs no class
     * loading or type resolution. Call this eagerly so that option validation passes, then wrap
     * {@link #from} in a {@code Supplier} to defer class loading to scan time.
     */
    public static int validateAndExtractKeyColumn(
            ResolvedCatalogTable catalogTable, Set<ConfigOption<?>> optionalOptions) {

        ResolvedSchema schema = catalogTable.getResolvedSchema();
        if (schema.getPrimaryKey().isEmpty()) {
            throw new ValidationException("Could not find the primary key in the table schema.");
        }

        List<String> keyFields = schema.getPrimaryKey().get().getColumns();
        if (keyFields.size() != 1) {
            throw new ValidationException(
                    "Only a single primary key must be defined in the table schema.");
        }

        DataType physicalDataType = schema.toPhysicalRowDataType();
        int keyIdx = TableMappingSupport.columnIndex(physicalDataType, keyFields.get(0));
        RowType rowType = (RowType) physicalDataType.getLogicalType();

        for (int colIdx : TableMappingSupport.valueColumnIndices(physicalDataType, keyIdx)) {
            RowType.RowField valueRowField = rowType.getFields().get(colIdx);
            optionalOptions.add(
                    TableMappingSupport.fieldOption(valueRowField.getName(), STATE_NAME)
                            .stringType()
                            .noDefaultValue());
        }

        return keyIdx;
    }

    /**
     * Builds a complete {@link StateTableMapping} from a {@link ResolvedCatalogTable}, loading
     * operator state metadata from the savepoint and resolving serializers and key type from it.
     *
     * <p>Assumes {@link #validateAndExtractKeyColumn} has already been called for this table. This
     * performs I/O (savepoint metadata loading); callers should invoke it lazily, deferred to scan
     * time, to keep planning free of savepoint access.
     */
    public static StateTableMapping from(
            ResolvedCatalogTable catalogTable,
            Configuration options,
            String statePath,
            OperatorIdentifier operatorIdentifier,
            SerializerConfig serializerConfig) {

        SavepointTypeInfoResolver typeResolver =
                TableMappingSupport.createTypeResolver(
                        statePath, operatorIdentifier, serializerConfig);

        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        List<String> keyFields =
                catalogTable.getResolvedSchema().getPrimaryKey().get().getColumns();
        int keyIdx = TableMappingSupport.columnIndex(physicalDataType, keyFields.get(0));

        TypeInformation<?> keyTypeInfo =
                typeResolver.resolveKeyType(rowType.getFields().get(keyIdx));

        List<StateValueColumnConfiguration> valueColumns = new ArrayList<>();
        for (int colIdx : TableMappingSupport.valueColumnIndices(physicalDataType, keyIdx)) {
            valueColumns.add(
                    TableMappingSupport.createValueColumnConfig(
                            colIdx, rowType, options, typeResolver));
        }

        return new StateTableMapping(keyIdx, keyTypeInfo, valueColumns, valueColumns);
    }
}
