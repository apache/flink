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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME;

/**
 * Maps the key column, the window (namespace) column and the state value columns to their positions
 * in the output row, for the general namespaced (e.g. window-scoped) keyed state table.
 *
 * <p>The key and window columns are identified by name ({@link #KEY_COLUMN_NAME}, {@link
 * #WINDOW_COLUMN_NAME}) rather than by primary key constraint or position, since namespaced
 * (window) tables never declare a primary key (see {@link #validateAndExtractKeyAndWindowColumns}).
 *
 * <p>Projection push-down remaps column indices exactly as in {@link StateTableMapping}.
 */
@Internal
public class WindowStateTableMapping implements Serializable, MultiColumnStateMapping {

    private static final long serialVersionUID = 1L;

    public static final String KEY_COLUMN_NAME = "state_key";
    public static final String WINDOW_COLUMN_NAME = "state_window";

    private final int keyColumnIndex;
    private final int windowColumnIndex;
    private final List<StateValueColumnConfiguration> valueColumns;
    private final List<StateValueColumnConfiguration> allValueColumns;

    /** Resolved key type info; {@code null} when the mapping was not created via {@link #from}. */
    @Nullable private final TypeInformation<?> keyTypeInfo;

    /**
     * Serializer of the namespace (e.g. window) under which the value states are registered; {@code
     * null} when the mapping was not created via {@link #from}.
     */
    @Nullable private final TypeSerializer<Object> namespaceSerializer;

    public WindowStateTableMapping(
            int keyColumnIndex,
            int windowColumnIndex,
            List<StateValueColumnConfiguration> valueColumns) {
        this(keyColumnIndex, windowColumnIndex, null, valueColumns, valueColumns, null);
    }

    private WindowStateTableMapping(
            int keyColumnIndex,
            int windowColumnIndex,
            @Nullable TypeInformation<?> keyTypeInfo,
            List<StateValueColumnConfiguration> valueColumns,
            List<StateValueColumnConfiguration> allValueColumns,
            @Nullable TypeSerializer<Object> namespaceSerializer) {
        this.keyColumnIndex = keyColumnIndex;
        this.windowColumnIndex = windowColumnIndex;
        this.keyTypeInfo = keyTypeInfo;
        this.valueColumns = valueColumns;
        this.allValueColumns = allValueColumns;
        this.namespaceSerializer = namespaceSerializer;
    }

    public int getKeyColumnIndex() {
        return keyColumnIndex;
    }

    public int getWindowColumnIndex() {
        return windowColumnIndex;
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

    @Nullable
    public TypeSerializer<Object> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    /**
     * Creates a new {@link WindowStateTableMapping} with column indices remapped to the projected
     * output. Only flat projections ({@code projectedFields[i].length == 1}) are supported.
     */
    @Override
    public WindowStateTableMapping project(int[][] projectedFields) {
        return new WindowStateTableMapping(
                TableMappingSupport.remapColumnIndex(projectedFields, this.keyColumnIndex),
                TableMappingSupport.remapColumnIndex(projectedFields, this.windowColumnIndex),
                keyTypeInfo,
                TableMappingSupport.remapValueColumns(projectedFields, this.valueColumns),
                allValueColumns,
                namespaceSerializer);
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Validates the table schema, registers all per-field connector {@link ConfigOption}s into
     * {@code optionalOptions} for option validation, and returns {@code {keyColumnIndex,
     * windowColumnIndex}} in the physical row data type.
     *
     * <p>Unlike {@link StateTableMapping}, the key and window columns are identified by name
     * ({@link #KEY_COLUMN_NAME}, {@link #WINDOW_COLUMN_NAME}) rather than by primary key
     * constraint: Flink's primary-key validation for arbitrary (including zero-field) {@code
     * ROW}-typed columns such as {@code state_window} is not reliable, so namespaced (window)
     * tables never declare one.
     *
     * <p>This is a purely structural operation: it analyses the table schema but performs no class
     * loading or type resolution. Call this eagerly so that option validation passes, then wrap
     * {@link #from} in a {@code Supplier} to defer class loading to scan time.
     */
    public static int[] validateAndExtractKeyAndWindowColumns(
            ResolvedCatalogTable catalogTable, Set<ConfigOption<?>> optionalOptions) {

        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        int keyIdx = TableMappingSupport.columnIndex(physicalDataType, KEY_COLUMN_NAME);
        int windowIdx = TableMappingSupport.columnIndex(physicalDataType, WINDOW_COLUMN_NAME);
        if (keyIdx < 0 || windowIdx < 0) {
            throw new ValidationException(
                    "Namespaced (window) keyed state tables must have a '"
                            + (keyIdx < 0 ? KEY_COLUMN_NAME : WINDOW_COLUMN_NAME)
                            + "' column.");
        }

        RowType rowType = (RowType) physicalDataType.getLogicalType();
        for (int colIdx :
                TableMappingSupport.valueColumnIndices(physicalDataType, keyIdx, windowIdx)) {
            RowType.RowField valueRowField = rowType.getFields().get(colIdx);
            optionalOptions.add(
                    TableMappingSupport.fieldOption(valueRowField.getName(), STATE_NAME)
                            .stringType()
                            .noDefaultValue());
        }

        return new int[] {keyIdx, windowIdx};
    }

    /**
     * Builds a complete {@link WindowStateTableMapping} from a {@link ResolvedCatalogTable},
     * loading operator state metadata from the savepoint and resolving serializers, key type and
     * namespace serializer from it.
     *
     * <p>Assumes {@link #validateAndExtractKeyAndWindowColumns} has already been called for this
     * table. This performs I/O (savepoint metadata loading); callers should invoke it lazily,
     * deferred to scan time, to keep planning free of savepoint access.
     */
    public static WindowStateTableMapping from(
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
        int keyIdx = TableMappingSupport.columnIndex(physicalDataType, KEY_COLUMN_NAME);
        int windowIdx = TableMappingSupport.columnIndex(physicalDataType, WINDOW_COLUMN_NAME);

        TypeInformation<?> keyTypeInfo =
                typeResolver.resolveKeyType(rowType.getFields().get(keyIdx));

        List<StateValueColumnConfiguration> valueColumns = new ArrayList<>();
        for (int colIdx :
                TableMappingSupport.valueColumnIndices(physicalDataType, keyIdx, windowIdx)) {
            valueColumns.add(
                    TableMappingSupport.createValueColumnConfig(
                            colIdx, rowType, options, typeResolver));
        }
        if (valueColumns.isEmpty()) {
            throw new ValidationException(
                    "Namespaced (window) keyed state tables must have at least one state column.");
        }

        @SuppressWarnings("unchecked")
        TypeSerializer<Object> namespaceSerializer =
                (TypeSerializer<Object>)
                        typeResolver.resolveNamespaceSerializer(valueColumns.get(0).getStateName());

        return new WindowStateTableMapping(
                keyIdx, windowIdx, keyTypeInfo, valueColumns, valueColumns, namespaceSerializer);
    }
}
