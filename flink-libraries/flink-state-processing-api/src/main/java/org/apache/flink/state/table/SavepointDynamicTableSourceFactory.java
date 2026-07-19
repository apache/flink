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
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID;
import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID_HASH;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_BACKEND_TYPE;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_PATH;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_READER_MODE;
import static org.apache.flink.state.table.SavepointConnectorOptionsUtil.getOperatorIdentifier;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Dynamic source factory for {@link SavepointDynamicTableSource}. */
public class SavepointDynamicTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        SerializerConfig serializerConfig = new SerializerConfigImpl(options);

        final String stateBackendType = options.getOptional(STATE_BACKEND_TYPE).orElse(null);
        final String statePath = options.get(STATE_PATH);
        final OperatorIdentifier operatorIdentifier = getOperatorIdentifier(options);

        SavepointConnectorOptions.StateReaderMode readerMode = options.get(STATE_READER_MODE);
        switch (readerMode) {
            case KEYED:
                return createKeyedDynamicTableSource(
                        context,
                        options,
                        serializerConfig,
                        stateBackendType,
                        statePath,
                        operatorIdentifier);
            case KEYED_FLAT:
                return createFlattenedDynamicTableSource(
                        context,
                        options,
                        serializerConfig,
                        stateBackendType,
                        statePath,
                        operatorIdentifier);
            default:
                throw new IllegalArgumentException("Unsupported state reader mode: " + readerMode);
        }
    }

    /**
     * Creates a {@link SavepointDynamicTableSource} for the general keyed-state table (selected via
     * {@link SavepointConnectorOptions#STATE_READER_MODE} being set to {@link
     * SavepointConnectorOptions.StateReaderMode#KEYED}, the default).
     */
    private DynamicTableSource createKeyedDynamicTableSource(
            Context context,
            Configuration options,
            SerializerConfig serializerConfig,
            String stateBackendType,
            String statePath,
            OperatorIdentifier operatorIdentifier) {

        Set<ConfigOption<?>> requiredOptions = new HashSet<>(requiredOptions());
        Set<ConfigOption<?>> optionalOptions = new HashSet<>(optionalOptions());

        // Validate schema and register per-field options eagerly (no class loading) so that
        // option validation passes at planning time.
        int keyColumnIndex =
                StateTableMapping.validateAndExtractKeyColumn(
                        context.getCatalogTable(), optionalOptions);

        validateOptions(options, requiredOptions, optionalOptions);

        // Defer I/O and class loading to scan time by creating the StateTableMapping lazily.
        Supplier<StateTableMapping> mappingSupplier =
                () ->
                        StateTableMapping.from(
                                context.getCatalogTable(),
                                options,
                                statePath,
                                operatorIdentifier,
                                serializerConfig);

        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        return new SavepointDynamicTableSource<>(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyColumnIndex,
                mappingSupplier,
                rowType,
                "Savepoint Table Source",
                SavepointDataStreamScanProvider::new);
    }

    /**
     * Creates a {@link FlattenedSavepointDynamicTableSource} for a table exposing a single
     * flattened LIST/MAP state (selected via {@link SavepointConnectorOptions#STATE_READER_MODE}
     * being set to {@link SavepointConnectorOptions.StateReaderMode#KEYED_FLAT}). The state name is
     * resolved from {@link SavepointConnectorOptions#FLATTENED_STATE_NAME}.
     */
    private DynamicTableSource createFlattenedDynamicTableSource(
            Context context,
            Configuration options,
            SerializerConfig serializerConfig,
            String stateBackendType,
            String statePath,
            OperatorIdentifier operatorIdentifier) {

        SavepointConnectorOptions.StateType stateType =
                FlattenedStateTableMapping.validateFlattenedSchema(context.getCatalogTable());

        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        String stateName = validateAndGetFlattenedStateName(options);

        // Defer I/O to scan time by creating the mapping lazily.
        Supplier<FlattenedStateTableMapping> mappingSupplier =
                () ->
                        FlattenedStateTableMapping.from(
                                context.getCatalogTable(),
                                stateName,
                                statePath,
                                operatorIdentifier,
                                serializerConfig,
                                stateType);

        return new FlattenedSavepointDynamicTableSource<>(
                stateBackendType,
                statePath,
                operatorIdentifier,
                FlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX,
                mappingSupplier,
                rowType,
                "Flattened Savepoint Table Source",
                FlattenedSavepointDataStreamScanProvider::new);
    }

    /**
     * Validates {@code options} against the required/optional option sets extended with {@link
     * SavepointConnectorOptions#FLATTENED_STATE_NAME}, and returns the resolved state name — shared
     * by every table kind whose columns represent a single named state's flattened value fields (or
     * a single scalar value column) rather than encoding the state's name via the column layout
     * itself.
     */
    private String validateAndGetFlattenedStateName(Configuration options) {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>(requiredOptions());
        requiredOptions.add(SavepointConnectorOptions.FLATTENED_STATE_NAME);
        Set<ConfigOption<?>> optionalOptions = new HashSet<>(optionalOptions());

        validateOptions(options, requiredOptions, optionalOptions);

        return options.get(SavepointConnectorOptions.FLATTENED_STATE_NAME);
    }

    /**
     * Validates {@code options} against the given required/optional option sets and ensures no
     * unrecognized keys remain (shared by both the general and flattened table source paths).
     */
    private void validateOptions(
            Configuration options,
            Set<ConfigOption<?>> requiredOptions,
            Set<ConfigOption<?>> optionalOptions) {
        FactoryUtil.validateFactoryOptions(requiredOptions, optionalOptions, options);

        Set<String> consumedOptionKeys = new HashSet<>();
        consumedOptionKeys.add(CONNECTOR.key());
        requiredOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        FactoryUtil.validateUnconsumedKeys(
                factoryIdentifier(), options.keySet(), consumedOptionKeys);
    }

    @Override
    public String factoryIdentifier() {
        return "savepoint";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(STATE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();

        options.add(STATE_BACKEND_TYPE);

        // Either UID or hash
        options.add(OPERATOR_UID);
        options.add(OPERATOR_UID_HASH);

        // Multiple values can be read so registering placeholders
        options.add(STATE_NAME_PLACEHOLDER);

        // Selects between the general and flattened keyed-state table schemas; set automatically
        // by StateCatalog.
        options.add(STATE_READER_MODE);

        // Required only for STATE_READER_MODE == KEYED_FLAT/WINDOWED_FLAT (enforced in
        // validateAndGetFlattenedStateName); listed here as optional so that generic option
        // introspection (docs, Table API tooling) can discover it regardless of mode.
        options.add(SavepointConnectorOptions.FLATTENED_STATE_NAME);

        return options;
    }
}
