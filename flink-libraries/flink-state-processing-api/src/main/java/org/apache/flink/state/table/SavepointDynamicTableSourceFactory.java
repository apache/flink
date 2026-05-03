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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.state.table.SavepointConnectorOptions.FIELDS;
import static org.apache.flink.state.table.SavepointConnectorOptions.KEY_CLASS;
import static org.apache.flink.state.table.SavepointConnectorOptions.KEY_CLASS_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.KEY_TYPE_FACTORY;
import static org.apache.flink.state.table.SavepointConnectorOptions.KEY_TYPE_INFO_FACTORY_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID;
import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID_HASH;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_BACKEND_TYPE;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_PATH;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_TYPE;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_TYPE_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_CLASS;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_CLASS_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_TYPE_FACTORY;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_TYPE_INFO_FACTORY_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptionsUtil.getOperatorIdentifier;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Dynamic source factory for {@link SavepointDynamicTableSource}. */
public class SavepointDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(SavepointDynamicTableSourceFactory.class);

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        SerializerConfig serializerConfig = new SerializerConfigImpl(options);

        final String stateBackendType = options.getOptional(STATE_BACKEND_TYPE).orElse(null);
        final String statePath = options.get(STATE_PATH);
        final OperatorIdentifier operatorIdentifier = getOperatorIdentifier(options);

        final Map<String, StateMetaInfoSnapshot> preloadedStateMetadata =
                preloadStateMetadata(statePath, operatorIdentifier);

        // Create resolver with preloaded metadata
        SavepointTypeInfoResolver typeResolver =
                new SavepointTypeInfoResolver(preloadedStateMetadata, serializerConfig);

        final Tuple2<Integer, int[]> keyValueProjections =
                createKeyValueProjections(context.getCatalogTable());

        LogicalType logicalType = context.getPhysicalRowDataType().getLogicalType();
        Preconditions.checkArgument(logicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        RowType rowType = (RowType) logicalType;

        Set<ConfigOption<?>> requiredOptions = new HashSet<>(requiredOptions());
        Set<ConfigOption<?>> optionalOptions = new HashSet<>(optionalOptions());

        RowType.RowField keyRowField = rowType.getFields().get(keyValueProjections.f0);
        ConfigOption<String> keyFormatOption =
                optionOf(keyRowField.getName(), VALUE_CLASS).stringType().noDefaultValue();
        optionalOptions.add(keyFormatOption);

        ConfigOption<String> keyTypeInfoFactoryOption =
                optionOf(keyRowField.getName(), VALUE_TYPE_FACTORY).stringType().noDefaultValue();
        optionalOptions.add(keyTypeInfoFactoryOption);

        TypeInformation<?> keyTypeInfo =
                typeResolver.resolveKeyType(
                        options, keyFormatOption, keyTypeInfoFactoryOption, keyRowField);

        final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueConfigProjections =
                Tuple2.of(
                        keyValueProjections.f0,
                        Arrays.stream(keyValueProjections.f1)
                                .mapToObj(
                                        columnIndex ->
                                                createStateColumnConfiguration(
                                                        columnIndex,
                                                        rowType,
                                                        options,
                                                        optionalOptions,
                                                        typeResolver))
                                .collect(Collectors.toList()));
        FactoryUtil.validateFactoryOptions(requiredOptions, optionalOptions, options);

        Set<String> consumedOptionKeys = new HashSet<>();
        consumedOptionKeys.add(CONNECTOR.key());
        requiredOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        FactoryUtil.validateUnconsumedKeys(
                factoryIdentifier(), options.keySet(), consumedOptionKeys);

        return new SavepointDynamicTableSource(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyTypeInfo,
                keyValueConfigProjections,
                rowType);
    }

    private StateValueColumnConfiguration createStateColumnConfiguration(
            int columnIndex,
            RowType rowType,
            Configuration options,
            Set<ConfigOption<?>> optionalOptions,
            SavepointTypeInfoResolver typeResolver) {

        RowType.RowField valueRowField = rowType.getFields().get(columnIndex);

        ConfigOption<String> stateNameOption =
                optionOf(valueRowField.getName(), STATE_NAME).stringType().noDefaultValue();
        optionalOptions.add(stateNameOption);

        ConfigOption<SavepointConnectorOptions.StateType> stateTypeOption =
                optionOf(valueRowField.getName(), STATE_TYPE)
                        .enumType(SavepointConnectorOptions.StateType.class)
                        .noDefaultValue();
        optionalOptions.add(stateTypeOption);

        ConfigOption<String> mapKeyFormatOption =
                optionOf(valueRowField.getName(), KEY_CLASS).stringType().noDefaultValue();
        optionalOptions.add(mapKeyFormatOption);

        ConfigOption<String> mapKeyTypeInfoFactoryOption =
                optionOf(valueRowField.getName(), KEY_TYPE_FACTORY).stringType().noDefaultValue();
        optionalOptions.add(mapKeyTypeInfoFactoryOption);

        ConfigOption<String> valueFormatOption =
                optionOf(valueRowField.getName(), VALUE_CLASS).stringType().noDefaultValue();
        optionalOptions.add(valueFormatOption);

        ConfigOption<String> valueTypeInfoFactoryOption =
                optionOf(valueRowField.getName(), VALUE_TYPE_FACTORY).stringType().noDefaultValue();
        optionalOptions.add(valueTypeInfoFactoryOption);

        LogicalType valueLogicalType = valueRowField.getType();

        SavepointConnectorOptions.StateType stateType =
                options.getOptional(stateTypeOption)
                        .orElseGet(() -> inferStateType(valueLogicalType));

        TypeSerializer<?> mapKeyTypeSerializer =
                typeResolver.resolveSerializer(
                        options,
                        mapKeyFormatOption,
                        mapKeyTypeInfoFactoryOption,
                        valueRowField,
                        stateType.equals(SavepointConnectorOptions.StateType.MAP),
                        SavepointTypeInfoResolver.InferenceContext.MAP_KEY);

        TypeSerializer<?> valueTypeSerializer =
                typeResolver.resolveSerializer(
                        options,
                        valueFormatOption,
                        valueTypeInfoFactoryOption,
                        valueRowField,
                        true,
                        SavepointTypeInfoResolver.InferenceContext.VALUE);

        return new StateValueColumnConfiguration(
                columnIndex,
                options.getOptional(stateNameOption).orElse(valueRowField.getName()),
                stateType,
                mapKeyTypeSerializer,
                valueTypeSerializer);
    }

    private static ConfigOptions.OptionBuilder optionOf(String rowField, String optionName) {
        return ConfigOptions.key(String.format("%s.%s.%s", FIELDS, rowField, optionName));
    }

    private Tuple2<Integer, int[]> createKeyValueProjections(ResolvedCatalogTable catalogTable) {
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
        int keyProjection = createKeyFormatProjection(physicalDataType, keyFields.get(0));
        int[] valueProjection = createValueFormatProjection(physicalDataType, keyProjection);

        return Tuple2.of(keyProjection, valueProjection);
    }

    private int createKeyFormatProjection(DataType physicalDataType, String keyField) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return physicalFields.indexOf(keyField);
    }

    private int[] createValueFormatProjection(DataType physicalDataType, int keyProjection) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        return physicalFields.filter(pos -> keyProjection != pos).toArray();
    }

    private SavepointConnectorOptions.StateType inferStateType(LogicalType logicalType) {
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
     * Preloads all state metadata for an operator in a single I/O operation.
     *
     * @param savepointPath Path to the savepoint
     * @param operatorIdentifier Operator UID or hash
     * @return Map from state name to StateMetaInfoSnapshot
     */
    private Map<String, StateMetaInfoSnapshot> preloadStateMetadata(
            String savepointPath, OperatorIdentifier operatorIdentifier) {
        try {
            return SavepointLoader.loadOperatorStateMetadata(savepointPath, operatorIdentifier);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to load state metadata from savepoint '%s' for operator '%s'. "
                                    + "Ensure the savepoint path is valid and the operator exists in the savepoint. ",
                            savepointPath, operatorIdentifier),
                    e);
        }
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
        options.add(STATE_TYPE_PLACEHOLDER);
        options.add(KEY_CLASS_PLACEHOLDER);
        options.add(KEY_TYPE_INFO_FACTORY_PLACEHOLDER);
        options.add(VALUE_CLASS_PLACEHOLDER);
        options.add(VALUE_TYPE_INFO_FACTORY_PLACEHOLDER);

        return options;
    }
}
