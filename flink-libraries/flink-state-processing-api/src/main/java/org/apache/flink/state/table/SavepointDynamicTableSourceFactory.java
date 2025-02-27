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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.state.table.SavepointConnectorOptions.FIELDS;
import static org.apache.flink.state.table.SavepointConnectorOptions.MAP_KEY_FORMAT;
import static org.apache.flink.state.table.SavepointConnectorOptions.MAP_KEY_FORMAT_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID;
import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID_HASH;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_BACKEND_TYPE;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_NAME_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_PATH;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_TYPE;
import static org.apache.flink.state.table.SavepointConnectorOptions.STATE_TYPE_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.state.table.SavepointConnectorOptions.VALUE_FORMAT_PLACEHOLDER;
import static org.apache.flink.state.table.SavepointConnectorOptionsUtil.getOperatorIdentifier;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Dynamic source factory for {@link SavepointDynamicTableSource}. */
public class SavepointDynamicTableSourceFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);

        final String stateBackendType = options.getOptional(STATE_BACKEND_TYPE).orElse(null);
        final String statePath = options.get(STATE_PATH);
        final OperatorIdentifier operatorIdentifier = getOperatorIdentifier(options);

        final Tuple2<Integer, int[]> keyValueProjections =
                createKeyValueProjections(context.getCatalogTable());

        LogicalType logicalType = context.getPhysicalRowDataType().getLogicalType();
        Preconditions.checkArgument(logicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        RowType rowType = (RowType) logicalType;

        Set<ConfigOption<?>> requiredOptions = new HashSet<>(requiredOptions());
        Set<ConfigOption<?>> optionalOptions = new HashSet<>(optionalOptions());

        RowType.RowField keyRowField = rowType.getFields().get(keyValueProjections.f0);
        ConfigOption<String> keyFormatOption =
                key(String.format("%s.%s.%s", FIELDS, keyRowField.getName(), VALUE_FORMAT))
                        .stringType()
                        .noDefaultValue();
        optionalOptions.add(keyFormatOption);
        final String keyFormat =
                options.getOptional(keyFormatOption)
                        .orElseGet(
                                () ->
                                        inferStateValueFormat(
                                                keyRowField.getName(), keyRowField.getType()));

        final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueConfigProjections =
                Tuple2.of(
                        keyValueProjections.f0,
                        Arrays.stream(keyValueProjections.f1)
                                .mapToObj(
                                        columnIndex -> {
                                            RowType.RowField valueRowField =
                                                    rowType.getFields().get(columnIndex);

                                            ConfigOption<String> stateNameOption =
                                                    key(String.format(
                                                                    "%s.%s.%s",
                                                                    FIELDS,
                                                                    valueRowField.getName(),
                                                                    STATE_NAME))
                                                            .stringType()
                                                            .noDefaultValue();
                                            optionalOptions.add(stateNameOption);

                                            ConfigOption<SavepointConnectorOptions.StateType>
                                                    stateTypeOption =
                                                            key(String.format(
                                                                            "%s.%s.%s",
                                                                            FIELDS,
                                                                            valueRowField.getName(),
                                                                            STATE_TYPE))
                                                                    .enumType(
                                                                            SavepointConnectorOptions
                                                                                    .StateType
                                                                                    .class)
                                                                    .noDefaultValue();
                                            optionalOptions.add(stateTypeOption);

                                            ConfigOption<String> mapKeyFormatOption =
                                                    key(String.format(
                                                                    "%s.%s.%s",
                                                                    FIELDS,
                                                                    valueRowField.getName(),
                                                                    MAP_KEY_FORMAT))
                                                            .stringType()
                                                            .noDefaultValue();
                                            optionalOptions.add(mapKeyFormatOption);

                                            ConfigOption<String> valueFormatOption =
                                                    key(String.format(
                                                                    "%s.%s.%s",
                                                                    FIELDS,
                                                                    valueRowField.getName(),
                                                                    VALUE_FORMAT))
                                                            .stringType()
                                                            .noDefaultValue();
                                            optionalOptions.add(valueFormatOption);

                                            LogicalType valueLogicalType = valueRowField.getType();
                                            return new StateValueColumnConfiguration(
                                                    columnIndex,
                                                    options.getOptional(stateNameOption)
                                                            .orElse(valueRowField.getName()),
                                                    options.getOptional(stateTypeOption)
                                                            .orElseGet(
                                                                    () ->
                                                                            inferStateType(
                                                                                    valueLogicalType)),
                                                    options.getOptional(mapKeyFormatOption)
                                                            .orElseGet(
                                                                    () ->
                                                                            inferStateMapKeyFormat(
                                                                                    valueRowField
                                                                                            .getName(),
                                                                                    valueLogicalType)),
                                                    options.getOptional(valueFormatOption)
                                                            .orElseGet(
                                                                    () ->
                                                                            inferStateValueFormat(
                                                                                    valueRowField
                                                                                            .getName(),
                                                                                    valueLogicalType)));
                                        })
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
                keyFormat,
                keyValueConfigProjections,
                rowType);
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

    @Nullable
    private String inferStateMapKeyFormat(String columnName, LogicalType logicalType) {
        return logicalType.is(LogicalTypeRoot.MAP)
                ? inferStateValueFormat(columnName, ((MapType) logicalType).getKeyType())
                : null;
    }

    private String inferStateValueFormat(String columnName, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return String.class.getName();

            case BOOLEAN:
                return Boolean.class.getName();

            case BINARY:
            case VARBINARY:
                return byte[].class.getName();

            case DECIMAL:
                return BigDecimal.class.getName();

            case TINYINT:
                return Byte.class.getName();

            case SMALLINT:
                return Short.class.getName();

            case INTEGER:
                return Integer.class.getName();

            case BIGINT:
                return Long.class.getName();

            case FLOAT:
                return Float.class.getName();

            case DOUBLE:
                return Double.class.getName();

            case DATE:
                return Integer.class.getName();

            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return Long.class.getName();

            case ARRAY:
                return inferStateValueFormat(
                        columnName, ((ArrayType) logicalType).getElementType());

            case MAP:
                return inferStateValueFormat(columnName, ((MapType) logicalType).getValueType());

            case NULL:
                return null;

            case ROW:
            case MULTISET:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            case DESCRIPTOR:
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unable to infer state format for SQL type: %s in column: %s. "
                                        + "Please override the type with the following config parameter: %s.%s.%s",
                                logicalType, columnName, FIELDS, columnName, VALUE_FORMAT));
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
        options.add(MAP_KEY_FORMAT_PLACEHOLDER);
        options.add(VALUE_FORMAT_PLACEHOLDER);

        return options;
    }
}
