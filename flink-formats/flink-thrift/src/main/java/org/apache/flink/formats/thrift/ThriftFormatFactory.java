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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.formats.thrift.typeutils.Utils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.thrift.TBase;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Table format factory for providing configured instances of THRIFT to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public class ThriftFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "thrift";

    public static final ConfigOption<String> CLASS =
            ConfigOptions.key("class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The thrift class, which is used to serialize/deserialize record");

    public static final ConfigOption<String> PROTOCOL =
            ConfigOptions.key("protocol")
                    .stringType()
                    .defaultValue("org.apache.thrift.protocol.TCompactProtocol")
                    .withDescription("TCompactProtocol is default value.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default");

    public static final ConfigOption<Boolean> IGNORE_FIELD_MISMATCH =
            ConfigOptions.key("ignore-field-mismatch")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<ThriftCodeGenerator> CODE_GENERATOR =
            ConfigOptions.key("code-generator")
                    .enumType(ThriftCodeGenerator.class)
                    .defaultValue(ThriftCodeGenerator.THRIFT)
                    .withDescription(
                            "Optional to set thrift code-generator, the default code generator is thrift");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(CLASS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CLASS);
        options.add(PROTOCOL);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(IGNORE_FIELD_MISMATCH);
        return options;
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        final String thriftClassName = formatOptions.get(CLASS);
        final String thriftProtocolName = formatOptions.get(PROTOCOL);
        final ThriftCodeGenerator codeGenerator = formatOptions.get(CODE_GENERATOR);
        final boolean ignoreFieldMismatch = formatOptions.get(IGNORE_FIELD_MISMATCH);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();

                if (!ignoreFieldMismatch) {
                    checkRowTypeMatchClass(rowType, thriftClassName);
                }

                return new ThriftRowDataSerializationSchema(
                        rowType,
                        thriftClassName,
                        thriftProtocolName,
                        codeGenerator,
                        ignoreFieldMismatch);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);

        final String thriftClassName = formatOptions.get(CLASS);
        final String thriftProtocolName = formatOptions.get(PROTOCOL);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        final Boolean ignoreFieldMismatch = formatOptions.get(IGNORE_FIELD_MISMATCH);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                if (!ignoreFieldMismatch) {
                    checkRowTypeMatchClass(rowType, thriftClassName);
                }

                return new ThriftRowDataDeserializationSchema(
                        rowType,
                        rowDataTypeInfo,
                        thriftClassName,
                        thriftProtocolName,
                        ignoreParseErrors,
                        ignoreFieldMismatch);
            }
        };
    }

    public static void validateFormatOptions(ReadableConfig tableOptions) {
        String thriftClassName = tableOptions.get(CLASS);
        String thriftProtocolName = tableOptions.get(PROTOCOL);

        try {
            if (!Utils.isAssignableFrom(TBase.class, thriftClassName)) {
                throw new ValidationException("Invalid thrift class");
            }

            if (!Utils.isAssignableFrom(TProtocolFactory.class, thriftProtocolName)) {
                throw new ValidationException("Invalid thrift protocol factory class");
            }
        } catch (IOException e) {
            throw new ValidationException("Could not find the needed class", e);
        }
    }

    /**
     * determine whether the field in the rowType matches the field in the thrift class. There are
     * three cases will case return false: 1. type mismatch : like declared as map type in thrift
     * but list type in RowType. 2. field mismatch: rowType has more fields than thrift class.
     *
     * <p>It is allowed by default that the fields in rowType is less than the fields in thrift.
     */
    public static void checkRowTypeMatchClass(RowType rowType, String className) {
        try {
            Class<? extends TBase> tClass = Class.forName(className).asSubclass(TBase.class);
            if (!checkRowTypeMatchClass(rowType, new StructMetaData(TType.STRUCT, tClass))) {
                throw new ValidationException("Table schema not match the thrift class");
            }
        } catch (ClassNotFoundException e) {
            throw new ValidationException("Can not get thrift class:" + className);
        }
    }

    public static boolean checkRowTypeMatchClass(LogicalType type, FieldValueMetaData metaData) {
        if (metaData == null) {
            return false;
        }
        switch (type.getTypeRoot()) {
            case NULL:
                return true;
            case BOOLEAN:
                return metaData.type == TType.BOOL;
            case TINYINT:
                return metaData.type == TType.BYTE;
            case SMALLINT:
                return metaData.type == TType.I16;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
                return metaData.type == TType.I32;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case BIGINT:
                return metaData.type == TType.I64;
            case FLOAT:
            case DOUBLE:
                return metaData.type == TType.DOUBLE;
            case VARCHAR:
                return metaData.type == TType.STRING || metaData.type == TType.ENUM;
            case DECIMAL:
            case BINARY:
            case VARBINARY:
                return metaData.type == TType.STRING;
            case MULTISET:
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                if (metaData.type == TType.MAP) {
                    FieldValueMetaData keyMetaData = ((MapMetaData) metaData).keyMetaData;
                    FieldValueMetaData valueMetaData = ((MapMetaData) metaData).valueMetaData;
                    return checkRowTypeMatchClass(keyType, keyMetaData)
                            && checkRowTypeMatchClass(valueType, valueMetaData);
                } else {
                    return false;
                }
            case ARRAY:
                LogicalType arrayValue = ((ArrayType) type).getElementType();
                if (metaData.type == TType.LIST) {
                    return checkRowTypeMatchClass(
                            arrayValue, ((ListMetaData) metaData).elemMetaData);
                } else if (metaData.type == TType.SET) {
                    return checkRowTypeMatchClass(
                            arrayValue, ((SetMetaData) metaData).elemMetaData);
                } else {
                    return false;
                }
            case STRUCTURED_TYPE:
            case ROW:
                RowType rowType = (RowType) type;
                if (metaData.type == TType.STRUCT) {
                    Class<? extends TBase> tClass = ((StructMetaData) metaData).structClass;
                    for (RowType.RowField rowField : rowType.getFields()) {
                        ThriftUtils.getFieldIdEnum(tClass, rowField.getName());
                        FieldValueMetaData fieldValueMetaData =
                                ThriftUtils.getFieldMetaData(tClass, rowField.getName());
                        if (!checkRowTypeMatchClass(rowField.getType(), fieldValueMetaData)) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    return false;
                }

            default:
                return false;
        }
    }
}
