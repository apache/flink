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

package org.apache.flink.formats.raw;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Format factory for raw format which allows to read and write raw (byte based) values as a single
 * column.
 */
@Internal
public class RawFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "raw";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RawFormatOptions.ENDIANNESS);
        options.add(RawFormatOptions.CHARSET);
        return options;
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String charsetName = validateAndGetCharsetName(formatOptions);
        final boolean isBigEndian = isBigEndian(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType physicalRowType = (RowType) producedDataType.getLogicalType();
                final LogicalType fieldType = validateAndExtractSingleField(physicalRowType);
                final TypeInformation<RowData> producedTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new RawFormatDeserializationSchema(
                        fieldType, producedTypeInfo, charsetName, isBigEndian);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String charsetName = validateAndGetCharsetName(formatOptions);
        final boolean isBigEndian = isBigEndian(formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType physicalRowType = (RowType) consumedDataType.getLogicalType();
                final LogicalType fieldType = validateAndExtractSingleField(physicalRowType);
                return new RawFormatSerializationSchema(fieldType, charsetName, isBigEndian);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    // ------------------------------------------------------------------------------------------

    private static final Set<LogicalTypeRoot> supportedTypes =
            Sets.newHashSet(
                    LogicalTypeRoot.CHAR,
                    LogicalTypeRoot.VARCHAR,
                    LogicalTypeRoot.BINARY,
                    LogicalTypeRoot.VARBINARY,
                    LogicalTypeRoot.RAW,
                    LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.TINYINT,
                    LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER,
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.FLOAT,
                    LogicalTypeRoot.DOUBLE);

    /** Checks the given field type is supported. */
    private static void checkFieldType(LogicalType fieldType) {
        if (!supportedTypes.contains(fieldType.getTypeRoot())) {
            throw new ValidationException(
                    String.format(
                            "The 'raw' format doesn't supports '%s' as column type.",
                            fieldType.asSummaryString()));
        }
    }

    /** Validates and extract the single field type from the given physical row schema. */
    private static LogicalType validateAndExtractSingleField(RowType physicalRowType) {
        if (physicalRowType.getFieldCount() != 1) {
            String schemaString =
                    physicalRowType.getFields().stream()
                            .map(RowType.RowField::asSummaryString)
                            .collect(Collectors.joining(", "));
            throw new ValidationException(
                    String.format(
                            "The 'raw' format only supports single physical column. "
                                    + "However the defined schema contains multiple physical columns: [%s]",
                            schemaString));
        }
        LogicalType fieldType = physicalRowType.getChildren().get(0);
        checkFieldType(fieldType);
        return fieldType;
    }

    private static boolean isBigEndian(ReadableConfig formatOptions) {
        String endiannessName = formatOptions.get(RawFormatOptions.ENDIANNESS);
        if (RawFormatOptions.BIG_ENDIAN.equalsIgnoreCase(endiannessName)) {
            return true;
        } else if (RawFormatOptions.LITTLE_ENDIAN.equalsIgnoreCase(endiannessName)) {
            return false;
        } else {
            throw new ValidationException(
                    String.format(
                            "Unsupported endianness name: %s. "
                                    + "Valid values of '%s.%s' option are 'big-endian' and 'little-endian'.",
                            endiannessName, IDENTIFIER, RawFormatOptions.ENDIANNESS.key()));
        }
    }

    private static String validateAndGetCharsetName(ReadableConfig formatOptions) {
        String charsetName = formatOptions.get(RawFormatOptions.CHARSET);
        try {
            Charset.forName(charsetName);
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Unsupported '%s.%s' name: %s.",
                            IDENTIFIER, RawFormatOptions.CHARSET.key(), charsetName),
                    e);
        }
        return charsetName;
    }
}
