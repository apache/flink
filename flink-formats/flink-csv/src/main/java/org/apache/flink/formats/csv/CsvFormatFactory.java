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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.text.StringEscapeUtils;

import java.util.Collections;
import java.util.Set;

import static org.apache.flink.formats.csv.CsvFormatOptions.ALLOW_COMMENTS;
import static org.apache.flink.formats.csv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.ESCAPE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.FIELD_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.csv.CsvFormatOptions.NULL_LITERAL;
import static org.apache.flink.formats.csv.CsvFormatOptions.QUOTE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION;

/**
 * Format factory for providing configured instances of CSV to RowData {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
@Internal
public final class CsvFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        CsvCommons.validateFormatOptions(formatOptions);

        return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType physicalDataType,
                    int[][] projections) {

                final DataType projectedDataType =
                        Projection.of(projections).project(physicalDataType);
                final RowType projectedRowType = (RowType) projectedDataType.getLogicalType();
                final RowType physicalRowType = (RowType) physicalDataType.getLogicalType();

                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(projectedRowType);

                final CsvRowDataDeserializationSchema.Builder schemaBuilder =
                        new CsvRowDataDeserializationSchema.Builder(
                                physicalRowType, projectedRowType, rowDataTypeInfo);

                configureDeserializationSchema(formatOptions, schemaBuilder);

                return schemaBuilder.build();
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        CsvCommons.validateFormatOptions(formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                final CsvRowDataSerializationSchema.Builder schemaBuilder =
                        new CsvRowDataSerializationSchema.Builder(rowType);
                configureSerializationSchema(formatOptions, schemaBuilder);
                return schemaBuilder.build();
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return CsvCommons.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return CsvCommons.optionalOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return CsvCommons.forwardOptions();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static void configureDeserializationSchema(
            ReadableConfig formatOptions, CsvRowDataDeserializationSchema.Builder schemaBuilder) {
        formatOptions
                .getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setFieldDelimiter);

        if (formatOptions.get(DISABLE_QUOTE_CHARACTER)) {
            schemaBuilder.disableQuoteCharacter();
        } else {
            formatOptions
                    .getOptional(QUOTE_CHARACTER)
                    .map(quote -> quote.charAt(0))
                    .ifPresent(schemaBuilder::setQuoteCharacter);
        }

        formatOptions.getOptional(ALLOW_COMMENTS).ifPresent(schemaBuilder::setAllowComments);

        formatOptions
                .getOptional(IGNORE_PARSE_ERRORS)
                .ifPresent(schemaBuilder::setIgnoreParseErrors);

        formatOptions
                .getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(schemaBuilder::setArrayElementDelimiter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions.getOptional(NULL_LITERAL).ifPresent(schemaBuilder::setNullLiteral);
    }

    private static void configureSerializationSchema(
            ReadableConfig formatOptions, CsvRowDataSerializationSchema.Builder schemaBuilder) {
        formatOptions
                .getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setFieldDelimiter);

        if (formatOptions.get(DISABLE_QUOTE_CHARACTER)) {
            schemaBuilder.disableQuoteCharacter();
        } else {
            formatOptions
                    .getOptional(QUOTE_CHARACTER)
                    .map(quote -> quote.charAt(0))
                    .ifPresent(schemaBuilder::setQuoteCharacter);
        }

        formatOptions
                .getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(schemaBuilder::setArrayElementDelimiter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions.getOptional(NULL_LITERAL).ifPresent(schemaBuilder::setNullLiteral);

        formatOptions
                .getOptional(WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION)
                .ifPresent(schemaBuilder::setWriteBigDecimalInScientificNotation);
    }
}
