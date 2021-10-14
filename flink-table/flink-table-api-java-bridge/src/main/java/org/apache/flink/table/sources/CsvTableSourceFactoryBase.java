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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.descriptors.OldCsvValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.COMMENT;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_COMMENT_PREFIX;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELDS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_IGNORE_FIRST_LINE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_IGNORE_PARSE_ERRORS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_LINE_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_QUOTE_CHARACTER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory base for creating configured instances of {@link CsvTableSource}.
 *
 * @deprecated The legacy CSV connector has been replaced by {@link FileSource}. It is kept only to
 *     support tests for the legacy connector stack.
 */
@Internal
@Deprecated
public abstract class CsvTableSourceFactoryBase implements TableFactory {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
        context.put(TableFactoryService.FORMAT_TYPE, FORMAT_TYPE_VALUE);
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        context.put(TableFactoryService.FORMAT_PROPERTY_VERSION, "1");
        return context;
    }

    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // connector
        properties.add(CONNECTOR_PATH);
        // format
        properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.TYPE);
        properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.DATA_TYPE);
        properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.NAME);
        properties.add(TableFactoryService.FORMAT_DERIVE_SCHEMA);
        properties.add(FORMAT_FIELD_DELIMITER);
        properties.add(FORMAT_LINE_DELIMITER);
        properties.add(FORMAT_QUOTE_CHARACTER);
        properties.add(FORMAT_COMMENT_PREFIX);
        properties.add(FORMAT_IGNORE_FIRST_LINE);
        properties.add(FORMAT_IGNORE_PARSE_ERRORS);
        properties.add(CONNECTOR_PATH);
        // schema
        properties.add(SCHEMA + ".#." + DescriptorProperties.TYPE);
        properties.add(SCHEMA + ".#." + DescriptorProperties.DATA_TYPE);
        properties.add(SCHEMA + ".#." + DescriptorProperties.NAME);
        properties.add(SCHEMA + ".#." + DescriptorProperties.EXPR);
        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);
        // table constraint
        properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
        properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);
        // comment
        properties.add(COMMENT);

        return properties;
    }

    protected CsvTableSource createTableSource(
            Boolean isStreaming, Map<String, String> properties) {

        DescriptorProperties params = new DescriptorProperties();
        params.putProperties(properties);

        // validate
        new FileSystemValidator().validate(params);
        new OldCsvValidator().validate(params);
        new SchemaValidator(isStreaming, false, false).validate(params);

        // build
        CsvTableSource.Builder csvTableSourceBuilder = new CsvTableSource.Builder();

        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(params.getTableSchema(SCHEMA));

        // if a schema is defined, no matter derive schema is set or not, will use the defined
        // schema
        final boolean hasSchema = params.hasPrefix(FORMAT_FIELDS);
        if (hasSchema) {
            TableSchema formatSchema = params.getTableSchema(FORMAT_FIELDS);
            // the CsvTableSource needs some rework first
            // for now the schema must be equal to the encoding
            // Ignore conversion classes in DataType
            if (!getFieldLogicalTypes(formatSchema).equals(getFieldLogicalTypes(tableSchema))) {
                throw new TableException(
                        String.format(
                                "Encodings that differ from the schema are not supported yet for"
                                        + " CsvTableSource, format schema is '%s', but table schema is '%s'.",
                                formatSchema, tableSchema));
            }
        }

        params.getOptionalString(CONNECTOR_PATH).ifPresent(csvTableSourceBuilder::path);
        params.getOptionalString(FORMAT_FIELD_DELIMITER)
                .ifPresent(csvTableSourceBuilder::fieldDelimiter);
        params.getOptionalString(FORMAT_LINE_DELIMITER)
                .ifPresent(csvTableSourceBuilder::lineDelimiter);

        for (int i = 0; i < tableSchema.getFieldCount(); ++i) {
            csvTableSourceBuilder.field(
                    tableSchema.getFieldNames()[i], tableSchema.getFieldDataTypes()[i]);
        }
        params.getOptionalCharacter(FORMAT_QUOTE_CHARACTER)
                .ifPresent(csvTableSourceBuilder::quoteCharacter);
        params.getOptionalString(FORMAT_COMMENT_PREFIX)
                .ifPresent(csvTableSourceBuilder::commentPrefix);
        params.getOptionalBoolean(FORMAT_IGNORE_FIRST_LINE)
                .ifPresent(
                        flag -> {
                            if (flag) {
                                csvTableSourceBuilder.ignoreFirstLine();
                            }
                        });

        params.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS)
                .ifPresent(
                        flag -> {
                            if (flag) {
                                csvTableSourceBuilder.ignoreParseErrors();
                            }
                        });

        return csvTableSourceBuilder.build();
    }

    public static List<LogicalType> getFieldLogicalTypes(TableSchema schema) {
        return Arrays.stream(schema.getFieldDataTypes())
                .map(DataType::getLogicalType)
                .collect(Collectors.toList());
    }
}
