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

package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.ValidationException;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BASIC_AUTH_USER_INFO;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BEARER_AUTH_TOKEN;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.PROPERTIES;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SCHEMA;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SUBJECT;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.URL;
import static org.apache.flink.formats.avro.registry.confluent.RegistryAvroFormatFactory.buildOptionalPropertiesMap;

/**
 * Format factory for providing configured instances of Debezium Avro to RowData {@link
 * DeserializationSchema}.
 */
@Internal
public class DebeziumAvroFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "debezium-avro-confluent";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);
        String schemaRegistryURL = formatOptions.get(URL);
        String schema = formatOptions.getOptional(SCHEMA).orElse(null);
        Map<String, ?> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

        return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType,
                    int[][] projections) {
                producedDataType = Projection.of(projections).project(producedDataType);
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> producedTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new DebeziumAvroDeserializationSchema(
                        rowType,
                        producedTypeInfo,
                        schemaRegistryURL,
                        schema,
                        optionalPropertiesMap);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);
        String schemaRegistryURL = formatOptions.get(URL);
        Optional<String> subject = formatOptions.getOptional(SUBJECT);
        String schema = formatOptions.getOptional(SCHEMA).orElse(null);
        Map<String, ?> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

        if (!subject.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s.%s' is required for serialization",
                            IDENTIFIER, SUBJECT.key()));
        }

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }

            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new DebeziumAvroSerializationSchema(
                        rowType, schemaRegistryURL, subject.get(), schema, optionalPropertiesMap);
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SUBJECT);
        options.add(PROPERTIES);
        options.add(SCHEMA);
        options.add(SSL_KEYSTORE_LOCATION);
        options.add(SSL_KEYSTORE_PASSWORD);
        options.add(SSL_TRUSTSTORE_LOCATION);
        options.add(SSL_TRUSTSTORE_PASSWORD);
        options.add(BASIC_AUTH_CREDENTIALS_SOURCE);
        options.add(BASIC_AUTH_USER_INFO);
        options.add(BEARER_AUTH_CREDENTIALS_SOURCE);
        options.add(BEARER_AUTH_TOKEN);
        return options;
    }

    static void validateSchemaString(@Nullable String schemaString, RowType rowType) {
        if (schemaString != null) {
            LogicalType convertedDataType =
                    AvroSchemaConverter.convertToDataType(schemaString).getLogicalType();

            if (!convertedDataType.equals(rowType)) {
                throw new IllegalArgumentException(
                        format(
                                "Schema provided for '%s' format must be a nullable record type with fields 'before', 'after', 'op'"
                                        + " and schema of fields 'before' and 'after' must match the table schema: %s",
                                IDENTIFIER, schemaString));
            }
        }
    }
}
