/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.apicurio.registry.serde.SerdeConfig;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BEARER_AUTH_TOKEN;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.ENABLE_HEADERS;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.HEADERS_HANDLER;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.ID_HANDLER;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.PROPERTIES;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SCHEMA;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SUBJECT;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.URL;

/** Apicurio avro format. */
@Internal
public class ApicurioRegistryAvroFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "avro-apicurio";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(URL);
        Optional<String> schemaString = formatOptions.getOptional(SCHEMA);
        Map<String, ?> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);
        return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
            // --------------------------------------------------------------------------------------------
            // Mutable attributes
            // --------------------------------------------------------------------------------------------

            private List<String> metadataKeys;

            // --------------------------------------------------------------------------------------------
            // Apicurio specific attributes
            // --------------------------------------------------------------------------------------------

            private final @Nullable String globalIdFromHeader = null;

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType,
                    int[][] projections) {
                producedDataType = Projection.of(projections).project(producedDataType);
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                // schema has been supplied
                final Schema schema =
                        schemaString
                                .map(s -> getAvroSchema(s, rowType))
                                .orElse(AvroSchemaConverter.convertToSchema(rowType));
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new AvroRowDataDeserializationSchema(
                        ApicurioRegistryAvroDeserializationSchema.forGeneric(
                                schema, schemaRegistryURL, optionalPropertiesMap),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        rowDataTypeInfo);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
            // --------------------------------------------------------------------------------------------
            // Metadata handling
            // --------------------------------------------------------------------------------------------

            @Override
            public Map<String, DataType> listReadableMetadata() {
                final Map<String, DataType> metadataMap = new LinkedHashMap<>();
                Stream.of(ReadableMetadata.values())
                        .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
                return metadataMap;
            }

            @Override
            public void applyReadableMetadata(List<String> metadataKeys) {
                this.metadataKeys = metadataKeys;
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(URL);
        Optional<String> subject = formatOptions.getOptional(SUBJECT);
        Optional<String> schemaString = formatOptions.getOptional(SCHEMA);
        Map<String, ?> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

        if (!subject.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option %s.%s is required for serialization",
                            IDENTIFIER, SUBJECT.key()));
        }

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                final Schema schema =
                        schemaString
                                .map(s -> getAvroSchema(s, rowType))
                                .orElse(AvroSchemaConverter.convertToSchema(rowType));
                return new AvroRowDataSerializationSchema(
                        rowType,
                        ApicurioRegistryAvroSerializationSchema.forGeneric(
                                subject.get(), schema, schemaRegistryURL, optionalPropertiesMap),
                        RowDataToAvroConverters.createConverter(rowType));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
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
        options.add(ENABLE_HEADERS);
        options.add(HEADERS_HANDLER);
        options.add(ID_HANDLER);
        options.add(ENABLE_CONFLUENT_ID_HANDLER);
        options.add(SCHEMA);
        options.add(PROPERTIES);
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

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        URL,
                        ENABLE_HEADERS,
                        HEADERS_HANDLER,
                        ID_HANDLER,
                        ENABLE_CONFLUENT_ID_HANDLER,
                        SUBJECT,
                        SCHEMA,
                        PROPERTIES,
                        SSL_KEYSTORE_LOCATION,
                        SSL_KEYSTORE_PASSWORD,
                        SSL_TRUSTSTORE_LOCATION,
                        SSL_TRUSTSTORE_PASSWORD,
                        BASIC_AUTH_CREDENTIALS_SOURCE,
                        BASIC_AUTH_USER_INFO,
                        BEARER_AUTH_CREDENTIALS_SOURCE,
                        BEARER_AUTH_TOKEN)
                .collect(Collectors.toSet());
    }

    public static @Nullable Map<String, Object> buildOptionalPropertiesMap(
            ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<>();

        formatOptions
                .getOptional(AvroApicurioFormatOptions.PROPERTIES)
                .ifPresent(properties::putAll);
        formatOptions
                .getOptional(AvroApicurioFormatOptions.ENABLE_HEADERS)
                .ifPresent(v -> properties.put(SerdeConfig.ENABLE_HEADERS, v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.HEADERS_HANDLER)
                .ifPresent(v -> properties.put(SerdeConfig.HEADERS_HANDLER, v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.ID_HANDLER)
                .ifPresent(v -> properties.put(SerdeConfig.ID_HANDLER, v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER)
                .ifPresent(v -> properties.put(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER, v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION)
                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.location", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD)
                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.password", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION)
                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.location", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD)
                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.password", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE)
                .ifPresent(v -> properties.put("basic.auth.credentials.source", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO)
                .ifPresent(v -> properties.put("basic.auth.user.info", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE)
                .ifPresent(v -> properties.put("bearer.auth.credentials.source", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BEARER_AUTH_TOKEN)
                .ifPresent(v -> properties.put("bearer.auth.token", v));
        // null pointers later if left as null TODO assess what behaviour we want for this.
        //        if (properties.isEmpty()) {
        //            return null;
        //        }
        return properties;
    }

    private static Schema getAvroSchema(String schemaString, RowType rowType) {
        LogicalType convertedDataType =
                AvroSchemaConverter.convertToDataType(schemaString).getLogicalType();

        if (convertedDataType.isNullable()) {
            convertedDataType = convertedDataType.copy(false);
        }

        if (!convertedDataType.equals(rowType)) {
            throw new IllegalArgumentException(
                    format(
                            "Schema provided for '%s' format does not match the table schema: %s",
                            IDENTIFIER, schemaString));
        }

        return new Parser().parse(schemaString);
    }
    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    /** List of metadata that can be read with this format. */
    enum ReadableMetadata {
        ENABLE_HEADERS(
                SerdeConfig.ENABLE_HEADERS,
                DataTypes.BOOLEAN(),
                DataTypes.FIELD(SerdeConfig.ENABLE_HEADERS, DataTypes.BOOLEAN()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getBoolean(pos);
                    }
                }),
        HEADERS_HANDLER(
                SerdeConfig.HEADERS_HANDLER,
                DataTypes.STRING().nullable(),
                DataTypes.FIELD(SerdeConfig.HEADERS_HANDLER, DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        ID_HANDLER(
                SerdeConfig.ID_HANDLER,
                DataTypes.STRING().nullable(),
                DataTypes.FIELD(SerdeConfig.HEADERS_HANDLER, DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        ENABLE_CONFLUENT_ID_HANDLER(
                SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER,
                DataTypes.BOOLEAN(),
                DataTypes.FIELD(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER, DataTypes.BOOLEAN()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getBoolean(pos);
                    }
                });

        final String key;

        final DataType dataType;

        final DataTypes.Field requiredJsonField;

        final MetadataConverter converter;

        ReadableMetadata(
                String key,
                DataType dataType,
                DataTypes.Field requiredJsonField,
                MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.requiredJsonField = requiredJsonField;
            this.converter = converter;
        }
    }
    // --------------------------------------------------------------------------------------------

    /**
     * Converter that extracts a metadata field from the row that comes out of the JSON schema and
     * converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {

        // Method for top-level access.
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }
}
