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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_PASSWORD;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_USERID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_CLIENT_ID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_CLIENT_SECRET;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_SCOPE;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_URL;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.PROPERTIES;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.REGISTERED_ARTIFACT_DESCRIPTION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.REGISTERED_ARTIFACT_ID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.REGISTERED_ARTIFACT_NAME;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.REGISTERED_ARTIFACT_VERSION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SCHEMA;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.URL;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_GLOBALID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_HEADERS;

/** Apicurio avro format. */
@Internal
public class ApicurioRegistryAvroFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    // used this key rather than apicurio-avro (the format name) to be consistent with Confluent.
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
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(URL);
        Optional<String> schemaString = formatOptions.getOptional(SCHEMA);
        Map<String, ?> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

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
                                schema, schemaRegistryURL, optionalPropertiesMap),
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
        return getOptionalOptions();
    }

    protected static Set<ConfigOption<?>> getOptionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USE_HEADERS);
        options.add(USE_GLOBALID);
        options.add(SCHEMA);
        options.add(REGISTERED_ARTIFACT_NAME);
        options.add(REGISTERED_ARTIFACT_DESCRIPTION);
        options.add(REGISTERED_ARTIFACT_ID);
        options.add(REGISTERED_ARTIFACT_VERSION);
        options.add(PROPERTIES);
        options.add(SSL_KEYSTORE_LOCATION);
        options.add(SSL_KEYSTORE_PASSWORD);
        options.add(SSL_TRUSTSTORE_LOCATION);
        options.add(SSL_TRUSTSTORE_PASSWORD);
        options.add(BASIC_AUTH_CREDENTIALS_USERID);
        options.add(BASIC_AUTH_CREDENTIALS_PASSWORD);
        options.add(OIDC_AUTH_URL);
        options.add(OIDC_AUTH_CLIENT_ID);
        options.add(OIDC_AUTH_CLIENT_SECRET);
        options.add(OIDC_AUTH_SCOPE);
        options.add(OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        URL,
                        USE_HEADERS,
                        USE_GLOBALID,
                        SCHEMA,
                        REGISTERED_ARTIFACT_NAME,
                        REGISTERED_ARTIFACT_DESCRIPTION,
                        REGISTERED_ARTIFACT_ID,
                        REGISTERED_ARTIFACT_VERSION,
                        PROPERTIES,
                        SSL_KEYSTORE_LOCATION,
                        SSL_KEYSTORE_PASSWORD,
                        SSL_TRUSTSTORE_LOCATION,
                        SSL_TRUSTSTORE_PASSWORD,
                        BASIC_AUTH_CREDENTIALS_USERID,
                        BASIC_AUTH_CREDENTIALS_PASSWORD,
                        OIDC_AUTH_URL,
                        OIDC_AUTH_CLIENT_ID,
                        OIDC_AUTH_CLIENT_SECRET,
                        OIDC_AUTH_SCOPE,
                        OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION)
                .collect(Collectors.toSet());
    }

    public static @Nullable Map<String, Object> buildOptionalPropertiesMap(
            ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<>();

        Set<ConfigOption<?>> configOptionsArray = getOptionalOptions();

        for (ConfigOption configOption : configOptionsArray) {
            formatOptions
                    .getOptional(configOption)
                    .ifPresent(v -> properties.put(configOption.key(), v));
            // when there is no value we would like to set the default as the value.
            // at java 11 we can use the orElse or isEmpty , but we are java 8
            String configOptionKey = configOption.key();
            if (properties.get(configOptionKey) == null && configOption.hasDefaultValue()) {
                properties.put(configOptionKey, configOption.defaultValue());
            }
        }
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
