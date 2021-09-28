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

 package org.apache.flink.formats.avro.glue.schema.registry;

import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.AUTO_REGISTRATION;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.AWS_REGION;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.CACHE_SIZE;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.CACHE_TTL_MS;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.COMPATIBILITY;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.COMPRESSION_TYPE;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.ENDPOINT;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.RECORD_TYPE;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.REGISTRY_NAME;
import static org.apache.flink.formats.avro.glue.schema.registry.AvroGlueFormatOptions.SCHEMA_REGISTRY_SUBJECT;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

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
import org.apache.flink.table.types.logical.RowType;

/**
 * Table format factory for providing configured instances of AWS Glue Schema
 * Registry Avro to RowData {@link SerializationSchema} and
 * {@link DeserializationSchema}.
 */
@Internal
public class GlueSchemaRegistryAvroFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final String IDENTIFIER = "avro-glue";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final Map<String, Object> configMap = buildConfigMap(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context,
                    DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(producedDataType);
                return new AvroRowDataDeserializationSchema(
                        GlueSchemaRegistryAvroDeserializationSchema
                                .forGeneric(AvroSchemaConverter.convertToSchema(rowType), configMap),
                        AvroToRowDataConverters.createRowConverter(rowType), rowDataTypeInfo);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.all();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context,
                    DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new AvroRowDataSerializationSchema(rowType,
                        GlueSchemaRegistryAvroSerializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(rowType),
                                formatOptions.get(SCHEMA_REGISTRY_SUBJECT), buildConfigMap(formatOptions)),
                        RowDataToAvroConverters.createConverter(rowType));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.all();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    private Map<String, Object> buildConfigMap(ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<String, Object>();
        formatOptions.getOptional(AWS_REGION).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.AWS_REGION, v));
        formatOptions.getOptional(REGISTRY_NAME).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, v));
        formatOptions.getOptional(RECORD_TYPE).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, v));
        formatOptions.getOptional(COMPRESSION_TYPE).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, v));
        formatOptions.getOptional(ENDPOINT).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, v));
        formatOptions.getOptional(COMPATIBILITY).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, v));
        formatOptions.getOptional(AUTO_REGISTRATION).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, v));
        formatOptions.getOptional(CACHE_SIZE).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.CACHE_SIZE, v));
        formatOptions.getOptional(CACHE_TTL_MS).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, v));
        formatOptions.getOptional(CACHE_TTL_MS).ifPresent(v -> properties.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, v));
        return properties;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(REGISTRY_NAME);
        result.add(AWS_REGION);
        result.add(SCHEMA_REGISTRY_SUBJECT);
        return result;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(COMPRESSION_TYPE);
        result.add(ENDPOINT);
        result.add(RECORD_TYPE);
        result.add(COMPATIBILITY);
        result.add(AUTO_REGISTRATION);
        result.add(CACHE_SIZE);
        result.add(CACHE_TTL_MS);
        return result;
    }
}
