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

package org.apache.flink.protobuf.registry.confluent;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.protobuf.registry.confluent.dynamic.deserializer.ProtoRegistryDynamicDeserializationSchema;
import org.apache.flink.protobuf.registry.confluent.dynamic.serializer.ProtoRegistryDynamicSerializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.BASIC_AUTH_USER_INFO;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.BEARER_AUTH_TOKEN;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.CUSTOM_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.MESSAGE_NAME;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.PACKAGE_NAME;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.PROPERTIES;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.READ_DEFAULT_VALUES;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.REGISTRY_CLIENT_CACHE_CAPACITY;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.SUBJECT;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.URL;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.USE_DEFAULT_PROTO_INCLUDES;
import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.WRITE_NULL_STRING_LITERAL;

public class ProtobufConfluentFormatFactoryUtils {

    public static ProtoRegistryDynamicDeserializationSchema createDynamicDeserializationSchema(
            DynamicTableSource.Context context,
            DataType producedDataType,
            int[][] projections,
            SchemaRegistryClientProvider schemaRegistryClientProvider,
            String schemaRegistryURL,
            ReadableConfig formatOptions) {
        producedDataType = Projection.of(projections).project(producedDataType);
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
                context.createTypeInformation(producedDataType);
        return new ProtoRegistryDynamicDeserializationSchema(
                schemaRegistryClientProvider,
                schemaRegistryURL,
                rowType,
                rowDataTypeInfo,
                formatOptions.get(IGNORE_PARSE_ERRORS),
                formatOptions.get(READ_DEFAULT_VALUES),
                formatOptions.get(USE_DEFAULT_PROTO_INCLUDES),
                parseCustomProtoIncludes(formatOptions.get(CUSTOM_PROTO_INCLUDES)));
    }

    public static ProtoRegistryDynamicSerializationSchema createDynamicSerializationSchema(
            DataType consumedDataType,
            SchemaRegistryClientProvider schemaRegistryClientProvider,
            String schemaRegistryURL,
            ReadableConfig formatOptions) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new ProtoRegistryDynamicSerializationSchema(
                formatOptions.get(PACKAGE_NAME),
                formatOptions.get(MESSAGE_NAME),
                rowType,
                formatOptions.get(SUBJECT),
                schemaRegistryClientProvider,
                schemaRegistryURL,
                formatOptions.get(USE_DEFAULT_PROTO_INCLUDES),
                parseCustomProtoIncludes(formatOptions.get(CUSTOM_PROTO_INCLUDES)));
    }

    public static Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        return options;
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REGISTRY_CLIENT_CACHE_CAPACITY);
        options.add(USE_DEFAULT_PROTO_INCLUDES);
        options.add(CUSTOM_PROTO_INCLUDES);
        options.add(SUBJECT);
        options.add(MESSAGE_NAME);
        options.add(PACKAGE_NAME);
        options.add(WRITE_NULL_STRING_LITERAL);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(READ_DEFAULT_VALUES);
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

    public static Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        URL,
                        REGISTRY_CLIENT_CACHE_CAPACITY,
                        USE_DEFAULT_PROTO_INCLUDES,
                        CUSTOM_PROTO_INCLUDES,
                        SUBJECT,
                        MESSAGE_NAME,
                        PACKAGE_NAME,
                        WRITE_NULL_STRING_LITERAL,
                        IGNORE_PARSE_ERRORS,
                        READ_DEFAULT_VALUES,
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

    public static void validateDynamicEncodingOptions(
            ReadableConfig formatOptions, String identifier) {
        List<ConfigOption> requiredOptions = new ArrayList<>();
        requiredOptions.add(SUBJECT);
        requiredOptions.add(MESSAGE_NAME);
        requiredOptions.add(PACKAGE_NAME);

        for (ConfigOption option : requiredOptions) {
            if (!formatOptions.getOptional(option).isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s.%s' is required for serialization",
                                identifier, option.key()));
            }
        }
    }

    public static SchemaRegistryClientProviders.CachedSchemaRegistryClientProvider
            createCachedSchemaRegistryClientProvider(ReadableConfig formatOptions) {
        String schemaRegistryURL = formatOptions.get(URL);
        List<SchemaProvider> providers = new ArrayList<>();
        providers.add(new ProtobufSchemaProvider());
        return new SchemaRegistryClientProviders.CachedSchemaRegistryClientProvider(
                schemaRegistryURL,
                formatOptions.get(REGISTRY_CLIENT_CACHE_CAPACITY),
                buildOptionalPropertiesMap(formatOptions));
    }

    static List<String> parseCustomProtoIncludes(String customProtoIncludes) {
        if (customProtoIncludes == null || customProtoIncludes.isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.asList(customProtoIncludes.split(","));
    }

    private static @Nullable Map<String, String> buildOptionalPropertiesMap(
            ReadableConfig formatOptions) {
        final Map<String, String> properties = new HashMap<>();

        formatOptions.getOptional(PROPERTIES).ifPresent(properties::putAll);

        formatOptions
                .getOptional(SSL_KEYSTORE_LOCATION)
                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.location", v));
        formatOptions
                .getOptional(SSL_KEYSTORE_PASSWORD)
                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.password", v));
        formatOptions
                .getOptional(SSL_TRUSTSTORE_LOCATION)
                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.location", v));
        formatOptions
                .getOptional(SSL_TRUSTSTORE_PASSWORD)
                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.password", v));
        formatOptions
                .getOptional(BASIC_AUTH_CREDENTIALS_SOURCE)
                .ifPresent(v -> properties.put("basic.auth.credentials.source", v));
        formatOptions
                .getOptional(BASIC_AUTH_USER_INFO)
                .ifPresent(v -> properties.put("basic.auth.user.info", v));
        formatOptions
                .getOptional(BEARER_AUTH_CREDENTIALS_SOURCE)
                .ifPresent(v -> properties.put("bearer.auth.credentials.source", v));
        formatOptions
                .getOptional(BEARER_AUTH_TOKEN)
                .ifPresent(v -> properties.put("bearer.auth.token", v));

        if (properties.isEmpty()) {
            return null;
        }
        return properties;
    }
}
