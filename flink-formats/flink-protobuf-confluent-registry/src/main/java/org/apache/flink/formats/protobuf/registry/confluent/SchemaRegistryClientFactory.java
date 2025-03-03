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

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FormatFactory;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.BASIC_AUTH_USER_INFO;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.BEARER_AUTH_TOKEN;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.PROPERTIES;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatOptions.SSL_TRUSTSTORE_PASSWORD;

/**
 * Shared across formats factory class for creating a {@link
 * org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder}.
 */
public class SchemaRegistryClientFactory {

    public static SchemaCoder getCoder(RowType rowType, ReadableConfig formatOptions) {

        SchemaRegistryClient schemaRegistryClient = getClient(formatOptions);
        final Optional<Integer> schemaId =
                formatOptions.getOptional(ProtoRegistryFormatOptions.SCHEMA_ID);
        final Optional<String> messageName =
                formatOptions.getOptional(ProtoRegistryFormatOptions.MESSAGE_NAME);
        final Optional<String> subject =
                formatOptions.getOptional(ProtoRegistryFormatOptions.SUBJECT);
        return schemaId.map(
                        id ->
                                SchemaCoderProviders.createForPreRegisteredSchema(
                                        id, messageName.orElse(null), schemaRegistryClient))
                .orElseGet(
                        () ->
                                SchemaCoderProviders.createDefault(
                                        subject.orElse(null), rowType, schemaRegistryClient));
    }

    private static SchemaRegistryClient getClient(ReadableConfig formatOptions) {
        final String schemaRegistryURL = formatOptions.get(ProtoRegistryFormatOptions.URL);
        final int cacheSize = formatOptions.get(ProtoRegistryFormatOptions.SCHEMA_CACHE_SIZE);
        final Map<String, String> schemaClientProperties =
                buildOptionalPropertiesMap(formatOptions);
        return new CachedSchemaRegistryClient(schemaRegistryURL, cacheSize, schemaClientProperties);
    }

    public static @Nullable Map<String, String> buildOptionalPropertiesMap(
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

    /** Should be used in {@link FormatFactory#requiredOptions()}. */
    public static Set<ConfigOption<?>> getRequiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ProtoRegistryFormatOptions.URL);
        return options;
    }

    /** Should be used in {@link FormatFactory#optionalOptions()}. */
    public static Set<ConfigOption<?>> getOptionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ProtoRegistryFormatOptions.SCHEMA_ID);
        options.add(ProtoRegistryFormatOptions.MESSAGE_NAME);
        options.add(ProtoRegistryFormatOptions.SUBJECT);
        options.add(ProtoRegistryFormatOptions.PROPERTIES);

        options.add(ProtoRegistryFormatOptions.SCHEMA_CACHE_SIZE);
        options.add(SSL_KEYSTORE_LOCATION);
        options.add(SSL_KEYSTORE_PASSWORD);
        options.add(SSL_TRUSTSTORE_LOCATION);
        options.add(SSL_TRUSTSTORE_PASSWORD);
        options.add(BASIC_AUTH_CREDENTIALS_SOURCE);
        options.add(BASIC_AUTH_USER_INFO);
        options.add(BEARER_AUTH_CREDENTIALS_SOURCE);

        return options;
    }

    /** Should be used in {@link FormatFactory#forwardOptions()}. */
    public static Set<ConfigOption<?>> getForwardOptions() {
        return Stream.of(
                        ProtoRegistryFormatOptions.URL,
                        ProtoRegistryFormatOptions.SCHEMA_ID,
                        ProtoRegistryFormatOptions.MESSAGE_NAME,
                        ProtoRegistryFormatOptions.SUBJECT,
                        ProtoRegistryFormatOptions.PROPERTIES,
                        ProtoRegistryFormatOptions.SCHEMA_CACHE_SIZE,
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
}
