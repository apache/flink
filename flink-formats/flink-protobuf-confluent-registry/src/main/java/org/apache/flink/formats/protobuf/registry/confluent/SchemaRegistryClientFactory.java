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

import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.BASIC_AUTH_USER_INFO;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.BEARER_AUTH_TOKEN;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.PROPERTIES;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.formats.protobuf.registry.confluent.RegistryFormatOptions.SSL_TRUSTSTORE_PASSWORD;

/** Shared across formats factory class for creating a {@link org.apache.flink.formats.protobuf.registry.confluent.SchemaCoder}. */
public class SchemaRegistryClientFactory {

    private static final String ROW = "row";
    private static final String PACKAGE = "io.confluent.generated";

    public static SchemaCoder getCoder(RowType rowType, ReadableConfig formatOptions) {

        SchemaRegistryClient schemaRegistryClient = getClient(formatOptions);
        final Optional<Integer> schemaId =
                formatOptions.getOptional(RegistryFormatOptions.SCHEMA_ID);
        final Optional<String> messageName =
                formatOptions.getOptional(RegistryFormatOptions.MESSAGE_NAME);
        final Optional<String> subject = formatOptions.getOptional(RegistryFormatOptions.SUBJECT);
        return schemaId.map(
                        id ->
                                SchemaCoderProviders.createForPreRegisteredSchema(
                                        id, messageName.get(), schemaRegistryClient))
                .orElseGet(
                        () ->
                                SchemaCoderProviders.createDefault(
                                        subject.get(), rowType, schemaRegistryClient));
    }

    public static SchemaRegistryClient getClient(ReadableConfig formatOptions) {
        final String schemaRegistryURL = formatOptions.get(RegistryFormatOptions.URL);
        final int cacheSize = formatOptions.get(RegistryFormatOptions.SCHEMA_CACHE_SIZE);
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
        options.add(RegistryFormatOptions.URL);
        return options;
    }

    /** Should be used in {@link FormatFactory#optionalOptions()}. */
    public static Set<ConfigOption<?>> getOptionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RegistryFormatOptions.SCHEMA_ID);
        options.add(RegistryFormatOptions.MESSAGE_NAME);
        options.add(RegistryFormatOptions.SUBJECT);
        options.add(RegistryFormatOptions.PROPERTIES);

        options.add(RegistryFormatOptions.SCHEMA_CACHE_SIZE);
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
                        RegistryFormatOptions.URL,
                        RegistryFormatOptions.SCHEMA_ID,
                        RegistryFormatOptions.MESSAGE_NAME,
                        RegistryFormatOptions.SUBJECT,
                        RegistryFormatOptions.PROPERTIES,
                        RegistryFormatOptions.SCHEMA_CACHE_SIZE,
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
