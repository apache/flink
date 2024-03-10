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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Shared across formats factory class for creating a {@link SchemaRegistryConfig}. */
public class RegistryClientConfigFactory {

    /** Creates a {@link SchemaRegistryConfig} from the given format options. */
    public static SchemaRegistryConfig get(ReadableConfig formatOptions) {
        final String schemaRegistryURL = formatOptions.get(RegistryFormatOptions.URL);
        final int schemaId = formatOptions.get(RegistryFormatOptions.SCHEMA_ID);
        final int cacheSize = formatOptions.get(RegistryFormatOptions.SCHEMA_CACHE_SIZE);
        final Map<String, Object> schemaClientProperties =
                new HashMap<>();
        formatOptions.getOptional(RegistryFormatOptions.PROPERTIES)
                .ifPresent(schemaClientProperties::putAll);

        return new DefaultSchemaRegistryConfig(
                schemaRegistryURL, cacheSize, schemaId, schemaClientProperties);
    }

    /** Should be used in {@link FormatFactory#requiredOptions()}. */
    public static Set<ConfigOption<?>> getRequiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RegistryFormatOptions.URL);
        options.add(RegistryFormatOptions.SCHEMA_ID);
        return options;
    }

    /** Should be used in {@link FormatFactory#optionalOptions()}. */
    public static Set<ConfigOption<?>> getOptionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RegistryFormatOptions.SCHEMA_CACHE_SIZE);
        options.add(RegistryFormatOptions.BASIC_AUTH_USER_INFO);
        return options;
    }

    /** Should be used in {@link FormatFactory#forwardOptions()}. */
    public static Set<ConfigOption<?>> getForwardOptions() {
        return Stream.of(
                        RegistryFormatOptions.URL,
                        RegistryFormatOptions.SCHEMA_ID,
                        RegistryFormatOptions.SCHEMA_CACHE_SIZE,
                        RegistryFormatOptions.BASIC_AUTH_USER_INFO)
                .collect(Collectors.toSet());
    }

    /** Default implementation of {@link SchemaRegistryConfig}. */
    private static final class DefaultSchemaRegistryConfig implements SchemaRegistryConfig {

        private final String schemaRegistryUrl;
        private final int identityMapCapacity;
        private final int schemaId;
        private final Map<String, Object> properties;

        DefaultSchemaRegistryConfig(
                String schemaRegistryUrl,
                int identityMapCapacity,
                int schemaId,
                Map<String, Object> properties) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.identityMapCapacity = identityMapCapacity;
            this.schemaId = schemaId;
            this.properties = properties;
        }

        @Override
        public int getSchemaId() {
            return schemaId;
        }

        @Override
        public SchemaRegistryClient createClient() {
            return new CachedSchemaRegistryClient(
                    schemaRegistryUrl,
                    identityMapCapacity,
                    Arrays.asList(new ProtobufSchemaProvider()),
                    properties);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DefaultSchemaRegistryConfig that = (DefaultSchemaRegistryConfig) o;
            return identityMapCapacity == that.identityMapCapacity
                    && schemaId == that.schemaId
                    && Objects.equals(schemaRegistryUrl, that.schemaRegistryUrl);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaRegistryUrl, identityMapCapacity, schemaId);
        }
    }
}
