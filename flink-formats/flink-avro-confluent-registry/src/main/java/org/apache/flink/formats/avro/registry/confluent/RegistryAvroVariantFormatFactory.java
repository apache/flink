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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.AvroVariantDecodingFormat;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BASIC_AUTH_USER_INFO;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.BEARER_AUTH_TOKEN;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.PROPERTIES;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_KEYSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions.URL;

/**
 * Table format factory for providing configured instances of Confluent Schema Registry Avro to
 * Variant {@link DeserializationSchema}. Deserialization only — no serialization support.
 */
@Internal
public class RegistryAvroVariantFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "avro-variant-confluent";

    private static final int SCHEMA_CACHE_CAPACITY = 1000;

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(URL);
        Map<String, String> optionalPropertiesMap =
                RegistryAvroFormatFactory.buildOptionalPropertiesMap(formatOptions);

        return new AvroVariantDecodingFormat(
                new CachedSchemaCoderProvider(
                        null, schemaRegistryURL, SCHEMA_CACHE_CAPACITY, optionalPropertiesMap),
                SCHEMA_CACHE_CAPACITY);
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
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
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
}
