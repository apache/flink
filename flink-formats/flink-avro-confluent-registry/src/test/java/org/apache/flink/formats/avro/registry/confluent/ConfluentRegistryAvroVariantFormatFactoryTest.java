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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroVariantDeserializationSchema;
import org.apache.flink.formats.avro.RegistryWriterAvroDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfluentRegistryAvroVariantFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(Column.physical("data", DataTypes.VARIANT()));

    private static final String REGISTRY_URL = "http://localhost:8081";
    private static final int SCHEMA_CACHE_CAPACITY = 1000;

    @Test
    void testDecodingFormat() {
        final AvroVariantDeserializationSchema expectedDeser =
                new AvroVariantDeserializationSchema(
                        new RegistryWriterAvroDeserializationSchema(
                                new CachedSchemaCoderProvider(
                                        null, REGISTRY_URL, SCHEMA_CACHE_CAPACITY, null)),
                        false,
                        SCHEMA_CACHE_CAPACITY,
                        InternalTypeInfo.of(RowType.of(new VariantType())));

        final DynamicTableSource actualSource = createTableSource(SCHEMA, getDefaultOptions());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testDecodingFormatWithOptionalProperties() {
        Map<String, String> expectedProperties = getExpectedProperties();

        final AvroVariantDeserializationSchema expectedDeser =
                new AvroVariantDeserializationSchema(
                        new RegistryWriterAvroDeserializationSchema(
                                new CachedSchemaCoderProvider(
                                        null,
                                        REGISTRY_URL,
                                        SCHEMA_CACHE_CAPACITY,
                                        expectedProperties)),
                        false,
                        SCHEMA_CACHE_CAPACITY,
                        InternalTypeInfo.of(RowType.of(new VariantType())));

        final DynamicTableSource actualSource =
                createTableSource(SCHEMA, getOptionsWithProperties());
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testMissingUrl() {
        final Map<String, String> options = getDefaultOptions();
        options.remove("avro-variant-confluent.url");

        assertThatThrownBy(() -> createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testDecodingFormatWithSchemaMetadata() {
        final AvroVariantDeserializationSchema expectedDeser =
                new AvroVariantDeserializationSchema(
                        new RegistryWriterAvroDeserializationSchema(
                                new CachedSchemaCoderProvider(
                                        null, REGISTRY_URL, SCHEMA_CACHE_CAPACITY, null)),
                        true,
                        SCHEMA_CACHE_CAPACITY,
                        InternalTypeInfo.of(
                                RowType.of(
                                        new VariantType(),
                                        new VarCharType(true, VarCharType.MAX_LENGTH))));

        final DynamicTableSource actualSource = createTableSource(SCHEMA, getDefaultOptions());
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        scanSourceMock.valueFormat.applyReadableMetadata(Collections.singletonList("schema"));

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    private Map<String, String> getDefaultOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", ConfluentRegistryAvroVariantFormatFactory.IDENTIFIER);
        options.put("avro-variant-confluent.url", REGISTRY_URL);
        return options;
    }

    private Map<String, String> getOptionsWithProperties() {
        final Map<String, String> options = getDefaultOptions();
        String prefix = "avro-variant-confluent.";
        options.put(prefix + "ssl.keystore.location", "/test-keystore.jks");
        options.put(prefix + "ssl.keystore.password", "123456");
        options.put(prefix + "ssl.truststore.location", "/test-keystore.jks");
        options.put(prefix + "ssl.truststore.password", "123456");
        options.put(prefix + "basic-auth.credentials-source", "USER_INFO");
        options.put(prefix + "basic-auth.user-info", "user:pwd");
        options.put(prefix + "properties.bearer.auth.token", "CUSTOM");
        return options;
    }

    private Map<String, String> getExpectedProperties() {
        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("schema.registry.ssl.keystore.location", "/test-keystore.jks");
        expectedProperties.put("schema.registry.ssl.keystore.password", "123456");
        expectedProperties.put("schema.registry.ssl.truststore.location", "/test-keystore.jks");
        expectedProperties.put("schema.registry.ssl.truststore.password", "123456");
        expectedProperties.put("basic.auth.credentials.source", "USER_INFO");
        expectedProperties.put("basic.auth.user.info", "user:pwd");
        expectedProperties.put("bearer.auth.token", "CUSTOM");
        return expectedProperties;
    }
}
