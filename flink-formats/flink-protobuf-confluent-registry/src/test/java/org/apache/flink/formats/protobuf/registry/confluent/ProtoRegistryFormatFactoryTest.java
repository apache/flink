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

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertThrows;

/**
 * Tests for {@link
 * org.apache.flink.formats.protobuf.registry.confluent.ProtoRegistryFormatFactory}.
 */
public class ProtoRegistryFormatFactoryTest {
    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.BIGINT()),
                    Column.physical("name", DataTypes.STRING()),
                    Column.physical("salary", DataTypes.INT()));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SUBJECT = "test-subject";
    private static final String REGISTRY_URL = "http://localhost:8081";
    private static final String SCHEMA_STRING =
            "syntax = \"proto3\";\n"
                    + "import \"google/protobuf/timestamp.proto\";\n"
                    + "package io.confluent.test;\n"
                    + "message Employee {\n"
                    + "  int64 id=1;\n"
                    + "  string name=2;\n"
                    + "  int32 salary=3;\n"
                    + "}\n";

    private static final Map<String, String> EXPECTED_OPTIONAL_PROPERTIES = new HashMap<>();

    private static SchemaRegistryClient client;
    private static int schemaId;

    static {
        EXPECTED_OPTIONAL_PROPERTIES.put(
                "schema.registry.ssl.keystore.location", getAbsolutePath("/test-keystore.jks"));
        EXPECTED_OPTIONAL_PROPERTIES.put("schema.registry.ssl.keystore.password", "123456");
        EXPECTED_OPTIONAL_PROPERTIES.put(
                "schema.registry.ssl.truststore.location", getAbsolutePath("/test-keystore.jks"));
        EXPECTED_OPTIONAL_PROPERTIES.put("schema.registry.ssl.truststore.password", "123456");
        EXPECTED_OPTIONAL_PROPERTIES.put("basic.auth.credentials.source", "USER_INFO");
        EXPECTED_OPTIONAL_PROPERTIES.put("basic.auth.user.info", "user:pwd");
        EXPECTED_OPTIONAL_PROPERTIES.put("bearer.auth.token", "CUSTOM");
    }

    @BeforeAll
    static void beforeClass() {

        client = new MockSchemaRegistryClient();
        try {
            schemaId = client.register(SUBJECT, new ProtobufSchema(SCHEMA_STRING));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getAbsolutePath(String path) {
        try {
            return SchemaCoderTest.class.getResource(path).toURI().getPath();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void testDeserializationSchemaInitializationWithDefaultCoder() {
        SchemaCoder coder = new SchemaCoderProviders.DefaultSchemaCoder(SUBJECT, ROW_TYPE, client);
        final ProtoRegistryDeserializationSchema expectedDeser =
                new ProtoRegistryDeserializationSchema(
                        coder, ROW_TYPE, InternalTypeInfo.of(ROW_TYPE));

        final DynamicTableSource actualSource = createTableSource(SCHEMA, getDefaultConfigs());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testDeserializationSchemaInitializationWithPreRegisteredSchema() {
        SchemaCoder coder =
                new SchemaCoderProviders.PreRegisteredSchemaCoder(schemaId, null, client);
        final ProtoRegistryDeserializationSchema expectedDeser =
                new ProtoRegistryDeserializationSchema(
                        coder, ROW_TYPE, InternalTypeInfo.of(ROW_TYPE));

        final DynamicTableSource actualSource =
                createTableSource(SCHEMA, getConfigsWithExplicitSchemaId());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testDeserializationSchemaWithOptionalProperties() {
        SchemaCoder coder = new SchemaCoderProviders.DefaultSchemaCoder(SUBJECT, ROW_TYPE, client);
        final ProtoRegistryDeserializationSchema expectedDeser =
                new ProtoRegistryDeserializationSchema(
                        coder, ROW_TYPE, InternalTypeInfo.of(ROW_TYPE));
        final DynamicTableSource actualSource = createTableSource(SCHEMA, getOptionalProperties());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testDeserializationSchemaWithInvalidSchemaIdShouldThrow() {
        SchemaCoder coder =
                new SchemaCoderProviders.PreRegisteredSchemaCoder(schemaId, null, client);
        final ProtoRegistryDeserializationSchema expectedDeser =
                new ProtoRegistryDeserializationSchema(
                        coder, ROW_TYPE, InternalTypeInfo.of(ROW_TYPE));

        Map<String, String> options = getConfigsWithExplicitSchemaId();
        // Overwrite invalid schema Id
        options.put("proto-confluent.schema-id", "invalidId");
        assertThrows(ValidationException.class, () -> createTableSource(SCHEMA, options));
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getDefaultConfigs();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> commonOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");
        options.put("format", ProtoRegistryFormatFactory.IDENTIFIER);
        return options;
    }

    private Map<String, String> getDefaultConfigs() {
        final Map<String, String> options = new HashMap<>();
        options.putAll(commonOptions());
        options.put("proto-confluent.subject", SUBJECT);
        options.put("proto-confluent.url", REGISTRY_URL);
        return options;
    }

    @Test
    void testMissingSubjectForSinkWithNoExplicitSchemaShouldThrow() {
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.remove("proto-confluent.subject"));

        assertThatThrownBy(() -> createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Option proto-confluent.subject is required for serialization"));
    }

    private Map<String, String> getConfigsWithExplicitSchemaId() {
        final Map<String, String> options = new HashMap<>();
        options.putAll(commonOptions());
        options.put("proto-confluent.schema-id", Integer.toString(schemaId));
        options.put("proto-confluent.url", REGISTRY_URL);
        return options;
    }

    private Map<String, String> getOptionalProperties() {
        final Map<String, String> properties = new HashMap<>();
        // defined via Flink maintained options
        properties.put(
                ProtoRegistryFormatOptions.SSL_KEYSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        properties.put(ProtoRegistryFormatOptions.SSL_KEYSTORE_PASSWORD.key(), "123456");
        properties.put(
                ProtoRegistryFormatOptions.SSL_TRUSTSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        properties.put(ProtoRegistryFormatOptions.SSL_TRUSTSTORE_PASSWORD.key(), "123456");
        properties.put(ProtoRegistryFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE.key(), "USER_INFO");
        properties.put(ProtoRegistryFormatOptions.BASIC_AUTH_USER_INFO.key(), "user:pwd");
        // defined via general property map
        properties.put("properties.bearer.auth.token", "CUSTOM");
        return getModifiedOptions(
                opts ->
                        properties.forEach(
                                (k, v) ->
                                        opts.put(
                                                String.format(
                                                        "%s.%s",
                                                        ProtoRegistryFormatFactory.IDENTIFIER, k),
                                                v)));
    }
}
