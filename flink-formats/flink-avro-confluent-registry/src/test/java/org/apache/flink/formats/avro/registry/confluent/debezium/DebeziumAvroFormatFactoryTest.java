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

package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link DebeziumAvroFormatFactory}. */
class DebeziumAvroFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    private static final String RECORD_SCHEMA =
            "    {\n"
                    + "        \"type\": \"record\",\n"
                    + "        \"name\": \"test\",\n"
                    + "        \"fields\": [\n"
                    + "            {\n"
                    + "                \"name\": \"before\",\n"
                    + "                \"type\": [\n"
                    + "                    \"null\",\n"
                    + "                    {\n"
                    + "                        \"type\": \"record\",\n"
                    + "                        \"name\": \"testSchema\",\n"
                    + "                        \"fields\": [\n"
                    + "                            {\n"
                    + "                                \"name\": \"a\",\n"
                    + "                                \"type\": [\n"
                    + "                                    \"null\",\n"
                    + "                                    \"string\"\n"
                    + "                                ],\n"
                    + "                                \"default\": null\n"
                    + "                            },\n"
                    + "                            {\n"
                    + "                                \"name\": \"b\",\n"
                    + "                                \"type\": [\n"
                    + "                                    \"null\",\n"
                    + "                                    \"int\"\n"
                    + "                                ],\n"
                    + "                                \"default\": null\n"
                    + "                            },\n"
                    + "                            {\n"
                    + "                                \"name\": \"c\",\n"
                    + "                                \"type\": [\n"
                    + "                                    \"null\",\n"
                    + "                                    \"boolean\"\n"
                    + "                                ],\n"
                    + "                                \"default\": null\n"
                    + "                            }\n"
                    + "                        ]\n"
                    + "                    }\n"
                    + "                ],\n"
                    + "                \"default\": null\n"
                    + "            },\n"
                    + "            {\n"
                    + "                \"name\": \"after\",\n"
                    + "                \"type\": [\n"
                    + "                    \"null\",\n"
                    + "                    \"testSchema\"\n"
                    + "                ],\n"
                    + "                \"default\": null\n"
                    + "            },\n"
                    + "            {\n"
                    + "                \"name\": \"op\",\n"
                    + "                \"type\": [\n"
                    + "                    \"null\",\n"
                    + "                    \"string\"\n"
                    + "                ],\n"
                    + "                \"default\": null\n"
                    + "            }\n"
                    + "        ]\n"
                    + "    }\n";
    private static final String AVRO_SCHEMA = "[\n\"null\",\n" + RECORD_SCHEMA + "]";

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SUBJECT = "test-debezium-avro";
    private static final String REGISTRY_URL = "http://localhost:8081";
    private static final String TOKEN_ENDPOINT_URL = "http://localhost:8080/token";

    @Test
    void testSeDeSchema() {
        final Map<String, String> options = getAllOptions();
        final Map<String, String> registryConfigs = getRegistryConfigs();

        DebeziumAvroDeserializationSchema expectedDeser =
                new DebeziumAvroDeserializationSchema(
                        ROW_TYPE,
                        InternalTypeInfo.of(ROW_TYPE),
                        REGISTRY_URL,
                        null,
                        registryConfigs);
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeser, actualDeser);

        DebeziumAvroSerializationSchema expectedSer =
                new DebeziumAvroSerializationSchema(
                        ROW_TYPE, REGISTRY_URL, SUBJECT, null, registryConfigs);
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testSeDeSchemaWithSchemaOption() {
        final Map<String, String> options = getAllOptions();
        options.put("debezium-avro-confluent.schema", AVRO_SCHEMA);

        final Map<String, String> registryConfigs = getRegistryConfigs();

        DebeziumAvroDeserializationSchema expectedDeser =
                new DebeziumAvroDeserializationSchema(
                        ROW_TYPE,
                        InternalTypeInfo.of(ROW_TYPE),
                        REGISTRY_URL,
                        AVRO_SCHEMA,
                        registryConfigs);
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertThat(actualDeser).isEqualTo(expectedDeser);

        DebeziumAvroSerializationSchema expectedSer =
                new DebeziumAvroSerializationSchema(
                        ROW_TYPE, REGISTRY_URL, SUBJECT, AVRO_SCHEMA, registryConfigs);
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertThat(actualSer).isEqualTo(expectedSer);
    }

    @Test
    public void testSeDeSchemaWithInvalidSchemaOption() {
        final Map<String, String> options = getAllOptions();
        options.put("debezium-avro-confluent.schema", RECORD_SCHEMA);

        assertThrows(IllegalArgumentException.class, () -> createDeserializationSchema(options));
        assertThrows(IllegalArgumentException.class, () -> createSerializationSchema(options));

        String basicSchema = "[ \"null\", \"string\" ]";
        options.put("debezium-avro-confluent.schema", basicSchema);
        assertThrows(IllegalArgumentException.class, () -> createDeserializationSchema(options));
        assertThrows(IllegalArgumentException.class, () -> createSerializationSchema(options));

        String invalidSchema =
                "[\"null\", "
                        + "{ \"type\" : \"record\","
                        + "\"name\" : \"debezium\","
                        + "\"fields\": [{\n"
                        + "        \"default\": null,\n"
                        + "        \"name\": \"op\",\n"
                        + "        \"type\": [\n"
                        + "          \"null\",\n"
                        + "          \"string\"\n"
                        + "        ]\n"
                        + "      }]"
                        + "}]";
        options.put("debezium-avro-confluent.schema", invalidSchema);
        assertThrows(IllegalArgumentException.class, () -> createDeserializationSchema(options));
        assertThrows(IllegalArgumentException.class, () -> createSerializationSchema(options));

        String invalidRecordSchema = AVRO_SCHEMA.replace("int", "string");
        options.put("debezium-avro-confluent.schema", invalidRecordSchema);
        assertThrows(IllegalArgumentException.class, () -> createDeserializationSchema(options));
        assertThrows(IllegalArgumentException.class, () -> createSerializationSchema(options));
    }

    @Nonnull
    private Map<String, String> getRegistryConfigs() {
        final Map<String, String> registryConfigs = new HashMap<>();
        registryConfigs.put("basic.auth.user.info", "something1");
        registryConfigs.put("basic.auth.credentials.source", "something2");
        return registryConfigs;
    }

    @Nonnull
    private Map<String, String> getExtendedRegistryConfigs() {
        final Map<String, String> registryConfigs = new HashMap<>();
        registryConfigs.put("basic.auth.user.info", "something1");
        registryConfigs.put("basic.auth.credentials.source", "something2");
        registryConfigs.put("bearer.auth.credentials.source", "CUSTOM");
        registryConfigs.put("bearer.auth.token", "CUSTOM_TOKEN");
        registryConfigs.put("sasl.oauthbearer.token.endpoint.url", TOKEN_ENDPOINT_URL);
        registryConfigs.put("sasl.jaas.config", "custom.jaas.config");
        registryConfigs.put("bearer.auth.logical.cluster", "test-cluster");
        registryConfigs.put("schema.registry.ssl.keystore.location", "dummy-keystore-path");
        registryConfigs.put("schema.registry.ssl.keystore.password", "dummy-password");
        registryConfigs.put("schema.registry.ssl.truststore.location", "dummy-truststore-path");
        registryConfigs.put("schema.registry.ssl.truststore.password", "dummy-password");
        return registryConfigs;
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", DebeziumAvroFormatFactory.IDENTIFIER);
        options.put("debezium-avro-confluent.url", REGISTRY_URL);
        options.put("debezium-avro-confluent.subject", SUBJECT);
        options.put("debezium-avro-confluent.basic-auth.user-info", "something1");
        options.put("debezium-avro-confluent.basic-auth.credentials-source", "something2");
        return options;
    }

    private Map<String, String> getExtendedOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", DebeziumAvroFormatFactory.IDENTIFIER);
        options.put("debezium-avro-confluent.url", REGISTRY_URL);
        options.put("debezium-avro-confluent.subject", SUBJECT);
        options.put("debezium-avro-confluent.basic-auth.user-info", "something1");
        options.put("debezium-avro-confluent.basic-auth.credentials-source", "something2");
        options.put("debezium-avro-confluent.bearer-auth.credentials-source", "CUSTOM");
        options.put("debezium-avro-confluent.bearer-auth.token", "CUSTOM_TOKEN");
        options.put("debezium-avro-confluent.bearer-auth.token.endpoint.url", TOKEN_ENDPOINT_URL);
        options.put("debezium-avro-confluent.bearer-auth.jaas.config", "custom.jaas.config");
        options.put("debezium-avro-confluent.bearer-auth.logical.cluster", "test-cluster");
        // Use dummy SSL paths to avoid Windows path issues
        options.put("debezium-avro-confluent.ssl.keystore.location", "dummy-keystore-path");
        options.put("debezium-avro-confluent.ssl.keystore.password", "dummy-password");
        options.put("debezium-avro-confluent.ssl.truststore.location", "dummy-truststore-path");
        options.put("debezium-avro-confluent.ssl.truststore.password", "dummy-password");
        return options;
    }

    @Test
    void testSeDeSchemaWithExtendedProperties() {
        final Map<String, String> options = getExtendedOptions();
        final Map<String, String> registryConfigs = getExtendedRegistryConfigs();

        DebeziumAvroDeserializationSchema expectedDeser =
                new DebeziumAvroDeserializationSchema(
                        ROW_TYPE,
                        InternalTypeInfo.of(ROW_TYPE),
                        REGISTRY_URL,
                        null,
                        registryConfigs);
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeser, actualDeser);

        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        return sinkMock.valueFormat.createRuntimeEncoder(
                new SinkRuntimeProviderContext(false), SCHEMA.toPhysicalRowDataType());
    }
}
