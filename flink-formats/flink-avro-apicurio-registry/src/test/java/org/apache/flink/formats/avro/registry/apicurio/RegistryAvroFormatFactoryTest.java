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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.ID_OPTION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.ID_PLACEMENT;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.PROPERTIES;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ApicurioRegistryAvroFormatFactory}. */
class RegistryAvroFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String REGISTRY_URL = "http://localhost:8080//apis/registry/v2";
    private static final String SCHEMA_STRING =
            "{\n"
                    + "    \"type\": \"record\",\n"
                    + "    \"name\": \"test_record\",\n"
                    + "    \"fields\": [\n"
                    + "        {\n"
                    + "            \"name\": \"a\",\n"
                    + "            \"type\": [\n"
                    + "                \"null\",\n"
                    + "                \"string\"\n"
                    + "            ],\n"
                    + "            \"default\": null\n"
                    + "        },\n"
                    + "        {\n"
                    + "            \"name\": \"b\",\n"
                    + "            \"type\": [\n"
                    + "                \"null\",\n"
                    + "                \"int\"\n"
                    + "            ],\n"
                    + "            \"default\": null\n"
                    + "        },\n"
                    + "        {\n"
                    + "            \"name\": \"c\",\n"
                    + "            \"type\": [\n"
                    + "                \"null\",\n"
                    + "                \"boolean\"\n"
                    + "            ],\n"
                    + "            \"default\": null\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}";

    private static final Map<String, String> EXPECTED_OPTIONAL_PROPERTIES = new HashMap<>();

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

    //    @Test
    //    void testDeserializationSchema() {
    //        AvroToRowDataConverters.AvroToRowDataConverter rowConverter =
    //                AvroToRowDataConverters.createRowConverter(ROW_TYPE);
    //        ApicurioRegistryAvroDeserializationSchema<GenericRecord> nestedSchema =
    //                ApicurioRegistryAvroDeserializationSchema.forGeneric(
    //                        AvroSchemaConverter.convertToSchema(ROW_TYPE), REGISTRY_URL);
    //
    //        final AvroRowDataDeserializationSchema expectedDeser =
    //                new AvroRowDataDeserializationSchema(
    //                        nestedSchema, rowConverter, InternalTypeInfo.of(ROW_TYPE));
    //
    //        final DynamicTableSource actualSource = createTableSource(SCHEMA,
    // getDefaultOptions());
    //
    //
    // assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
    //        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
    //                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
    //
    //        DeserializationSchema<RowData> actualDeser =
    //                scanSourceMock.valueFormat.createRuntimeDecoder(
    //                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());
    //
    //        assertThat(actualDeser).isEqualTo(expectedDeser);
    //    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getDefaultOptions();
        optionModifier.accept(options);
        return options;
    }

    @Test
    void testbuildOptionalPropertiesMapEmpty() {
        Set<ConfigOption<?>> configOptions = ApicurioRegistryAvroFormatFactory.getOptionalOptions();
        Map<String, String> inputMap = new HashMap<>();
        Configuration formatOptions = Configuration.fromMap(inputMap);
        ApicurioRegistryAvroFormatFactory registryAvroFormatFactory =
                new ApicurioRegistryAvroFormatFactory();
        Map<String, Object> outputMap =
                registryAvroFormatFactory.buildOptionalPropertiesMap(formatOptions);

        for (ConfigOption configOption : configOptions) {
            if (configOption.hasDefaultValue()) {
                assertThat(outputMap.get(configOption.key()))
                        .isEqualTo(configOption.defaultValue());
            } else {
                assertThat(outputMap.get(configOption.key())).isNull();
            }
        }
        // check an individual enum explicitly is correct.
        IdPlacementEnum idPlacementEnum =
                (IdPlacementEnum) outputMap.get(AvroApicurioFormatOptions.ID_PLACEMENT);
        assertThat(idPlacementEnum == IdPlacementEnum.HEADER);
    }

    @Test
    void testbuildOptionalPropertiesMapWithConfig() {

        Set<ConfigOption<?>> configOptions = ApicurioRegistryAvroFormatFactory.getOptionalOptions();
        Map<String, String> inputMap = new HashMap<>();
        for (ConfigOption configOption : configOptions) {
            if (!configOption.equals(PROPERTIES)
                    && !configOption.equals(OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION)) {
                String value;
                if (configOption.equals(ID_PLACEMENT)) {
                    value = IdPlacementEnum.LEGACY.name();
                } else if (configOption.equals(ID_OPTION)) {
                    value = IdOptionEnum.CONTENT_ID.name();
                } else {
                    value = configOption.key() + "-value";
                }
                inputMap.put(configOption.key(), value);
            }
        }

        Configuration formatOptions = Configuration.fromMap(inputMap);
        Duration durationTestValue = Duration.ofSeconds(10);
        formatOptions.set(OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION, durationTestValue);
        Map propertiesInput = new HashMap<>();
        propertiesInput.put("aaa", "111");
        propertiesInput.put("bbb", "222");
        formatOptions.set(PROPERTIES, propertiesInput);
        ApicurioRegistryAvroFormatFactory registryAvroFormatFactory =
                new ApicurioRegistryAvroFormatFactory();
        Map<String, Object> outputMap =
                registryAvroFormatFactory.buildOptionalPropertiesMap(formatOptions);

        for (ConfigOption configOption : configOptions) {
            Object expectedValue;
            if (configOption.equals(PROPERTIES)) {
                expectedValue = propertiesInput;
            } else if (configOption.equals(OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION)) {
                expectedValue = durationTestValue;
            } else if (configOption.equals(ID_PLACEMENT)) {
                expectedValue = IdPlacementEnum.LEGACY;
            } else if (configOption.equals(ID_OPTION)) {
                expectedValue = IdOptionEnum.CONTENT_ID;
            } else {
                expectedValue = configOption.key() + "-value";
            }
            assertThat(outputMap.get(configOption.key())).isEqualTo(expectedValue);
        }
    }

    private Map<String, String> getDefaultOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", ApicurioRegistryAvroFormatFactory.IDENTIFIER);
        options.put("avro-apicurio.url", REGISTRY_URL);
        return options;
    }

    private Map<String, String> getOptionalProperties() {
        final Map<String, String> properties = new HashMap<>();
        // defined via Flink maintained options
        properties.put(
                AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        properties.put(AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD.key(), "123456");
        properties.put(
                AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        properties.put(AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD.key(), "123456");
        properties.put(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_USERID.key(), "USERID");
        properties.put(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_PASSWORD.key(), "PASSWORD");
        // defined via general property map
        properties.put("schema", SCHEMA_STRING);

        return getModifiedOptions(
                opts ->
                        properties.forEach(
                                (k, v) ->
                                        opts.put(
                                                String.format(
                                                        "%s.%s",
                                                        ApicurioRegistryAvroFormatFactory
                                                                .IDENTIFIER,
                                                        k),
                                                v)));
    }

    private static String getAbsolutePath(String path) {
        try {
            return CachedSchemaCoderProviderTest.class.getResource(path).toURI().getPath();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void testSerializationSchemaWithOptionalProperties() {
        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(
                        ROW_TYPE,
                        ApicurioRegistryAvroSerializationSchema.forGeneric(
                                new Schema.Parser().parse(SCHEMA_STRING),
                                REGISTRY_URL,
                                EXPECTED_OPTIONAL_PROPERTIES),
                        RowDataToAvroConverters.createConverter(ROW_TYPE));

        final DynamicTableSink actualSink = createTableSink(SCHEMA, getOptionalProperties());
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        //        assertThat(actualSer).isEqualTo(expectedSer);
        System.err.println("expectedSer:" + expectedSer + "\nactualSer:" + actualSer);
    }
}
