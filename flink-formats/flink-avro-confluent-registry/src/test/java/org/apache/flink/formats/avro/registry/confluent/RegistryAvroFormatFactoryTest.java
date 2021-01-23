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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for the {@link RegistryAvroFormatFactory}. */
public class RegistryAvroFormatFactoryTest {
    private static final TableSchema SCHEMA =
            TableSchema.builder()
                    .field("a", DataTypes.STRING())
                    .field("b", DataTypes.INT())
                    .field("c", DataTypes.BOOLEAN())
                    .build();
    private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();
    private static final String SUBJECT = "test-subject";
    private static final String REGISTRY_URL = "http://localhost:8081";

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testDeserializationSchema() {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(ROW_TYPE), REGISTRY_URL),
                        AvroToRowDataConverters.createRowConverter(ROW_TYPE),
                        InternalTypeInfo.of(ROW_TYPE));

        final DynamicTableSource actualSource = createTableSource(getDefaultOptions());
        assertThat(actualSource, instanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class));
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toRowDataType());

        assertEquals(expectedDeser, actualDeser);
    }

    @Test
    public void testSerializationSchema() {
        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(
                        ROW_TYPE,
                        ConfluentRegistryAvroSerializationSchema.forGeneric(
                                SUBJECT,
                                AvroSchemaConverter.convertToSchema(ROW_TYPE),
                                REGISTRY_URL),
                        RowDataToAvroConverters.createConverter(ROW_TYPE));

        final DynamicTableSink actualSink = createTableSink(getDefaultOptions());
        assertThat(actualSink, instanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class));
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toRowDataType());

        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testMissingSubjectForSink() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Option avro-confluent.schema-registry.subject "
                                        + "is required for serialization")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.remove("avro-confluent.schema-registry.subject"));

        createTableSink(options);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

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

    private Map<String, String> getDefaultOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", RegistryAvroFormatFactory.IDENTIFIER);
        options.put("avro-confluent.schema-registry.subject", SUBJECT);
        options.put("avro-confluent.schema-registry.url", REGISTRY_URL);
        return options;
    }

    private DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock source"),
                new Configuration(),
                RegistryAvroFormatFactoryTest.class.getClassLoader(),
                false);
    }

    private DynamicTableSink createTableSink(Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock sink"),
                new Configuration(),
                RegistryAvroFormatFactoryTest.class.getClassLoader(),
                false);
    }
}
