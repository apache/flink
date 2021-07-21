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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

/** Tests for {@link DebeziumAvroFormatFactory}. */
public class DebeziumAvroFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SUBJECT = "test-debezium-avro";
    private static final String REGISTRY_URL = "http://localhost:8081";

    @Test
    public void testSeDeSchema() {
        final Map<String, String> options = getAllOptions();

        final Map<String, String> registryConfigs = new HashMap<>();
        registryConfigs.put("basic.auth.user.info", "something1");
        registryConfigs.put("basic.auth.credentials.source", "something2");

        DebeziumAvroDeserializationSchema expectedDeser =
                new DebeziumAvroDeserializationSchema(
                        ROW_TYPE, InternalTypeInfo.of(ROW_TYPE), REGISTRY_URL, registryConfigs);
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeser, actualDeser);

        DebeziumAvroSerializationSchema expectedSer =
                new DebeziumAvroSerializationSchema(
                        ROW_TYPE, REGISTRY_URL, SUBJECT, registryConfigs);
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        Assert.assertEquals(expectedSer, actualSer);
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", DebeziumAvroFormatFactory.IDENTIFIER);
        options.put("debezium-avro-confluent.schema-registry.url", REGISTRY_URL);
        options.put("debezium-avro-confluent.schema-registry.subject", SUBJECT);
        options.put("debezium-avro-confluent.basic-auth.user-info", "something1");
        options.put("debezium-avro-confluent.basic-auth.credentials-source", "something2");
        return options;
    }

    private static DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assertThat(actualSource, instanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class));
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        return scanSourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());
    }

    private static SerializationSchema<RowData> createSerializationSchema(
            Map<String, String> options) {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assertThat(actualSink, instanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class));
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        return sinkMock.valueFormat.createRuntimeEncoder(
                new SinkRuntimeProviderContext(false), SCHEMA.toPhysicalRowDataType());
    }
}
