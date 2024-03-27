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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link AvroFormatFactory}. */
class AvroFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()),
                    Column.physical("d", DataTypes.TIMESTAMP(3)));

    private static final ResolvedSchema NEW_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()),
                    Column.physical("d", DataTypes.TIMESTAMP(3)),
                    Column.physical("e", DataTypes.TIMESTAMP(6)),
                    Column.physical("f", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                    Column.physical("g", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final RowType NEW_ROW_TYPE =
            (RowType) NEW_SCHEMA.toPhysicalRowDataType().getLogicalType();

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSeDeSchema(AvroEncoding encoding) {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        ROW_TYPE, InternalTypeInfo.of(ROW_TYPE), encoding);

        final Map<String, String> options = getAllOptions(true);

        final DynamicTableSource actualSource = FactoryMocks.createTableSource(SCHEMA, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);

        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(ROW_TYPE, encoding);

        final DynamicTableSink actualSink = FactoryMocks.createTableSink(SCHEMA, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        assertThat(actualSer).isEqualTo(expectedSer);
    }

    @Test
    void testOldSeDeNewSchema() {
        assertThatThrownBy(
                        () -> {
                            new AvroRowDataDeserializationSchema(
                                    NEW_ROW_TYPE, InternalTypeInfo.of(NEW_ROW_TYPE));
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Avro does not support TIMESTAMP type with precision: 6, it only supports precision less than 3.");

        assertThatThrownBy(
                        () -> {
                            new AvroRowDataSerializationSchema(NEW_ROW_TYPE);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Avro does not support TIMESTAMP type with precision: 6, it only supports precision less than 3.");
    }

    @Test
    void testNewSeDeNewSchema() {
        testSeDeSchema(NEW_ROW_TYPE, NEW_SCHEMA, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSeDeSchema(boolean legacyTimestampMapping) {
        testSeDeSchema(ROW_TYPE, SCHEMA, legacyTimestampMapping);
    }

    void testSeDeSchema(RowType rowType, ResolvedSchema schema, boolean legacyTimestampMapping) {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        AvroEncoding.BINARY,
                        legacyTimestampMapping);

        final Map<String, String> options = getAllOptions(legacyTimestampMapping);

        final DynamicTableSource actualSource = FactoryMocks.createTableSource(schema, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, schema.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);

        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(
                        rowType, AvroEncoding.BINARY, legacyTimestampMapping);

        final DynamicTableSink actualSink = FactoryMocks.createTableSink(schema, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, schema.toPhysicalRowDataType());

        assertThat(actualSer).isEqualTo(expectedSer);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private Map<String, String> getAllOptions(boolean legacyTimestampMapping) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        if (!legacyTimestampMapping) {
            options.put("avro.timestamp_mapping.legacy", "false");
        }

        options.put("format", AvroFormatFactory.IDENTIFIER);
        return options;
    }
}
