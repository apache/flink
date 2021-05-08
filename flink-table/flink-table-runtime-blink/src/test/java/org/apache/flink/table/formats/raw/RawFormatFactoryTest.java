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

package org.apache.flink.table.formats.raw;

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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/** Tests for {@link RawFormatFactory}. */
public class RawFormatFactoryTest extends TestLogger {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(Column.physical("field1", DataTypes.STRING()));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    @Test
    public void testSeDeSchema() {
        final Map<String, String> tableOptions = getBasicOptions();

        // test deserialization
        final RawFormatDeserializationSchema expectedDeser =
                new RawFormatDeserializationSchema(
                        ROW_TYPE.getTypeAt(0), InternalTypeInfo.of(ROW_TYPE), "UTF-8", true);
        DeserializationSchema<RowData> actualDeser =
                createDeserializationSchema(SCHEMA, tableOptions);
        assertEquals(expectedDeser, actualDeser);

        // test serialization
        final RawFormatSerializationSchema expectedSer =
                new RawFormatSerializationSchema(ROW_TYPE.getTypeAt(0), "UTF-8", true);
        SerializationSchema<RowData> actualSer = createSerializationSchema(SCHEMA, tableOptions);
        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testCharsetAndEndiannessOption() {
        final Map<String, String> tableOptions =
                getModifiedOptions(
                        options -> {
                            options.put("raw.charset", "UTF-16");
                            options.put("raw.endianness", "little-endian");
                        });

        // test deserialization
        final RawFormatDeserializationSchema expectedDeser =
                new RawFormatDeserializationSchema(
                        ROW_TYPE.getTypeAt(0), InternalTypeInfo.of(ROW_TYPE), "UTF-16", false);
        DeserializationSchema<RowData> actualDeser =
                createDeserializationSchema(SCHEMA, tableOptions);
        assertEquals(expectedDeser, actualDeser);

        // test serialization
        final RawFormatSerializationSchema expectedSer =
                new RawFormatSerializationSchema(ROW_TYPE.getTypeAt(0), "UTF-16", false);
        SerializationSchema<RowData> actualSer = createSerializationSchema(SCHEMA, tableOptions);
        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testInvalidSchema() {
        ResolvedSchema invalidSchema =
                ResolvedSchema.of(
                        Column.physical("f0", DataTypes.STRING()),
                        Column.physical("f1", DataTypes.BIGINT()));
        String expectedError =
                "The 'raw' format only supports single physical column. "
                        + "However the defined schema contains multiple physical columns: [`f0` STRING, `f1` BIGINT]";

        try {
            createDeserializationSchema(invalidSchema, getBasicOptions());
            fail();
        } catch (Exception e) {
            assertThat(e, hasMessage(equalTo(expectedError)));
        }

        try {
            createSerializationSchema(invalidSchema, getBasicOptions());
            fail();
        } catch (Exception e) {
            assertThat(e, hasMessage(equalTo(expectedError)));
        }
    }

    @Test
    public void testInvalidCharset() {
        final Map<String, String> tableOptions =
                getModifiedOptions(
                        options -> {
                            options.put("raw.charset", "UNKNOWN");
                        });

        String expectedError = "Unsupported 'raw.charset' name: UNKNOWN.";

        try {
            createDeserializationSchema(SCHEMA, tableOptions);
            fail();
        } catch (Exception e) {
            assertThat(e.getCause().getCause(), hasMessage(equalTo(expectedError)));
        }

        try {
            createSerializationSchema(SCHEMA, tableOptions);
            fail();
        } catch (Exception e) {
            assertThat(e.getCause().getCause(), hasMessage(equalTo(expectedError)));
        }
    }

    @Test
    public void testInvalidEndianness() {
        final Map<String, String> tableOptions =
                getModifiedOptions(
                        options -> {
                            options.put("raw.endianness", "BIG_ENDIAN");
                        });

        String expectedError =
                "Unsupported endianness name: BIG_ENDIAN. "
                        + "Valid values of 'raw.endianness' option are 'big-endian' and 'little-endian'.";

        try {
            createDeserializationSchema(SCHEMA, tableOptions);
            fail();
        } catch (Exception e) {
            assertThat(e.getCause().getCause(), hasMessage(equalTo(expectedError)));
        }

        try {
            createSerializationSchema(SCHEMA, tableOptions);
            fail();
        } catch (Exception e) {
            assertThat(e.getCause().getCause(), hasMessage(equalTo(expectedError)));
        }
    }

    @Test
    public void testInvalidFieldTypes() {
        try {
            createDeserializationSchema(
                    ResolvedSchema.of(Column.physical("field1", DataTypes.TIMESTAMP(3))),
                    getBasicOptions());
            fail();
        } catch (Exception e) {
            assertThat(
                    e,
                    hasMessage(
                            equalTo(
                                    "The 'raw' format doesn't supports 'TIMESTAMP(3)' as column type.")));
        }

        try {
            createDeserializationSchema(
                    ResolvedSchema.of(
                            Column.physical(
                                    "field1", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))),
                    getBasicOptions());
            fail();
        } catch (Exception e) {
            assertThat(
                    e,
                    hasMessage(
                            equalTo(
                                    "The 'raw' format doesn't supports 'MAP<INT, STRING>' as column type.")));
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static DeserializationSchema<RowData> createDeserializationSchema(
            ResolvedSchema schema, Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(schema, options);
        assertThat(actualSource, instanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class));
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        return scanSourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, schema.toPhysicalRowDataType());
    }

    private static SerializationSchema<RowData> createSerializationSchema(
            ResolvedSchema schema, Map<String, String> options) {
        final DynamicTableSink actualSink = createTableSink(schema, options);
        assertThat(actualSink, instanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class));
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        return sinkMock.valueFormat.createRuntimeEncoder(
                new SinkRuntimeProviderContext(false), schema.toPhysicalRowDataType());
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getBasicOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getBasicOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", RawFormatFactory.IDENTIFIER);
        return options;
    }
}
