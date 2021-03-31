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

package org.apache.flink.formats.json.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_DATA_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for {@link DebeziumJsonFormatFactory}. */
public class DebeziumJsonFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSeDeSchema() {
        final DebeziumJsonDeserializationSchema expectedDeser =
                new DebeziumJsonDeserializationSchema(
                        PHYSICAL_DATA_TYPE,
                        Collections.emptyList(),
                        InternalTypeInfo.of(PHYSICAL_TYPE),
                        false,
                        true,
                        TimestampFormat.ISO_8601);

        final Map<String, String> options = getAllOptions();

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE);

        assertEquals(expectedDeser, actualDeser);

        final DebeziumJsonSerializationSchema expectedSer =
                new DebeziumJsonSerializationSchema(
                        (RowType) PHYSICAL_DATA_TYPE.getLogicalType(),
                        TimestampFormat.ISO_8601,
                        JsonOptions.MapNullKeyMode.LITERAL,
                        "null",
                        true);

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(
                        new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE);

        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testInvalidIgnoreParseError() {
        thrown.expect(
                containsCause(
                        new IllegalArgumentException(
                                "Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("debezium-json.ignore-parse-errors", "abc"));

        createTableSource(SCHEMA, options);
    }

    @Test
    public void testSchemaIncludeOption() {
        Map<String, String> options = getAllOptions();
        options.put("debezium-json.schema-include", "true");

        final DebeziumJsonDeserializationSchema expectedDeser =
                new DebeziumJsonDeserializationSchema(
                        PHYSICAL_DATA_TYPE,
                        Collections.emptyList(),
                        InternalTypeInfo.of(PHYSICAL_DATA_TYPE.getLogicalType()),
                        true,
                        true,
                        TimestampFormat.ISO_8601);
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE);
        assertEquals(expectedDeser, actualDeser);

        try {
            final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
            TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                    (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;
            // should fail
            sinkMock.valueFormat.createRuntimeEncoder(
                    new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE);
            fail();
        } catch (Exception e) {
            assertEquals(
                    e.getCause().getCause().getMessage(),
                    "Debezium JSON serialization doesn't support "
                            + "'debezium-json.schema-include' option been set to true.");
        }
    }

    @Test
    public void testInvalidOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifiedOptions(
                        opts -> opts.put("debezium-json.timestamp-format.standard", "test"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'test' for timestamp-format.standard. Supported values are [SQL, ISO-8601].")));
        createTableSource(SCHEMA, tableOptions);
    }

    @Test
    public void testInvalidOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifiedOptions(opts -> opts.put("debezium-json.map-null-key.mode", "invalid"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'invalid' for option map-null-key.mode. Supported values are [LITERAL, FAIL, DROP].")));
        createTableSink(SCHEMA, tableOptions);
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
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", "debezium-json");
        options.put("debezium-json.ignore-parse-errors", "true");
        options.put("debezium-json.timestamp-format.standard", "ISO-8601");
        options.put("debezium-json.map-null-key.mode", "LITERAL");
        options.put("debezium-json.map-null-key.literal", "null");
        options.put("debezium-json.encode.decimal-as-plain-number", "true");
        return options;
    }
}
