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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

/** Tests for the {@link JsonFormatFactory}. */
public class JsonFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSeDeSchema() {
        final Map<String, String> tableOptions = getAllOptions();

        testSchemaSerializationSchema(tableOptions);

        testSchemaDeserializationSchema(tableOptions);
    }

    @Test
    public void testFailOnMissingField() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json.fail-on-missing-field", "true"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "fail-on-missing-field and ignore-parse-errors shouldn't both be true.")));
        testSchemaDeserializationSchema(tableOptions);
    }

    @Test
    public void testInvalidOptionForIgnoreParseErrors() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json.ignore-parse-errors", "abc"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new IllegalArgumentException(
                                "Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));
        testSchemaDeserializationSchema(tableOptions);
    }

    @Test
    public void testInvalidOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json.timestamp-format.standard", "test"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'test' for timestamp-format.standard. Supported values are [SQL, ISO-8601].")));
        testSchemaDeserializationSchema(tableOptions);
    }

    @Test
    public void testLowerCaseOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifyOptions(
                        options -> options.put("json.timestamp-format.standard", "iso-8601"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'iso-8601' for timestamp-format.standard. Supported values are [SQL, ISO-8601].")));
        testSchemaDeserializationSchema(tableOptions);
    }

    @Test
    public void testInvalidOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json.map-null-key.mode", "invalid"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'invalid' for option map-null-key.mode. Supported values are [LITERAL, FAIL, DROP].")));
        testSchemaSerializationSchema(tableOptions);
    }

    @Test
    public void testLowerCaseOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json.map-null-key.mode", "fail"));

        testSchemaDeserializationSchema(tableOptions);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void testSchemaDeserializationSchema(Map<String, String> options) {
        final JsonRowDataDeserializationSchema expectedDeser =
                new JsonRowDataDeserializationSchema(
                        PHYSICAL_TYPE,
                        InternalTypeInfo.of(PHYSICAL_TYPE),
                        false,
                        true,
                        TimestampFormat.ISO_8601);

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertEquals(expectedDeser, actualDeser);
    }

    private void testSchemaSerializationSchema(Map<String, String> options) {
        final JsonRowDataSerializationSchema expectedSer =
                new JsonRowDataSerializationSchema(
                        PHYSICAL_TYPE,
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

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifyOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", JsonFormatFactory.IDENTIFIER);
        options.put("json.fail-on-missing-field", "false");
        options.put("json.ignore-parse-errors", "true");
        options.put("json.timestamp-format.standard", "ISO-8601");
        options.put("json.map-null-key.mode", "LITERAL");
        options.put("json.map-null-key.literal", "null");
        options.put("json.encode.decimal-as-plain-number", "true");
        return options;
    }
}
