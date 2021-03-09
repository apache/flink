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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.data.StringData.fromString;
import static org.junit.Assert.assertEquals;

/** Tests for {@link CsvFormatFactory}. */
public class CsvFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    private static final TableSchema SCHEMA =
            TableSchema.builder()
                    .field("a", DataTypes.STRING())
                    .field("b", DataTypes.INT())
                    .field("c", DataTypes.BOOLEAN())
                    .build();

    private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

    @Test
    public void testSeDeSchema() {
        final CsvRowDataDeserializationSchema expectedDeser =
                new CsvRowDataDeserializationSchema.Builder(ROW_TYPE, InternalTypeInfo.of(ROW_TYPE))
                        .setFieldDelimiter(';')
                        .setQuoteCharacter('\'')
                        .setAllowComments(true)
                        .setIgnoreParseErrors(true)
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        final Map<String, String> options = getAllOptions();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeser, actualDeser);

        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(ROW_TYPE)
                        .setFieldDelimiter(';')
                        .setQuoteCharacter('\'')
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testDisableQuoteCharacter() {
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> {
                            opts.put("csv.disable-quote-character", "true");
                            opts.remove("csv.quote-character");
                        });

        final CsvRowDataDeserializationSchema expectedDeser =
                new CsvRowDataDeserializationSchema.Builder(ROW_TYPE, InternalTypeInfo.of(ROW_TYPE))
                        .setFieldDelimiter(';')
                        .setAllowComments(true)
                        .setIgnoreParseErrors(true)
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .disableQuoteCharacter()
                        .build();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);

        assertEquals(expectedDeser, actualDeser);

        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(ROW_TYPE)
                        .setFieldDelimiter(';')
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .disableQuoteCharacter()
                        .build();
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);

        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testDisableQuoteCharacterException() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Format cannot define a quote character and disabled quote character at the same time.")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("csv.disable-quote-character", "true"));

        createTableSink(options);
    }

    @Test
    public void testInvalidCharacterOption() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Option 'csv.quote-character' must be a string with single character, but was: abc")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("csv.quote-character", "abc"));

        createTableSink(options);
    }

    @Test
    public void testEscapedFieldDelimiter() throws IOException {
        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(ROW_TYPE)
                        .setFieldDelimiter('\t')
                        .setQuoteCharacter('\'')
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        final CsvRowDataDeserializationSchema expectedDeser =
                new CsvRowDataDeserializationSchema.Builder(ROW_TYPE, InternalTypeInfo.of(ROW_TYPE))
                        .setFieldDelimiter('\t')
                        .setQuoteCharacter('\'')
                        .setAllowComments(true)
                        .setIgnoreParseErrors(true)
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();

        // test schema
        final Map<String, String> options1 =
                getModifiedOptions(opts -> opts.put("csv.field-delimiter", "\t"));
        SerializationSchema<RowData> serializationSchema1 = createSerializationSchema(options1);
        DeserializationSchema<RowData> deserializationSchema1 =
                createDeserializationSchema(options1);
        assertEquals(expectedSer, serializationSchema1);
        assertEquals(expectedDeser, deserializationSchema1);

        final Map<String, String> options2 =
                getModifiedOptions(opts -> opts.put("csv.field-delimiter", "\\t"));
        SerializationSchema<RowData> serializationSchema2 = createSerializationSchema(options2);
        DeserializationSchema<RowData> deserializationSchema2 =
                createDeserializationSchema(options2);
        assertEquals(expectedSer, serializationSchema2);
        assertEquals(expectedDeser, deserializationSchema2);

        // test (de)serialization
        RowData rowData = GenericRowData.of(fromString("abc"), 123, false);
        byte[] bytes = serializationSchema2.serialize(rowData);
        assertEquals("abc\t123\tfalse", new String(bytes));
        RowData actual = deserializationSchema2.deserialize("abc\t123\tfalse".getBytes());
        assertEquals(rowData, actual);
    }

    @Test
    public void testDeserializeWithEscapedFieldDelimiter() throws IOException {
        // test deserialization schema
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("csv.field-delimiter", "\t"));

        final DynamicTableSource actualSource = createTableSource(options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> deserializationSchema =
                sourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toRowDataType());
        RowData expected = GenericRowData.of(fromString("abc"), 123, false);
        RowData actual = deserializationSchema.deserialize("abc\t123\tfalse".getBytes());
        assertEquals(expected, actual);
    }

    @Test
    public void testInvalidIgnoreParseError() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new IllegalArgumentException(
                                "Unrecognized option for boolean: abc. "
                                        + "Expected either true or false(case insensitive)")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("csv.ignore-parse-errors", "abc"));

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
    private static Map<String, String> getModifiedOptions(
            Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private static Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", CsvFormatFactory.IDENTIFIER);
        options.put("csv.field-delimiter", ";");
        options.put("csv.quote-character", "'");
        options.put("csv.allow-comments", "true");
        options.put("csv.ignore-parse-errors", "true");
        options.put("csv.array-element-delimiter", "|");
        options.put("csv.escape-character", "\\");
        options.put("csv.null-literal", "n/a");
        return options;
    }

    private static DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        return sourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, SCHEMA.toRowDataType());
    }

    private static SerializationSchema<RowData> createSerializationSchema(
            Map<String, String> options) {
        final DynamicTableSink actualSink = createTableSink(options);
        assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        return sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toRowDataType());
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock source"),
                new Configuration(),
                CsvFormatFactoryTest.class.getClassLoader(),
                false);
    }

    private static DynamicTableSink createTableSink(Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock sink"),
                new Configuration(),
                CsvFormatFactoryTest.class.getClassLoader(),
                false);
    }
}
