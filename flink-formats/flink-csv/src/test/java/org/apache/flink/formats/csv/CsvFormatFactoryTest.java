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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.connector.testutils.formats.SchemaTestUtils.open;
import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.data.DecimalData.fromBigDecimal;
import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_DATA_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CsvFormatFactory}. */
public class CsvFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSeDeSchema() {
        final CsvRowDataDeserializationSchema expectedDeser =
                new CsvRowDataDeserializationSchema.Builder(
                                PHYSICAL_TYPE, InternalTypeInfo.of(PHYSICAL_TYPE))
                        .setFieldDelimiter(';')
                        .setQuoteCharacter('\'')
                        .setAllowComments(true)
                        .setIgnoreParseErrors(true)
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        open(expectedDeser);
        final Map<String, String> options = getAllOptions();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertThat(actualDeser).isEqualTo(expectedDeser);

        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(PHYSICAL_TYPE)
                        .setFieldDelimiter(';')
                        .setQuoteCharacter('\'')
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertThat(actualSer).isEqualTo(expectedSer);
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
                new CsvRowDataDeserializationSchema.Builder(
                                PHYSICAL_TYPE, InternalTypeInfo.of(PHYSICAL_TYPE))
                        .setFieldDelimiter(';')
                        .setAllowComments(true)
                        .setIgnoreParseErrors(true)
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .disableQuoteCharacter()
                        .build();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);

        assertThat(actualDeser).isEqualTo(expectedDeser);

        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(PHYSICAL_TYPE)
                        .setFieldDelimiter(';')
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .disableQuoteCharacter()
                        .build();
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);

        assertThat(actualSer).isEqualTo(expectedSer);
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

        createTableSink(SCHEMA, options);
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

        createTableSink(SCHEMA, options);
    }

    @Test
    public void testEscapedFieldDelimiter() throws IOException {
        final CsvRowDataSerializationSchema expectedSer =
                new CsvRowDataSerializationSchema.Builder(PHYSICAL_TYPE)
                        .setFieldDelimiter('\t')
                        .setQuoteCharacter('\'')
                        .setArrayElementDelimiter("|")
                        .setEscapeCharacter('\\')
                        .setNullLiteral("n/a")
                        .build();
        final CsvRowDataDeserializationSchema expectedDeser =
                new CsvRowDataDeserializationSchema.Builder(
                                PHYSICAL_TYPE, InternalTypeInfo.of(PHYSICAL_TYPE))
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
        assertThat(serializationSchema1).isEqualTo(expectedSer);
        assertThat(deserializationSchema1).isEqualTo(expectedDeser);

        final Map<String, String> options2 =
                getModifiedOptions(opts -> opts.put("csv.field-delimiter", "\\t"));
        SerializationSchema<RowData> serializationSchema2 = createSerializationSchema(options2);
        DeserializationSchema<RowData> deserializationSchema2 =
                createDeserializationSchema(options2);
        assertThat(serializationSchema2).isEqualTo(expectedSer);
        assertThat(deserializationSchema2).isEqualTo(expectedDeser);

        // test (de)serialization
        RowData rowData = GenericRowData.of(fromString("abc"), 123, false);
        byte[] bytes = serializationSchema2.serialize(rowData);
        assertThat(new String(bytes)).isEqualTo("abc\t123\tfalse");
        RowData actual = deserializationSchema2.deserialize("abc\t123\tfalse".getBytes());
        assertThat(actual).isEqualTo(rowData);
    }

    @Test
    public void testDeserializeWithEscapedFieldDelimiter() throws IOException {
        // test deserialization schema
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("csv.field-delimiter", "\t"));

        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                createDynamicTableSourceMock(options);

        DeserializationSchema<RowData> deserializationSchema =
                sourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE);
        open(deserializationSchema);
        RowData expected = GenericRowData.of(fromString("abc"), 123, false);
        RowData actual = deserializationSchema.deserialize("abc\t123\tfalse".getBytes());
        assertThat(actual).isEqualTo(expected);
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

        createTableSink(SCHEMA, options);
    }

    @Test
    public void testSerializationWithWriteBigDecimalInScientificNotation() {
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> opts.put("csv.write-bigdecimal-in-scientific-notation", "true"));

        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("a", DataTypes.STRING()),
                        Column.physical("b", DataTypes.DECIMAL(10, 3)),
                        Column.physical("c", DataTypes.BOOLEAN()));
        final DynamicTableSink actualSink = createTableSink(schema, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> runtimeEncoder =
                sinkMock.valueFormat.createRuntimeEncoder(null, schema.toPhysicalRowDataType());
        open(runtimeEncoder);

        RowData rowData =
                GenericRowData.of(
                        fromString("abc"), fromBigDecimal(new BigDecimal("100000"), 10, 3), false);
        byte[] bytes = runtimeEncoder.serialize(rowData);
        assertThat(new String(bytes)).isEqualTo("abc;'1E+5';false");
    }

    @Test
    public void testSerializationWithNotWriteBigDecimalInScientificNotation() {
        final Map<String, String> options =
                getModifiedOptions(
                        opts -> opts.put("csv.write-bigdecimal-in-scientific-notation", "false"));

        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("a", DataTypes.STRING()),
                        Column.physical("b", DataTypes.DECIMAL(10, 3)),
                        Column.physical("c", DataTypes.BOOLEAN()));
        final DynamicTableSink actualSink = createTableSink(schema, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> runtimeEncoder =
                sinkMock.valueFormat.createRuntimeEncoder(null, schema.toPhysicalRowDataType());
        open(runtimeEncoder);

        RowData rowData =
                GenericRowData.of(
                        fromString("abc"), fromBigDecimal(new BigDecimal("100000"), 10, 3), false);
        byte[] bytes = runtimeEncoder.serialize(rowData);
        assertThat(new String(bytes)).isEqualTo("abc;'100000';false");
    }

    @Test
    public void testProjectionPushdown() throws IOException {
        final Map<String, String> options = getAllOptions();

        final Projection projection =
                Projection.fromFieldNames(PHYSICAL_DATA_TYPE, Collections.singletonList("c"));

        final int[][] projectionMatrix = projection.toNestedIndexes();
        DeserializationSchema<RowData> actualDeser =
                createDeserializationSchema(options, projectionMatrix);

        String data = "a1;2;false";
        RowData deserialized = actualDeser.deserialize(data.getBytes());
        GenericRowData expected = GenericRowData.of(false);

        assertThat(deserialized).isEqualTo(expected);
    }

    @Test
    public void testProjectionPushdownNoOpProjection() throws IOException {
        final Map<String, String> options = getAllOptions();

        List<String> fields = Arrays.asList("a", "b", "c");
        final Projection projection = Projection.fromFieldNames(PHYSICAL_DATA_TYPE, fields);

        final int[][] projectionMatrix = projection.toNestedIndexes();
        DeserializationSchema<RowData> actualDeser =
                createDeserializationSchema(options, projectionMatrix);

        String data = "a1;2;false";
        RowData deserialized = actualDeser.deserialize(data.getBytes());
        GenericRowData expected = GenericRowData.of(fromString("a1"), 2, false);

        assertThat(deserialized).isEqualTo(expected);
    }

    @Test
    public void testProjectionPushdownEmptyProjection() throws IOException {
        final Map<String, String> options = getAllOptions();

        final int[][] projectionMatrix = new int[][] {};
        DeserializationSchema<RowData> actualDeser =
                createDeserializationSchema(options, projectionMatrix);

        String data = "a1;2;false";
        RowData deserialized = actualDeser.deserialize(data.getBytes());
        GenericRowData expected = GenericRowData.of();

        assertThat(deserialized).isEqualTo(expected);
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

        options.put("format", CsvCommons.IDENTIFIER);
        options.put("csv.field-delimiter", ";");
        options.put("csv.quote-character", "'");
        options.put("csv.allow-comments", "true");
        options.put("csv.ignore-parse-errors", "true");
        options.put("csv.array-element-delimiter", "|");
        options.put("csv.escape-character", "\\");
        options.put("csv.null-literal", "n/a");
        options.put("csv.write-bigdecimal-in-scientific-notation", "true");
        return options;
    }

    private static DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                createDynamicTableSourceMock(options);

        final DeserializationSchema<RowData> schema =
                sourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE);
        open(schema);
        return schema;
    }

    private static DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options, int[][] projections) {
        TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                createDynamicTableSourceMock(options);

        ProjectableDecodingFormat<DeserializationSchema<RowData>> valueFormat =
                (ProjectableDecodingFormat<DeserializationSchema<RowData>>) sourceMock.valueFormat;

        final DeserializationSchema<RowData> schema =
                valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE, projections);
        open(schema);
        return schema;
    }

    private static TestDynamicTableFactory.DynamicTableSourceMock createDynamicTableSourceMock(
            Map<String, String> options) {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        return (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
    }

    private static SerializationSchema<RowData> createSerializationSchema(
            Map<String, String> options) {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        final SerializationSchema<RowData> schema =
                sinkMock.valueFormat.createRuntimeEncoder(null, PHYSICAL_DATA_TYPE);
        open(schema);
        return schema;
    }
}
