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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.raw.RawFormatDeserializationSchema;
import org.apache.flink.formats.raw.RawFormatSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RawFormatDeserializationSchema} and {@link RawFormatSerializationSchema} with
 * the {@code raw.line-delimiter} option.
 */
class RawFormatLineDelimiterTest {

    private static final VarCharType STRING_TYPE = VarCharType.STRING_TYPE;

    // -----------------------------------------------------------------------
    // Deserialization tests
    // -----------------------------------------------------------------------

    @Test
    void testDeserializeWithoutDelimiter_singleRow() throws Exception {
        RawFormatDeserializationSchema schema =
                new RawFormatDeserializationSchema(
                        STRING_TYPE,
                        TypeInformation.of(RowData.class),
                        StandardCharsets.UTF_8.name(),
                        true,
                        null);
        openDeser(schema);

        List<RowData> rows = collectRows(schema, "hello".getBytes(StandardCharsets.UTF_8));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0)).hasToString("hello");
    }

    @Test
    void testDeserializeWithNewlineDelimiter_multipleRows() throws Exception {
        RawFormatDeserializationSchema schema =
                new RawFormatDeserializationSchema(
                        STRING_TYPE,
                        TypeInformation.of(RowData.class),
                        StandardCharsets.UTF_8.name(),
                        true,
                        "\n");
        openDeser(schema);

        byte[] message = "line1\nline2\nline3".getBytes(StandardCharsets.UTF_8);
        List<RowData> rows = collectRows(schema, message);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0)).hasToString("line1");
        assertThat(rows.get(1).getString(0)).hasToString("line2");
        assertThat(rows.get(2).getString(0)).hasToString("line3");
    }

    @Test
    void testDeserializeWithCustomMultiCharDelimiter() throws Exception {
        RawFormatDeserializationSchema schema =
                new RawFormatDeserializationSchema(
                        STRING_TYPE,
                        TypeInformation.of(RowData.class),
                        StandardCharsets.UTF_8.name(),
                        true,
                        "||");
        openDeser(schema);

        byte[] message = "record1||record2||record3".getBytes(StandardCharsets.UTF_8);
        List<RowData> rows = collectRows(schema, message);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0)).hasToString("record1");
        assertThat(rows.get(1).getString(0)).hasToString("record2");
        assertThat(rows.get(2).getString(0)).hasToString("record3");
    }

    @Test
    void testDeserializeWithNullMessage_noOutput() throws Exception {
        RawFormatDeserializationSchema schema =
                new RawFormatDeserializationSchema(
                        STRING_TYPE,
                        TypeInformation.of(RowData.class),
                        StandardCharsets.UTF_8.name(),
                        true,
                        "\n");
        openDeser(schema);

        List<RowData> rows = collectRows(schema, null);
        assertThat(rows).isEmpty();
    }

    @Test
    void testDeserializeWithGbkCharset() throws Exception {
        Charset gbk = Charset.forName("GBK");
        String original = "你好\n世界";
        byte[] message = original.getBytes(gbk);

        RawFormatDeserializationSchema schema =
                new RawFormatDeserializationSchema(
                        STRING_TYPE, TypeInformation.of(RowData.class), "GBK", true, "\n");
        openDeser(schema);

        List<RowData> rows = collectRows(schema, message);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(0)).hasToString("你好");
        assertThat(rows.get(1).getString(0)).hasToString("世界");
    }

    // -----------------------------------------------------------------------
    // Serialization tests
    // -----------------------------------------------------------------------

    @Test
    void testSerializeWithoutDelimiter_noAppend() throws Exception {
        RawFormatSerializationSchema schema =
                new RawFormatSerializationSchema(
                        STRING_TYPE, StandardCharsets.UTF_8.name(), true, null);
        openSer(schema);

        RowData row = buildStringRow("hello");
        byte[] result = schema.serialize(row);
        assertThat(result).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testSerializeWithNewlineDelimiter_appendsDelimiter() throws Exception {
        RawFormatSerializationSchema schema =
                new RawFormatSerializationSchema(
                        STRING_TYPE, StandardCharsets.UTF_8.name(), true, "\n");
        openSer(schema);

        RowData row = buildStringRow("hello");
        byte[] result = schema.serialize(row);
        assertThat(result).isEqualTo("hello\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testSerializeWithCustomDelimiter_appendsDelimiter() throws Exception {
        RawFormatSerializationSchema schema =
                new RawFormatSerializationSchema(
                        STRING_TYPE, StandardCharsets.UTF_8.name(), true, "||");
        openSer(schema);

        RowData row = buildStringRow("record1");
        byte[] result = schema.serialize(row);
        assertThat(result).isEqualTo("record1||".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testSerializeNullRow_returnsNull() throws Exception {
        RawFormatSerializationSchema schema =
                new RawFormatSerializationSchema(
                        STRING_TYPE, StandardCharsets.UTF_8.name(), true, "\n");
        openSer(schema);

        GenericRowData nullRow = new GenericRowData(1);
        nullRow.setField(0, null);
        byte[] result = schema.serialize(nullRow);
        assertThat(result).isNull();
    }

    @Test
    void testDeserializeTrailingDelimiter_noExtraRow() throws Exception {
        // Verify that a message ending with the delimiter does not produce a trailing empty row.
        // This ensures round-trip compatibility: serialize("hello") -> "hello\n" ->
        // deserialize -> ["hello"] (1 row, not 2).
        RawFormatDeserializationSchema schema =
                new RawFormatDeserializationSchema(
                        STRING_TYPE,
                        TypeInformation.of(RowData.class),
                        StandardCharsets.UTF_8.name(),
                        true,
                        "\n");
        openDeser(schema);

        // Message already ends with the delimiter (as produced by the serializer)
        byte[] message = "hello\n".getBytes(StandardCharsets.UTF_8);
        List<RowData> rows = collectRows(schema, message);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0)).hasToString("hello");
    }

    @Test
    void testRoundTrip_serializeThenDeserialize() throws Exception {
        // Verify that rows written by the serializer can be read back correctly by the
        // deserializer when both share the same delimiter configuration.
        RawFormatSerializationSchema ser =
                new RawFormatSerializationSchema(
                        STRING_TYPE, StandardCharsets.UTF_8.name(), true, "\n");
        openSer(ser);

        RawFormatDeserializationSchema deser =
                new RawFormatDeserializationSchema(
                        STRING_TYPE,
                        TypeInformation.of(RowData.class),
                        StandardCharsets.UTF_8.name(),
                        true,
                        "\n");
        openDeser(deser);

        // Serialize a single row -> "hello\n"
        byte[] serialized = ser.serialize(buildStringRow("hello"));

        // Deserialize "hello\n" -> should yield exactly 1 row
        List<RowData> rows = collectRows(deser, serialized);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0)).hasToString("hello");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void openDeser(RawFormatDeserializationSchema schema) throws Exception {
        schema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        throw new UnsupportedOperationException();
                    }
                });
    }

    private void openSer(RawFormatSerializationSchema schema) throws Exception {
        schema.open(
                new SerializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        throw new UnsupportedOperationException();
                    }
                });
    }

    private List<RowData> collectRows(RawFormatDeserializationSchema schema, byte[] message)
            throws Exception {
        List<RowData> rows = new ArrayList<>();
        schema.deserialize(
                message,
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData record) {
                        rows.add(record);
                    }

                    @Override
                    public void close() {}
                });
        return rows;
    }

    private RowData buildStringRow(String value) {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(value));
        return row;
    }
}
