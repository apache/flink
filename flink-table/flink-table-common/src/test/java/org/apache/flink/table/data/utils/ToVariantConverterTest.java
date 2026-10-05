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

package org.apache.flink.table.data.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.utils.UuidUtils;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder.FieldEntry;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.data.utils.ToVariantConverter.MAX_PAYLOAD_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToVariantConverterTest {

    private static final VariantBuilder BUILDER = Variant.newBuilder();

    private static final String REPLACEMENT_CHARACTER = "\uFFFD";

    private static final LocalDateTime DATE_TIME =
            LocalDateTime.of(2026, 9, 25, 10, 15, 30, 123_000_000);

    private static final UUID UUID_VALUE = UUID.fromString("0e1f2a3b-4c5d-6e7f-8091-a2b3c4d5e6f7");

    private static final LogicalType LEAVES =
            DataTypes.ROW(
                            FIELD("bool", DataTypes.BOOLEAN()),
                            FIELD("tiny", DataTypes.TINYINT()),
                            FIELD("small", DataTypes.SMALLINT()),
                            FIELD("int", DataTypes.INT()),
                            FIELD("big", DataTypes.BIGINT()),
                            FIELD("float", DataTypes.FLOAT()),
                            FIELD("double", DataTypes.DOUBLE()),
                            FIELD("decimal", DataTypes.DECIMAL(10, 2)),
                            FIELD("char", DataTypes.CHAR(4)),
                            FIELD("bytes", DataTypes.BYTES()),
                            FIELD("date", DataTypes.DATE()),
                            FIELD("time", DataTypes.TIME()),
                            FIELD("ts", DataTypes.TIMESTAMP(3)),
                            FIELD("tsNanos", DataTypes.TIMESTAMP(9)),
                            FIELD("ltz", DataTypes.TIMESTAMP_LTZ(6)),
                            FIELD("ltzNanos", DataTypes.TIMESTAMP_LTZ(9)),
                            FIELD("uuid", DataTypes.UUID()))
                    .getLogicalType();

    private static final LogicalType MONEY =
            DistinctType.newBuilder(ObjectIdentifier.of("cat", "db", "Money"), new IntType())
                    .build();

    private static final LogicalType ORDER =
            StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "Order"))
                    .attributes(
                            List.of(
                                    new StructuredAttribute("id", new IntType()),
                                    new StructuredAttribute("price", MONEY)))
                    .build();

    @Test
    void testLeavesKeepTheKindOfTheirType() {
        final Variant variant = convert(LEAVES, leaves());

        assertThat(variant.getField("bool").getBoolean()).isTrue();
        assertThat(variant.getField("tiny").getType()).isEqualTo(Variant.Type.TINYINT);
        assertThat(variant.getField("small").getType()).isEqualTo(Variant.Type.SMALLINT);
        assertThat(variant.getField("int").getType()).isEqualTo(Variant.Type.INT);
        assertThat(variant.getField("big").getType()).isEqualTo(Variant.Type.BIGINT);
        assertThat(variant.getField("float").getFloat()).isEqualTo(1.5f);
        assertThat(variant.getField("double").getDouble()).isEqualTo(2.5d);
        assertThat(variant.getField("decimal").getDecimal()).isEqualByComparingTo("12.50");
        assertThat(variant.getField("char").getString()).isEqualTo("ab  ");
        assertThat(variant.getField("bytes").getBytes()).containsExactly((byte) 0xCA, (byte) 0xFE);
        assertThat(variant.getField("date").getDate()).isEqualTo(LocalDate.of(2026, 9, 25));
        assertThat(variant.getField("time").getTime()).isEqualTo(LocalTime.of(10, 15, 30));
        assertThat(variant.getField("ts").getType()).isEqualTo(Variant.Type.TIMESTAMP);
        assertThat(variant.getField("ts").getDateTime()).isEqualTo(DATE_TIME);
        assertThat(variant.getField("tsNanos").getType()).isEqualTo(Variant.Type.TIMESTAMP_NS);
        assertThat(variant.getField("ltz").getType()).isEqualTo(Variant.Type.TIMESTAMP_LTZ);
        assertThat(variant.getField("ltzNanos").getType()).isEqualTo(Variant.Type.TIMESTAMP_LTZ_NS);
        assertThat(variant.getField("uuid").getUuid()).isEqualTo(UUID_VALUE);
    }

    @Test
    void testNullBecomesVariantNull() {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("a", DataTypes.INT()),
                                FIELD("list", DataTypes.ARRAY(DataTypes.INT())),
                                FIELD("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))
                        .getLogicalType();
        final GenericRowData row =
                GenericRowData.of(
                        null,
                        new GenericArrayData(new Object[] {1, null}),
                        mapOf(new Object[] {StringData.fromString("k")}, new Object[] {null}));

        assertThat(convert(type, row).toJson())
                .isEqualTo("{\"a\":null,\"list\":[1,null],\"map\":{\"k\":null}}");
        final GenericArrayData nullElement = new GenericArrayData(new Object[] {null});
        assertThat(toJson(DataTypes.ARRAY(DataTypes.NULL()), nullElement)).isEqualTo("[null]");
        final GenericRowData nullField = GenericRowData.of((Object) null);
        assertThat(toJson(DataTypes.ROW(FIELD("n", DataTypes.NULL())), nullField))
                .isEqualTo("{\"n\":null}");
    }

    @Test
    void testEmptyValues() {
        assertThat(toJson(DataTypes.ARRAY(DataTypes.INT()), new GenericArrayData(new Object[0])))
                .isEqualTo("[]");
        final GenericMapData emptyMap = new GenericMapData(Map.of());
        assertThat(toJson(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), emptyMap))
                .isEqualTo("{}");
        assertThat(convert(new RowType(List.of()), new GenericRowData(0)).toJson()).isEqualTo("{}");
    }

    @Test
    void testDuplicateMapKeyKeepsTheLastValue() {
        final MapData map =
                mapOf(
                        new Object[] {StringData.fromString("a"), StringData.fromString("a")},
                        new Object[] {1, 2});

        final DataType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

        assertThat(convert(type, map).toJson()).isEqualTo("{\"a\":2}");
        // a builder that rejects duplicate keys fails instead
        final ToVariantConverter converter = ToVariantConverter.create(type.getLogicalType());
        final BinaryVariantInternalBuilder strict = new BinaryVariantInternalBuilder(false);
        assertThatThrownBy(() -> converter.writeTo(strict, map))
                .isSameAs(BinaryVariantInternalBuilder.VARIANT_DUPLICATE_KEY_EXCEPTION);
    }

    @Test
    void testNullMapKeyFails() {
        final MapData map = mapOf(new Object[] {null}, new Object[] {1});

        assertThatThrownBy(() -> convert(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), map))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a value of type MAP<STRING, INT> with a NULL key to VARIANT. "
                                + "A VARIANT object key cannot be NULL.");
    }

    @Test
    void testStructuredAndDistinctTypes() {
        final Variant variant = convert(ORDER, GenericRowData.of(1, 42));

        assertThat(variant.toJson()).isEqualTo("{\"id\":1,\"price\":42}");
        assertThat(variant.getField("price").getType()).isEqualTo(Variant.Type.INT);
    }

    @Test
    void testLeafHoldsUpToTheSizeLimit() {
        final byte[] bytes = new byte[MAX_PAYLOAD_BYTES];
        assertThat(convert(DataTypes.BYTES(), bytes).getBytes()).hasSize(MAX_PAYLOAD_BYTES);
        final StringData string = StringData.fromString("x".repeat(MAX_PAYLOAD_BYTES));
        assertThat(convert(DataTypes.STRING(), string).getString()).hasSize(MAX_PAYLOAD_BYTES);
    }

    @Test
    void testLeafOverTheSizeLimitFails() {
        final byte[] bytes = new byte[MAX_PAYLOAD_BYTES + 1];
        assertThatThrownBy(() -> convert(DataTypes.BYTES(), bytes))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a binary value of 16777212 bytes to VARIANT. A VARIANT is "
                                + "limited to 16 MiB, so a string or binary value can have at "
                                + "most 16777211 bytes.");
        final StringData string = StringData.fromString("x".repeat(MAX_PAYLOAD_BYTES + 1));
        assertThatThrownBy(() -> convert(DataTypes.STRING(), string))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith("Cannot cast a string value of 16777212 bytes to VARIANT.");
        final byte[] tooLong = new byte[MAX_PAYLOAD_BYTES + 1];
        Arrays.fill(tooLong, (byte) 'x');
        assertThatThrownBy(() -> convert(DataTypes.STRING(), binaryString(tooLong)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith("Cannot cast a string value of 16777212 bytes to VARIANT.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "hello", "Grüße, 世界 🚀"})
    void testStringKeepsItsUtf8Bytes(final String str) {
        assertThat(convert(DataTypes.STRING(), binaryString(str.getBytes(UTF_8))))
                .isEqualTo(BUILDER.of(str));
    }

    @ParameterizedTest
    @MethodSource("invalidUtf8")
    void testInvalidUtf8BecomesReplacementCharacters(final byte[] invalid) {
        final Variant variant = convert(DataTypes.STRING(), binaryString(invalid));

        assertThat(variant.getString()).contains(REPLACEMENT_CHARACTER);
        assertThat(variant).isEqualTo(BUILDER.of(binaryString(invalid).toString()));
    }

    private static Stream<byte[]> invalidUtf8() {
        return Stream.of(
                new byte[] {'a', (byte) 0xFF, 'b'},
                new byte[] {'a', (byte) 0xC3},
                new byte[] {(byte) 0xC0, (byte) 0xAF},
                new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80});
    }

    @Test
    void testJavaStringIsEncodedWithoutItsBinaryForm() {
        // An unpaired surrogate has no UTF-8 form, so encoding stores '?' in its place.
        final StringData value = StringData.fromString("a\uD800b");

        final Variant variant = convert(DataTypes.STRING(), value);

        assertThat(variant).isEqualTo(BUILDER.of(value.toString()));
        assertThat(variant.getString()).isEqualTo("a?b");
        assertThat(((BinaryStringData) value).getBinarySection()).isNull();
    }

    @ParameterizedTest
    @EnumSource(Layout.class)
    void testStringIsReadFromEverySegmentLayout(final Layout layout) {
        final String valid = "Grüße, 世界 🚀";
        final byte[] invalid = {'a', (byte) 0xFF, 'b'};

        assertThat(convert(DataTypes.STRING(), layout.place.apply(valid.getBytes(UTF_8))))
                .isEqualTo(BUILDER.of(layout.place.apply(valid.getBytes(UTF_8)).toString()));
        assertThat(convert(DataTypes.STRING(), layout.place.apply(invalid)))
                .isEqualTo(BUILDER.of(layout.place.apply(invalid).toString()));
    }

    /** Where the bytes of a {@link BinaryStringData} live. */
    private enum Layout {
        HEAP_SEGMENT(ToVariantConverterTest::binaryString),
        OFF_HEAP_SEGMENT(ToVariantConverterTest::offHeapString),
        FIRST_OF_TWO_SEGMENTS(ToVariantConverterTest::firstOfTwoSegments),
        ACROSS_TWO_SEGMENTS(ToVariantConverterTest::splitString),
        JAVA_OBJECT(utf8 -> StringData.fromString(new String(utf8, UTF_8)));

        private final Function<byte[], StringData> place;

        Layout(final Function<byte[], StringData> place) {
            this.place = place;
        }
    }

    @ParameterizedTest(name = "{0} as DECIMAL({1}, {2})")
    @CsvSource({
        "0, 1, 0",
        "1.50, 10, 2",
        "999999999, 9, 0",
        "-0.999999999, 9, 9",
        "1000000000, 10, 0",
        "0.0000000001, 10, 10",
        "-999999999999999999, 18, 0",
        "0.999999999999999999, 18, 18",
        "1000000000000000000, 19, 0",
        "-99999999999999999999999999999999999999, 38, 0",
        "0.99999999999999999999999999999999999999, 38, 38"
    })
    void testDecimalKeepsUnscaledValueAndScale(
            final BigDecimal decimal, final int precision, final int scale) {
        assertThat(
                        convert(
                                DataTypes.DECIMAL(precision, scale),
                                DecimalData.fromBigDecimal(decimal, precision, scale)))
                .isEqualTo(BUILDER.of(decimal));
    }

    @Test
    void testValueOverTheSizeLimitFails() {
        final StringData half = StringData.fromString("x".repeat(9 * 1024 * 1024));
        final ArrayData array = new GenericArrayData(new Object[] {half, half});

        assertThatThrownBy(() -> convert(DataTypes.ARRAY(DataTypes.STRING()), array))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a value of type ARRAY<STRING> to VARIANT. A VARIANT is "
                                + "limited to 16 MiB.");
    }

    @Test
    void testDeeplyNestedVariantFails() throws InterruptedException {
        // [[[...[1]...]]], built without recursion
        final BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendInt(1);
        for (int i = 0; i < 10_000; i++) {
            builder.finishWritingArray(0, new ArrayList<>(List.of(0)));
        }
        final ArrayData array = new GenericArrayData(new Object[] {builder.build()});

        // A small stack makes the overflow independent of the JVM's default stack size.
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread thread =
                new Thread(
                        null,
                        () -> {
                            try {
                                convert(DataTypes.ARRAY(DataTypes.VARIANT()), array);
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        },
                        "deeply-nested-variant",
                        256 * 1024);
        thread.start();
        thread.join();

        assertThat(error.get())
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a value of type ARRAY<VARIANT> to VARIANT because it is "
                                + "nested too deeply.");
    }

    @Test
    void testWriteIntoASharedBuilder() {
        final ToVariantConverter converter =
                ToVariantConverter.create(DataTypes.ARRAY(DataTypes.INT()).getLogicalType());
        final BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        final int start = builder.getWritePos();
        final ArrayList<FieldEntry> fields = new ArrayList<>();
        fields.add(new FieldEntry("a", builder.addKey("a"), builder.getWritePos() - start));
        converter.writeTo(builder, new GenericArrayData(new Object[] {1, null}));
        fields.add(new FieldEntry("b", builder.addKey("b"), builder.getWritePos() - start));
        converter.writeTo(builder, null);
        builder.finishWritingObject(start, fields);

        assertThat(builder.build().toJson()).isEqualTo("{\"a\":[1,null],\"b\":null}");
    }

    @Test
    void testTimeOutsideOneDayFails() {
        assertThat(convert(DataTypes.TIME(), 86_399_999).getTime())
                .isEqualTo(LocalTime.of(23, 59, 59, 999_000_000));
        assertThatThrownBy(() -> convert(DataTypes.TIME(), -1))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast the TIME value -1 to VARIANT. A TIME holds the milliseconds "
                                + "of one day, from 0 to 86399999.");
        assertThatThrownBy(() -> convert(DataTypes.TIME(), 86_400_000))
                .isInstanceOf(TableRuntimeException.class);
    }

    @Test
    void testNanosecondTimestampCoversTheWholeLongRange() {
        final DataType type = DataTypes.TIMESTAMP(9);
        final TimestampData min = nanosSinceEpoch(Long.MIN_VALUE);
        final TimestampData max = nanosSinceEpoch(Long.MAX_VALUE);

        assertThat(convert(type, min).getDateTime()).isEqualTo(min.toLocalDateTime());
        assertThat(convert(type, max).getDateTime()).isEqualTo(max.toLocalDateTime());
        final TimestampData belowMin =
                TimestampData.fromEpochMillis(min.getMillisecond(), min.getNanoOfMillisecond() - 1);
        assertThatThrownBy(() -> convert(type, belowMin))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("only covers 1677-09-21 to 2262-04-11");
        final TimestampData aboveMax =
                TimestampData.fromEpochMillis(max.getMillisecond(), max.getNanoOfMillisecond() + 1);
        assertThatThrownBy(() -> convert(type, aboveMax))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("only covers 1677-09-21 to 2262-04-11");
    }

    @Test
    void testCreateRejectsTypesWithoutVariantKind() {
        assertThatThrownBy(
                        () ->
                                ToVariantConverter.create(
                                        DataTypes.MULTISET(DataTypes.INT()).getLogicalType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot cast MULTISET<INT> to VARIANT.");
        assertThatThrownBy(
                        () ->
                                ToVariantConverter.create(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())
                                                .getLogicalType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageEndingWith("A MAP key must be a character string.");
    }

    @Test
    void testConverterSurvivesJavaSerialization() throws Exception {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("leaves", DataTypes.of(LEAVES)),
                                FIELD("list", DataTypes.ARRAY(DataTypes.INT())),
                                FIELD("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                FIELD("nothing", DataTypes.NULL()),
                                FIELD("variant", DataTypes.VARIANT()),
                                FIELD("order", DataTypes.of(ORDER)))
                        .getLogicalType();
        final GenericRowData row =
                GenericRowData.of(
                        leaves(),
                        new GenericArrayData(new Object[] {1, null}),
                        mapOf(new Object[] {StringData.fromString("k")}, new Object[] {2}),
                        null,
                        Variant.newBuilder().of("embedded"),
                        GenericRowData.of(1, 42));
        final ToVariantConverter converter = ToVariantConverter.create(type);

        assertThat(InstantiationUtil.clone(converter).convert(row))
                .isEqualTo(converter.convert(row));
    }

    private static GenericRowData leaves() {
        return GenericRowData.of(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                1.5f,
                2.5d,
                DecimalData.fromBigDecimal(new BigDecimal("12.50"), 10, 2),
                StringData.fromString("ab  "),
                new byte[] {(byte) 0xCA, (byte) 0xFE},
                (int) LocalDate.of(2026, 9, 25).toEpochDay(),
                LocalTime.of(10, 15, 30).toSecondOfDay() * 1000,
                TimestampData.fromLocalDateTime(DATE_TIME),
                TimestampData.fromLocalDateTime(DATE_TIME),
                TimestampData.fromEpochMillis(0),
                TimestampData.fromEpochMillis(0, 1),
                UuidUtils.toBytes(UUID_VALUE));
    }

    private static TimestampData nanosSinceEpoch(long nanos) {
        return TimestampData.fromEpochMillis(
                Math.floorDiv(nanos, 1_000_000L), (int) Math.floorMod(nanos, 1_000_000L));
    }

    /** Places the bytes inside a larger segment, the way a row field points into its row. */
    private static StringData binaryString(final byte[] utf8) {
        final byte[] row = new byte[utf8.length + 2];
        System.arraycopy(utf8, 0, row, 1, utf8.length);
        return BinaryStringData.fromAddress(
                new MemorySegment[] {MemorySegmentFactory.wrap(row)}, 1, utf8.length);
    }

    private static StringData offHeapString(final byte[] utf8) {
        final MemorySegment segment =
                MemorySegmentFactory.wrapOffHeapMemory(ByteBuffer.allocateDirect(utf8.length + 2));
        segment.put(1, utf8);
        return BinaryStringData.fromAddress(new MemorySegment[] {segment}, 1, utf8.length);
    }

    /** Places the bytes in the first of two segments, the way a short field sits in a large row. */
    private static StringData firstOfTwoSegments(final byte[] utf8) {
        final byte[] first = new byte[utf8.length + 2];
        System.arraycopy(utf8, 0, first, 1, utf8.length);
        final MemorySegment[] segments = {
            MemorySegmentFactory.wrap(first), MemorySegmentFactory.wrap(new byte[first.length])
        };
        return BinaryStringData.fromAddress(segments, 1, utf8.length);
    }

    /** Splits the bytes over two segments of the same size, the way a large row spans pages. */
    private static StringData splitString(final byte[] utf8) {
        final int segmentSize = utf8.length / 2 + 1;
        final byte[] row = new byte[2 * segmentSize];
        System.arraycopy(utf8, 0, row, 1, utf8.length);
        final MemorySegment[] segments = {
            MemorySegmentFactory.wrap(Arrays.copyOfRange(row, 0, segmentSize)),
            MemorySegmentFactory.wrap(Arrays.copyOfRange(row, segmentSize, row.length))
        };
        return BinaryStringData.fromAddress(segments, 1, utf8.length);
    }

    private static Variant convert(DataType dataType, Object value) {
        return convert(dataType.getLogicalType(), value);
    }

    private static String toJson(DataType dataType, Object value) {
        return convert(dataType, value).toJson();
    }

    private static Variant convert(LogicalType type, Object value) {
        return ToVariantConverter.create(type).convert(value);
    }

    private static MapData mapOf(Object[] keys, Object[] values) {
        return new MapData() {
            @Override
            public int size() {
                return keys.length;
            }

            @Override
            public ArrayData keyArray() {
                return new GenericArrayData(keys);
            }

            @Override
            public ArrayData valueArray() {
                return new GenericArrayData(values);
            }
        };
    }
}
