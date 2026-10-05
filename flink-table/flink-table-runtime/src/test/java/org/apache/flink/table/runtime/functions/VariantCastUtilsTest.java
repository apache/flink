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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.runtime.functions.VariantCastUtils.MAX_PAYLOAD_BYTES;
import static org.apache.flink.table.runtime.functions.VariantCastUtils.fromBytes;
import static org.apache.flink.table.runtime.functions.VariantCastUtils.fromDecimal;
import static org.apache.flink.table.runtime.functions.VariantCastUtils.fromString;
import static org.apache.flink.table.runtime.functions.VariantCastUtils.toPrintString;
import static org.apache.flink.types.variant.BinaryVariantUtil.primitiveHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.shortStrHeader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VariantCastUtilsTest {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final VariantBuilder BUILDER = Variant.newBuilder();

    private static final String REPLACEMENT_CHARACTER = "\uFFFD";

    @Test
    void testCastToVariantHoldsUpToTheSizeLimit() {
        assertThat(fromBytes(new byte[MAX_PAYLOAD_BYTES]).getBytes()).hasSize(MAX_PAYLOAD_BYTES);
        assertThat(fromString(StringData.fromString("x".repeat(MAX_PAYLOAD_BYTES))).getString())
                .hasSize(MAX_PAYLOAD_BYTES);
    }

    @Test
    void testCastToVariantRejectsValuesOverTheSizeLimit() {
        assertThatThrownBy(() -> fromBytes(new byte[MAX_PAYLOAD_BYTES + 1]))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a binary value of 16777212 bytes to VARIANT. A VARIANT is "
                                + "limited to 16 MiB, so a string or binary value can have at "
                                + "most 16777211 bytes.");
        assertThatThrownBy(
                        () -> fromString(StringData.fromString("x".repeat(MAX_PAYLOAD_BYTES + 1))))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith("Cannot cast a string value of 16777212 bytes to VARIANT.");
        final byte[] tooLong = new byte[MAX_PAYLOAD_BYTES + 1];
        Arrays.fill(tooLong, (byte) 'x');
        assertThatThrownBy(() -> fromString(binaryString(tooLong)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith("Cannot cast a string value of 16777212 bytes to VARIANT.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "hello", "Grüße, 世界 🚀"})
    void testCastStringToVariantStoresItsUtf8Bytes(final String str) {
        assertThat(fromString(binaryString(str.getBytes(UTF_8)))).isEqualTo(BUILDER.of(str));
    }

    @ParameterizedTest
    @MethodSource("invalidUtf8")
    void testCastStringToVariantReplacesInvalidUtf8(final byte[] invalid) {
        final Variant variant = fromString(binaryString(invalid));

        assertThat(variant.getString()).contains(REPLACEMENT_CHARACTER);
        assertThat(variant).isEqualTo(BUILDER.of(binaryString(invalid).toString()));
    }

    @Test
    void testCastStringToVariantEncodesAJavaStringWithoutItsBinaryForm() {
        // An unpaired surrogate has no UTF-8 form, so encoding stores '?' in its place.
        final StringData value = StringData.fromString("a\uD800b");

        final Variant variant = fromString(value);

        assertThat(variant).isEqualTo(BUILDER.of(value.toString()));
        assertThat(variant.getString()).isEqualTo("a?b");
        assertThat(((BinaryStringData) value).getBinarySection()).isNull();
    }

    private static Stream<byte[]> invalidUtf8() {
        return Stream.of(
                new byte[] {'a', (byte) 0xFF, 'b'},
                new byte[] {'a', (byte) 0xC3},
                new byte[] {(byte) 0xC0, (byte) 0xAF},
                new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80});
    }

    @ParameterizedTest
    @EnumSource(Layout.class)
    void testCastStringToVariantReadsEverySegmentLayout(final Layout layout) {
        final String valid = "Grüße, 世界 🚀";
        final byte[] invalid = {'a', (byte) 0xFF, 'b'};

        assertThat(fromString(layout.place.apply(valid.getBytes(UTF_8))))
                .isEqualTo(BUILDER.of(layout.place.apply(valid.getBytes(UTF_8)).toString()));
        assertThat(fromString(layout.place.apply(invalid)))
                .isEqualTo(BUILDER.of(layout.place.apply(invalid).toString()));
    }

    /** Where the bytes of a {@link BinaryStringData} live. */
    private enum Layout {
        HEAP_SEGMENT(VariantCastUtilsTest::binaryString),
        OFF_HEAP_SEGMENT(VariantCastUtilsTest::offHeapString),
        FIRST_OF_TWO_SEGMENTS(VariantCastUtilsTest::firstOfTwoSegments),
        ACROSS_TWO_SEGMENTS(VariantCastUtilsTest::splitString),
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
    void testCastDecimalToVariantKeepsUnscaledValueAndScale(
            final BigDecimal decimal, final int precision, final int scale) {
        assertThat(fromDecimal(DecimalData.fromBigDecimal(decimal, precision, scale)))
                .isEqualTo(BUILDER.of(decimal));
    }

    @Test
    void testPrintRendersLikeCast() {
        final Variant variant =
                BUILDER.object()
                        .add(
                                "list",
                                BUILDER.array().add(BUILDER.of("x")).add(BUILDER.ofNull()).build())
                        .build();

        assertThat(toPrintString(variant, UTC))
                .isEqualTo("{list=[x, NULL]}")
                .isEqualTo(
                        VariantCastUtils.toStringValue(variant, UTC, Integer.MAX_VALUE, false)
                                .toString());
    }

    @Test
    void testPrintShowsValuesTheCastRejects() {
        assertThat(toPrintString(BUILDER.of(Double.NaN), UTC)).isEqualTo("NaN");
        assertThat(toPrintString(BUILDER.ofNull(), UTC)).isEqualTo("NULL");
        assertThat(toPrintString(BUILDER.of(new byte[] {(byte) 0xC3, (byte) 0x28}), UTC))
                .isEqualTo("x'c328'");
    }

    @Test
    void testPrintShowsUndecodableNodes() {
        final BinaryVariant ints =
                (BinaryVariant) BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of(2)).build();
        final BinaryVariant strings =
                (BinaryVariant) BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of("xy")).build();

        assertThat(toPrintString(withHeader(ints, primitiveHeader(31)), UTC))
                .isEqualTo("[1, <UNKNOWN>]");
        // Claim a longer string than the buffer holds.
        assertThat(toPrintString(withHeader(strings, shortStrHeader(63)), UTC))
                .isEqualTo("[1, <INVALID>]");
    }

    @Test
    void testPrintShowsTimestampLtzInSessionZone() {
        final Variant variant = BUILDER.of(Instant.parse("2021-09-24T12:34:56.123456Z"));

        assertThat(toPrintString(variant, TimeZone.getTimeZone("Europe/Berlin")))
                .isEqualTo("2021-09-24 14:34:56.123456");
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

    /** Replaces the header of the array's second element. */
    private static Variant withHeader(final BinaryVariant array, final byte header) {
        final byte[] value = array.getValue().clone();
        value[((BinaryVariant) array.getElement(1)).getPos()] = header;
        return new BinaryVariant(value, array.getMetadata());
    }
}
