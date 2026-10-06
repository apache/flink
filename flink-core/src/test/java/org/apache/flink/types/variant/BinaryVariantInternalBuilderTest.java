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

package org.apache.flink.types.variant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_16;
import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL16;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL4;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL8;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_SHORT_STR_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.U32_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.primitiveHeader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryVariantInternalBuilderTest {

    @Test
    void testParseScalarJson() throws IOException {
        assertThat(BinaryVariantInternalBuilder.parseJson("1", false).getByte())
                .isEqualTo((byte) 1);
        short s = (short) (Byte.MAX_VALUE + 1L);
        assertThat(BinaryVariantInternalBuilder.parseJson(String.valueOf(s), false).getShort())
                .isEqualTo(s);
        int i = (int) (Short.MAX_VALUE + 1L);
        assertThat(BinaryVariantInternalBuilder.parseJson(String.valueOf(i), false).getInt())
                .isEqualTo(i);
        long l = Integer.MAX_VALUE + 1L;
        assertThat(BinaryVariantInternalBuilder.parseJson(String.valueOf(l), false).getLong())
                .isEqualTo(l);

        BigDecimal bigDecimal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        assertThat(
                        BinaryVariantInternalBuilder.parseJson(bigDecimal.toPlainString(), false)
                                .getDecimal())
                .isEqualTo(bigDecimal);

        assertThat(BinaryVariantInternalBuilder.parseJson("1.123", false).getDecimal())
                .isEqualTo(BigDecimal.valueOf(1.123));
        assertThat(
                        BinaryVariantInternalBuilder.parseJson(
                                        String.valueOf(Double.MAX_VALUE), false)
                                .getDouble())
                .isEqualTo(Double.MAX_VALUE);

        assertThat(BinaryVariantInternalBuilder.parseJson("\"hello\"", false).getString())
                .isEqualTo("hello");

        assertThat(BinaryVariantInternalBuilder.parseJson("true", false).getBoolean()).isTrue();

        assertThat(BinaryVariantInternalBuilder.parseJson("false", false).getBoolean()).isFalse();

        assertThat(BinaryVariantInternalBuilder.parseJson("null", false).isNull()).isTrue();
    }

    @Test
    void testParseJsonArray() throws IOException {
        BinaryVariant variant = BinaryVariantInternalBuilder.parseJson("[]", false);
        assertThat(variant.getElement(0)).isNull();

        variant = BinaryVariantInternalBuilder.parseJson("[1,\"hello\",3.1, null]", false);
        assertThat(variant.getElement(0).getByte()).isEqualTo((byte) 1);
        assertThat(variant.getElement(1).getString()).isEqualTo("hello");
        assertThat(variant.getElement(2).getDecimal()).isEqualTo(BigDecimal.valueOf(3.1));
        assertThat(variant.getElement(3).isNull()).isTrue();

        variant = BinaryVariantInternalBuilder.parseJson("[1,[\"hello\",[3.1]]]", false);
        assertThat(variant.getElement(0).getByte()).isEqualTo((byte) 1);
        assertThat(variant.getElement(1).getElement(0).getString()).isEqualTo("hello");
        assertThat(variant.getElement(1).getElement(1).getElement(0).getDecimal())
                .isEqualTo(BigDecimal.valueOf(3.1));
    }

    @Test
    void testParseJsonObject() throws IOException {
        BinaryVariant variant = BinaryVariantInternalBuilder.parseJson("{}", false);
        assertThat(variant.getField("a")).isNull();

        variant =
                BinaryVariantInternalBuilder.parseJson(
                        "{\"a\":1,\"b\":\"hello\",\"c\":3.1}", false);

        assertThat(variant.getField("a").getByte()).isEqualTo((byte) 1);
        assertThat(variant.getField("b").getString()).isEqualTo("hello");
        assertThat(variant.getField("c").getDecimal()).isEqualTo(BigDecimal.valueOf(3.1));

        variant =
                BinaryVariantInternalBuilder.parseJson(
                        "{\"a\":1,\"b\":{\"c\":\"hello\",\"d\":[3.1]}}", false);
        assertThat(variant.getField("a").getByte()).isEqualTo((byte) 1);
        assertThat(variant.getField("b").getField("c").getString()).isEqualTo("hello");
        assertThat(variant.getField("b").getField("d").getElement(0).getDecimal())
                .isEqualTo(BigDecimal.valueOf(3.1));

        assertThatThrownBy(
                        () ->
                                BinaryVariantInternalBuilder.parseJson(
                                        "{\"k1\":1,\"k1\":2,\"k2\":1.5}", false))
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("VARIANT_DUPLICATE_KEY");

        variant = BinaryVariantInternalBuilder.parseJson("{\"k1\":1,\"k1\":2,\"k2\":1.5}", true);
        assertThat(variant.getField("k1").getByte()).isEqualTo((byte) 2);
        assertThat(variant.getField("k2").getDecimal()).isEqualTo(BigDecimal.valueOf(1.5));
    }

    @Test
    void testParseJsonWithNonAsciiStringsAndKeys() throws IOException {
        String json = "{\"schlüssel\":\"Grüße, 世界 🚀\",\"キー\":[\"äöü\"]}";

        BinaryVariant variant = BinaryVariantInternalBuilder.parseJson(json, false);

        assertThat(variant.getFieldNames()).containsExactlyInAnyOrder("schlüssel", "キー");
        assertThat(variant.getField("schlüssel").getString()).isEqualTo("Grüße, 世界 🚀");
        assertThat(variant.getField("キー").getElement(0).getString()).isEqualTo("äöü");
        assertThat(variant.toJson()).isEqualTo(json);
    }

    @Test
    void testParseJsonFromUtf8Bytes() throws IOException {
        final String json = "{\"schlüssel\":\"Grüße, 世界 🚀\",\"キー\":[\"äöü\"]}";

        assertThat(BinaryVariantInternalBuilder.parseJson(json.getBytes(UTF_8), false))
                .isEqualTo(BinaryVariantInternalBuilder.parseJson(json, false));
    }

    private static Stream<Arguments> nonUtf8JsonBytes() {
        return Stream.of(
                // Charset detection would read these as UTF-16 or UTF-32, or skip the BOM.
                Arguments.of("trailing NUL", "1\u0000".getBytes(UTF_8)),
                Arguments.of("leading NUL", "\u00001".getBytes(UTF_8)),
                Arguments.of("UTF-8 BOM", "\uFEFF1".getBytes(UTF_8)),
                Arguments.of("UTF-16BE", "{\"a\":1}".getBytes(UTF_16BE)),
                Arguments.of("UTF-16LE", "{\"a\":1}".getBytes(UTF_16LE)),
                Arguments.of("UTF-16 with BOM", "{\"a\":1}".getBytes(UTF_16)),
                Arguments.of("invalid start byte", new byte[] {'"', (byte) 0xFF, '"'}),
                Arguments.of("truncated sequence", new byte[] {'"', 'a', (byte) 0xC3, '"'}),
                Arguments.of(
                        "surrogate code point",
                        new byte[] {'"', (byte) 0xED, (byte) 0xA0, (byte) 0x80, '"'}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonUtf8JsonBytes")
    void testParseJsonRejectsBytesThatAreNotUtf8Json(final String name, final byte[] bytes) {
        assertThatThrownBy(() -> BinaryVariantInternalBuilder.parseJson(bytes, false))
                .isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"NaN", "Infinity", "-Infinity", "1e400", "-1e400"})
    void testParseJsonRejectsNonFiniteNumbers(final String nonFiniteNumber) {
        // NaN and the infinities are not valid JSON; 1e400 is valid JSON but overflows the double
        // range. Both must be rejected so PARSE_JSON errors and TRY_PARSE_JSON returns NULL.
        assertThatThrownBy(() -> BinaryVariantInternalBuilder.parseJson(nonFiniteNumber, false))
                .isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "123456789012345678901234567890123456789",
                "0.000000000000000000000000000000000000001"
            })
    void testParseJsonStoresNumbersOutsideDecimalRangeAsDouble(final String number)
            throws IOException {
        BinaryVariant variant = BinaryVariantInternalBuilder.parseJson(number, false);
        assertThat(variant.getType()).isSameAs(Variant.Type.DOUBLE);
        assertThat(variant.getDouble()).isEqualTo(Double.parseDouble(number));
    }

    @Test
    void testAppendFloat() {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        ArrayList<Float> floatList = new ArrayList<>(Collections.nCopies(25, 4.2f));

        assertThatCode(() -> floatList.forEach(builder::appendFloat)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, MAX_SHORT_STR_SIZE, MAX_SHORT_STR_SIZE + 1})
    void testAppendStringStoresUtf8BytesAsTheyAre(final int length) {
        final String str = "x".repeat(length);
        final byte[] utf8 = str.getBytes(UTF_8);
        // The bytes follow one other byte, so a range that ignores its offset reads the wrong ones.
        final byte[] buffer = new byte[1 + length];
        System.arraycopy(utf8, 0, buffer, 1, length);
        final BinaryVariantInternalBuilder fromRange = new BinaryVariantInternalBuilder(false);
        fromRange.appendString(buffer, 1, length);
        final BinaryVariantInternalBuilder fromArray = new BinaryVariantInternalBuilder(false);
        fromArray.appendString(utf8);

        final BinaryVariant variant = fromRange.build();
        final byte[] value = variant.getValue();
        final int headerSize = 1 + (length > MAX_SHORT_STR_SIZE ? U32_SIZE : 0);
        assertThat(Arrays.copyOfRange(value, headerSize, value.length)).isEqualTo(utf8);
        assertThat(variant.getString()).isEqualTo(str);
        assertThat(variant).isEqualTo(fromArray.build());
    }

    @ParameterizedTest(name = "unscaled={0}, scale={1}")
    @MethodSource("unscaledDecimals")
    void testAppendDecimalFromUnscaledLong(
            final long unscaled, final int scale, final int decimalType) {
        final BigDecimal decimal = BigDecimal.valueOf(unscaled, scale);
        final BinaryVariantInternalBuilder fromLong = new BinaryVariantInternalBuilder(false);
        fromLong.appendDecimal(unscaled, scale);
        final BinaryVariantInternalBuilder fromBigDecimal = new BinaryVariantInternalBuilder(false);
        fromBigDecimal.appendDecimal(decimal);

        final BinaryVariant variant = fromLong.build();
        assertThat(variant).isEqualTo(fromBigDecimal.build());
        assertThat(variant.getValue()[0]).isEqualTo(primitiveHeader(decimalType));
        assertThat(variant.getDecimal()).isEqualByComparingTo(decimal);
    }

    private static Stream<Arguments> unscaledDecimals() {
        return Stream.of(
                Arguments.of(0L, 0, DECIMAL4),
                Arguments.of(999_999_999L, 9, DECIMAL4),
                Arguments.of(-999_999_999L, 9, DECIMAL4),
                Arguments.of(1_000_000_000L, 0, DECIMAL8),
                Arguments.of(-1_000_000_000L, 0, DECIMAL8),
                Arguments.of(1L, 10, DECIMAL8),
                Arguments.of(999_999_999_999_999_999L, 18, DECIMAL8),
                Arguments.of(-999_999_999_999_999_999L, 18, DECIMAL8),
                Arguments.of(1_000_000_000_000_000_000L, 0, DECIMAL16),
                Arguments.of(-1_000_000_000_000_000_000L, 0, DECIMAL16),
                Arguments.of(1L, 19, DECIMAL16),
                Arguments.of(Long.MAX_VALUE, 38, DECIMAL16),
                Arguments.of(Long.MIN_VALUE, 0, DECIMAL16),
                // A negative scale is rescaled to 0.
                Arguments.of(5L, -1, DECIMAL4));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "0",
                "-0",
                "127",
                "128",
                "-129",
                "32768",
                "2147483648",
                "9223372036854775807",
                "-9223372036854775808",
                // Beyond a long: a decimal while it fits 38 digits, then a double.
                "9223372036854775808",
                "-9223372036854775809",
                "123456789012345678901234567890123456789",
                "3.14",
                "-1.5",
                "0.10",
                "0.000000000000000000000000000000000000001",
                "1e10",
                "1E10",
                "1e+10",
                "1e-10",
                "-1.5e3",
                "1e308",
                // Whitespace around a value is valid JSON.
                " 1.5\n"
            })
    void testAppendJsonNumberMatchesParseJson(final String literal) throws IOException {
        final BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendJsonNumber(literal);

        assertThat(builder.build())
                .isEqualTo(BinaryVariantInternalBuilder.parseJson(literal, false));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // Locale-specific separators.
                "1,5",
                "1.000,5",
                "1'000",
                // Accepted by Java's number parsers, but not by JSON.
                "+5",
                "0x1p4",
                "0x1.8p3",
                "10d",
                "1.5f",
                "1" + (char) 0x06F8,
                "NaN",
                "Infinity",
                "-Infinity",
                // Incomplete or malformed.
                "",
                "-",
                "01",
                "-01",
                "1.",
                ".5",
                "1e",
                "1e+",
                "1.5.5",
                "--1",
                // Another JSON value, or more than one.
                "\"5\"",
                "true",
                "null",
                "[1]",
                "5 6",
                // A JSON number outside the range of a double.
                "1e400",
                "-1e400"
            })
    void testAppendJsonNumberRejectsInvalidLiterals(final String literal) {
        final BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);

        assertThatThrownBy(() -> builder.appendJsonNumber(literal)).isInstanceOf(IOException.class);
    }
}
