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

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.types.variant.BinaryVariantUtil.BINARY_SEARCH_THRESHOLD;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryVariantTest {

    // U+E000 is EE 80 80 in UTF-8 and U+1F600 is F0 9F 98 80, so UTF-8 byte order puts U+E000
    // first. UTF-16 order compares E000 with the surrogate D83D and puts U+1F600 first.
    private static final String BMP_KEY = "\uE000";
    private static final String SUPPLEMENTARY_KEY = "\uD83D\uDE00";

    private BinaryVariantBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new BinaryVariantBuilder();
    }

    @Test
    void testScalarVariant() {

        assertThat(builder.of((byte) 10).isPrimitive()).isTrue();
        assertThat(builder.of((byte) 10).isNull()).isFalse();
        assertThat(builder.of((byte) 10).isArray()).isFalse();
        assertThat(builder.of((byte) 10).isObject()).isFalse();
        assertThat(builder.of((byte) 10).getType()).isEqualTo(Variant.Type.TINYINT);

        assertThat(builder.of((byte) 10).getByte()).isEqualTo((byte) 10);
        assertThat(builder.of((byte) 10).get()).isEqualTo((byte) 10);
        assertThat((byte) builder.of((byte) 10).getAs()).isEqualTo((byte) 10);

        assertThat(builder.of((short) 10).getShort()).isEqualTo((short) 10);
        assertThat(builder.of((short) 10).get()).isEqualTo((short) 10);

        assertThat(builder.of(10).getInt()).isEqualTo(10);
        assertThat(builder.of(10).get()).isEqualTo(10);

        assertThat(builder.of(10L).getLong()).isEqualTo(10L);
        assertThat(builder.of(10L).get()).isEqualTo(10L);

        assertThat(builder.of(10.0).getDouble()).isEqualTo(10.0d);
        assertThat(builder.of(10.0).get()).isEqualTo(10.0d);

        assertThat(builder.of(10.0f).getFloat()).isEqualTo(10.0f);
        assertThat(builder.of(10.0f).get()).isEqualTo(10.0f);

        assertThat(builder.of("hello").getString()).isEqualTo("hello");
        assertThat(builder.of("hello").get()).isEqualTo("hello");

        assertThat(builder.of("hello".getBytes()).getBytes()).isEqualTo("hello".getBytes());
        assertThat(builder.of("hello".getBytes()).get()).isEqualTo("hello".getBytes());

        assertThat(builder.of(true).getBoolean()).isTrue();
        assertThat(builder.of(true).get()).isEqualTo(true);

        assertThat(builder.of(BigDecimal.valueOf(100)).getDecimal())
                .isEqualByComparingTo(BigDecimal.valueOf(100));
        assertThat((BigDecimal) builder.of(BigDecimal.valueOf(100)).get())
                .isEqualByComparingTo(BigDecimal.valueOf(100));

        Instant instant = Instant.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(instant).getInstant()).isEqualTo(instant);
        assertThat(builder.of(instant).get()).isEqualTo(instant);

        LocalDateTime localDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(localDateTime).getDateTime()).isEqualTo(localDateTime);
        assertThat(builder.of(localDateTime).get()).isEqualTo(localDateTime);

        LocalDate localDate = LocalDate.now();
        assertThat(builder.of(localDate).getDate()).isEqualTo(localDate);
        assertThat(builder.of(localDate).get()).isEqualTo(localDate);

        LocalTime localTime = LocalTime.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(localTime).getTime()).isEqualTo(localTime);
        assertThat(builder.of(localTime).get()).isEqualTo(localTime);

        UUID uuid = UUID.randomUUID();
        assertThat(builder.of(uuid).getUuid()).isEqualTo(uuid);
        assertThat(builder.of(uuid).get()).isEqualTo(uuid);

        assertThat(builder.ofNull().get()).isEqualTo(null);
        assertThat(builder.ofNull().isNull()).isTrue();
    }

    @Test
    void testDecimalWithNegativeScale() {
        Variant variant = builder.of(new BigDecimal("-1e5"));
        assertThat(variant.getType()).isSameAs(Variant.Type.DECIMAL);
        assertThat(variant.getDecimal()).isEqualByComparingTo("-100000");
        assertThat(variant.toJson()).isEqualTo("-100000");

        // 38 digits after rescaling
        assertThat(builder.of(new BigDecimal("1e37")).getDecimal()).isEqualByComparingTo("1e37");
        assertThat(builder.of(new BigDecimal("0e999999999")).getDecimal())
                .isEqualByComparingTo(BigDecimal.ZERO);

        // getDecimal() strips trailing zeros, so its result can have a negative scale
        BigDecimal stripped = builder.of(BigDecimal.valueOf(100)).getDecimal();
        assertThat(stripped.scale()).isNegative();
        assertThat(builder.of(stripped).getDecimal()).isEqualByComparingTo("100");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "12345678901234567890123456789012345678",
                "-0.12345678901234567890123456789012345678"
            })
    void testDecimalWithMaxPrecisionAndScale(final String decimal) {
        assertThat(builder.of(new BigDecimal(decimal)).getDecimal()).isEqualByComparingTo(decimal);
    }

    @ParameterizedTest
    @CsvSource({
        // precision 39
        "123456789012345678901234567890123456789, 39, 0",
        // scale 39
        "0.000000000000000000000000000000000000001, 1, 39",
        // 39 digits after rescaling the negative scale
        "1e38, 1, -38",
        "-1e999999999, 1, -999999999",
        // the scale does not fit into a byte
        "1e2147483647, 1, -2147483647"
    })
    void testDecimalOutOfRange(final String decimal, final int precision, final int scale) {
        assertThatThrownBy(() -> builder.of(new BigDecimal(decimal)))
                .isInstanceOf(VariantTypeException.class)
                .hasMessage(
                        "Decimal with precision %d and scale %d is outside the range supported by "
                                + "variant decimals. After rescaling a negative scale to 0, the "
                                + "precision and scale must not exceed 38.",
                        precision, scale);
    }

    @Test
    void testNanosecondPrecisionVariant() {
        // Microsecond-precision values keep using the compact TIMESTAMP/TIMESTAMP_LTZ encoding,
        // matching the pre-existing on-wire format.
        Instant microInstant = Instant.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(microInstant).getType()).isEqualTo(Variant.Type.TIMESTAMP_LTZ);
        assertThat(builder.of(microInstant).getInstant()).isEqualTo(microInstant);

        LocalDateTime microLocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(microLocalDateTime).getType()).isEqualTo(Variant.Type.TIMESTAMP);
        assertThat(builder.of(microLocalDateTime).getDateTime()).isEqualTo(microLocalDateTime);

        // Sub-microsecond precision switches to the nanosecond encoding instead of truncating,
        // but getInstant()/getDateTime() still work regardless of which encoding was picked.
        Instant nanoInstant = Instant.now().truncatedTo(ChronoUnit.MICROS).plusNanos(123);
        Variant instantVariant = builder.of(nanoInstant);
        assertThat(instantVariant.getType()).isEqualTo(Variant.Type.TIMESTAMP_LTZ_NS);
        assertThat(instantVariant.getInstant()).isEqualTo(nanoInstant);
        assertThat(instantVariant.get()).isEqualTo(nanoInstant);

        LocalDateTime nanoLocalDateTime = LocalDateTime.now().withNano(123456789);
        Variant dateTimeVariant = builder.of(nanoLocalDateTime);
        assertThat(dateTimeVariant.getType()).isEqualTo(Variant.Type.TIMESTAMP_NS);
        assertThat(dateTimeVariant.getDateTime()).isEqualTo(nanoLocalDateTime);
        assertThat(dateTimeVariant.get()).isEqualTo(nanoLocalDateTime);
    }

    @Test
    void testNanosecondTimestampPrecisionRange() {
        // Nanosecond timestamps only span +/-292 years around 1970, beyond that must fail with
        // proper exception
        LocalDateTime outOfRangeLocalDateTime = LocalDateTime.of(2300, 1, 1, 0, 0, 0, 1);
        assertThatThrownBy(() -> builder.of(outOfRangeLocalDateTime))
                .isInstanceOf(VariantTypeException.class)
                .hasMessageContaining("nanosecond precision");

        Instant outOfRangeInstant = outOfRangeLocalDateTime.toInstant(ZoneOffset.UTC);
        assertThatThrownBy(() -> builder.of(outOfRangeInstant))
                .isInstanceOf(VariantTypeException.class)
                .hasMessageContaining("nanosecond precision");
    }

    @Test
    void testMicrosecondTimestampPrecisionRange() {
        // Microsecond precision spans = +/-292.000 years around 1970
        LocalDateTime inRangeLocalDateTime = LocalDateTime.of(150_000, 1, 1, 0, 0, 0, 0);
        Variant inRangeLocalDateTimeVariant = builder.of(inRangeLocalDateTime);
        assertThat(inRangeLocalDateTimeVariant.getType()).isEqualTo(Variant.Type.TIMESTAMP);
        assertThat(inRangeLocalDateTimeVariant.getDateTime()).isEqualTo(inRangeLocalDateTime);

        Instant inRangeInstant = inRangeLocalDateTime.toInstant(ZoneOffset.UTC);
        Variant inRangeInstantVariant = builder.of(inRangeInstant);
        assertThat(inRangeInstantVariant.getType()).isEqualTo(Variant.Type.TIMESTAMP_LTZ);
        assertThat(inRangeInstantVariant.getInstant()).isEqualTo(inRangeInstant);

        // beyond that must fail with proper exception
        LocalDateTime outOfRangeLocalDateTime = LocalDateTime.of(350_000, 1, 1, 0, 0, 0, 0);
        assertThatThrownBy(() -> builder.of(outOfRangeLocalDateTime))
                .isInstanceOf(VariantTypeException.class)
                .hasMessageContaining("microsecond precision");

        Instant outOfRangeInstant = outOfRangeLocalDateTime.toInstant(ZoneOffset.UTC);
        assertThatThrownBy(() -> builder.of(outOfRangeInstant))
                .isInstanceOf(VariantTypeException.class)
                .hasMessageContaining("microsecond precision");
    }

    @Test
    void testTimeSubMicrosecondTruncation() {
        // A sub-microsecond LocalTime silently loses precision below the microsecond.
        // TIME has no nanosecond-precision counterpart in the variant spec.
        LocalTime nanoTime = LocalTime.of(23, 59, 59, 999999999);
        assertThat(builder.of(nanoTime).getTime()).isEqualTo(LocalTime.of(23, 59, 59, 999999000));
    }

    @Test
    void testArrayVariant() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Variant variant =
                builder.array()
                        .add(builder.of(1))
                        .add(builder.of("hello"))
                        .add(builder.of(now))
                        .add(builder.array().add(builder.of("hello2")).add(builder.of(10f)).build())
                        .add(builder.ofNull())
                        .build();

        assertThat(variant.isArray()).isTrue();
        assertThat(variant.isPrimitive()).isFalse();
        assertThat(variant.isObject()).isFalse();
        assertThat(variant.getType()).isEqualTo(Variant.Type.ARRAY);
        assertThat(variant.getArraySize()).isEqualTo(5);

        assertThat(variant.getElement(-1)).isNull();
        assertThat(variant.getElement(0).getInt()).isEqualTo(1);
        assertThat(variant.getElement(1).getString()).isEqualTo("hello");
        assertThat(variant.getElement(2).getInstant()).isEqualTo(now);
        assertThat(variant.getElement(3).getElement(0).getString()).isEqualTo("hello2");
        assertThat(variant.getElement(3).getElement(1).getFloat()).isEqualTo(10f);
        assertThat(variant.getElement(4).isNull()).isTrue();
        assertThat(variant.getElement(5)).isNull();
    }

    @Test
    void testObjectVariant() {
        Variant variant =
                builder.object()
                        .add(
                                "list",
                                builder.array().add(builder.of("hello")).add(builder.of(1)).build())
                        .add(
                                "object",
                                builder.object()
                                        .add("ss", builder.of((short) 1))
                                        .add("ff", builder.of(10.0f))
                                        .build())
                        .add("bb", builder.of((byte) 10))
                        .build();

        assertThat(variant.isArray()).isFalse();
        assertThat(variant.isPrimitive()).isFalse();
        assertThat(variant.isObject()).isTrue();
        assertThat(variant.getType()).isEqualTo(Variant.Type.OBJECT);
        assertThat(variant.getFieldNames()).containsExactlyInAnyOrder("list", "object", "bb");

        assertThat(variant.getField("list").isArray()).isTrue();
        assertThat(variant.getField("list").getArraySize()).isEqualTo(2);
        assertThat(variant.getField("list").getElement(0).getString()).isEqualTo("hello");
        assertThat(variant.getField("list").getElement(1).getInt()).isEqualTo(1);

        assertThat(variant.getField("object").isObject()).isTrue();
        assertThat(variant.getField("object").getFieldNames())
                .containsExactlyInAnyOrder("ss", "ff");
        assertThat(variant.getField("object").getField("ss").getShort()).isEqualTo((short) 1);
        assertThat(variant.getField("object").getField("ff").getFloat()).isEqualTo((10.0f));

        assertThat(variant.getField("bb").getByte()).isEqualTo((byte) 10);
        assertThat(variant.getField("non_exist")).isNull();

        BinaryVariantBuilder.VariantObjectBuilder objectBuilder = builder.object();

        for (int i = 0; i < 100; i++) {
            objectBuilder.add(String.valueOf(i), builder.of(i));
        }
        variant = objectBuilder.build();
        for (int i = 0; i < 100; i++) {
            assertThat(variant.getField(String.valueOf(i)).getInt()).isEqualTo(i);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("duplicateKeys")
    void testDuplicatedKeyObjectVariant(
            final String description,
            final String key,
            final String duplicate,
            final List<String> fieldNames) {
        assertThatThrownBy(
                        () ->
                                builder.object(false)
                                        .add(key, builder.of((byte) 10))
                                        .add(duplicate, builder.of("hello"))
                                        .build())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("VARIANT_DUPLICATE_KEY");

        Variant variant =
                builder.object(true)
                        .add(key, builder.of((byte) 10))
                        .add(duplicate, builder.of("hello"))
                        .add("k1", builder.of(10))
                        .build();

        assertThat(variant.getFieldNames()).containsExactlyElementsOf(fieldNames);
        assertThat(variant.getField(key).getString()).isEqualTo("hello");
        assertThat(variant.getField("k1").getInt()).isEqualTo(10);
    }

    private static Stream<Arguments> duplicateKeys() {
        return Stream.of(
                Arguments.of("same key", "k", "k", List.of("k", "k1")),
                // UTF-8 has no unpaired surrogates, so the builder stores both keys as "k?".
                Arguments.of("same UTF-8 bytes", "k?", "k\uD800", List.of("k1", "k?")));
    }

    @Test
    void testObjectFieldsAreSortedByUtf8Bytes() {
        final Variant object =
                builder.object()
                        .add(SUPPLEMENTARY_KEY, builder.of(1))
                        .add(BMP_KEY, builder.of(2))
                        .build();

        assertThat(object.getFieldNames()).containsExactly(BMP_KEY, SUPPLEMENTARY_KEY);
    }

    @Test
    void testToJsonScalar() {
        Instant instant = Instant.EPOCH;
        LocalDateTime localDateTime = LocalDateTime.of(2000, 1, 1, 0, 0);
        LocalDate localDate = LocalDate.of(2000, 1, 1);
        LocalTime localTime = LocalTime.of(13, 45, 30, 123456789);
        Instant nanoInstant = Instant.EPOCH.plusNanos(123456789);
        LocalDateTime nanoLocalDateTime = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 123456789);
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

        assertThat(builder.of((byte) 1).toJson()).isEqualTo("1");
        assertThat(builder.of((short) 1).toJson()).isEqualTo("1");
        assertThat(builder.of(1L).toJson()).isEqualTo("1");
        assertThat(builder.of(1).toJson()).isEqualTo("1");
        assertThat(builder.of("hello").toJson()).isEqualTo("\"hello\"");
        assertThat(builder.of(true).toJson()).isEqualTo("true");
        assertThat(builder.of(10.0f).toJson()).isEqualTo("10.0");
        assertThat(builder.of(10.0d).toJson()).isEqualTo("10.0");
        assertThat(builder.of(BigDecimal.valueOf(100)).toJson()).isEqualTo("100");
        assertThat(builder.of(instant).toJson()).isEqualTo("\"1970-01-01T00:00:00Z\"");
        assertThat(builder.of(localDateTime).toJson()).isEqualTo("\"2000-01-01T00:00:00\"");
        assertThat(builder.of(localDate).toJson()).isEqualTo("\"2000-01-01\"");
        assertThat(builder.of(localTime).toJson()).isEqualTo("\"13:45:30.123456\"");
        assertThat(builder.of(nanoInstant).toJson())
                .isEqualTo("\"1970-01-01T00:00:00.123456789Z\"");
        assertThat(builder.of(nanoLocalDateTime).toJson())
                .isEqualTo("\"2000-01-01T00:00:00.123456789\"");
        assertThat(builder.of("hello".getBytes()).toJson()).isEqualTo("\"aGVsbG8=\"");
        assertThat(builder.of(uuid).toJson()).isEqualTo("\"00112233-4455-6677-8899-aabbccddeeff\"");
        assertThat(builder.ofNull().toJson()).isEqualTo("null");
    }

    @Test
    void testUuidDecodeFromSpecBytes() {
        // Interop check against the shared variant wire format: this is the exact byte sequence
        // from Iceberg's TestSerializedPrimitives#testUUID (primitive header for type 20 followed
        // by 16 big-endian UUID bytes). Decoding it must produce the same UUID, which proves Flink
        // reads variants written by other implementations of the spec.
        // https://github.com/apache/iceberg/blob/9da109dd2537e77e8e5034068933575aa4e235ff/api/src/test/java/org/apache/iceberg/variants/TestSerializedPrimitives.java#L586
        byte[] value = {
            BinaryVariantUtil.primitiveHeader(BinaryVariantUtil.UUID),
            (byte) 0xf2,
            0x4f,
            (byte) 0x9b,
            0x64,
            (byte) 0x81,
            (byte) 0xfa,
            0x49,
            (byte) 0xd1,
            (byte) 0xb7,
            0x4e,
            (byte) 0x8c,
            0x09,
            (byte) 0xa6,
            (byte) 0xe3,
            0x1c,
            0x56
        };
        // A primitive carries no dictionary keys, so reuse an empty metadata block.
        byte[] metadata = ((BinaryVariant) builder.of(0)).getMetadata();
        Variant variant = new BinaryVariant(value, metadata);

        UUID expected = UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56");
        assertThat(variant.getType()).isEqualTo(Variant.Type.UUID);
        assertThat(variant.getUuid()).isEqualTo(expected);
        assertThat(variant.get()).isEqualTo(expected);
    }

    @Test
    void testToJsonNested() {
        Variant variant =
                builder.object()
                        .add(
                                "list",
                                builder.array().add(builder.of("hello")).add(builder.of(1)).build())
                        .add(
                                "object",
                                builder.object()
                                        .add("ss", builder.of((short) 1))
                                        .add("ff", builder.of(10.0f))
                                        .build())
                        .build();

        String json = variant.toJson();
        assertThat(json)
                .isEqualTo("{" + "\"list\":[\"hello\",1]," + "\"object\":{\"ff\":10.0,\"ss\":1}}");
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN})
    void testToJsonRejectsNonFiniteDouble(final double nonFinite) {
        assertThatThrownBy(() -> builder.of(nonFinite).toJson())
                .isInstanceOf(VariantTypeException.class)
                .hasMessageContaining("cannot be serialized to JSON");
    }

    @ParameterizedTest
    @ValueSource(floats = {Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN})
    void testToJsonRejectsNonFiniteFloat(final float nonFinite) {
        assertThatThrownBy(() -> builder.of(nonFinite).toJson())
                .isInstanceOf(VariantTypeException.class)
                .hasMessageContaining("cannot be serialized to JSON");
    }

    @Test
    void testNonAsciiStringsAndFieldNames() {
        // Multi-byte code points make the UTF-8 byte length differ from the character count, so a
        // charset mismatch between writing and reading mangles the text instead of preserving it.
        final String nestedKey = "キー";
        final String shortValue = "Grüße, 世界 🚀";
        final String longValue = String.join("", Collections.nCopies(20, "äö🚀"));

        assertThat(longValue.getBytes(StandardCharsets.UTF_8).length)
                .as("long string must not fit into the short string encoding")
                .isGreaterThan(BinaryVariantUtil.MAX_SHORT_STR_SIZE);

        final BinaryVariant variant =
                (BinaryVariant)
                        builder.object()
                                .add("schlüssel", builder.of(shortValue))
                                .add(
                                        nestedKey,
                                        builder.object()
                                                .add("schlüssel", builder.of(longValue))
                                                .build())
                                .build();

        // Reading through the raw binaries is what happens once a variant has been serialized, and
        // it is the only path that decodes the field names from the metadata.
        final BinaryVariant decoded = new BinaryVariant(variant.getValue(), variant.getMetadata());

        assertThat(decoded.getFieldNames()).containsExactlyInAnyOrder("schlüssel", nestedKey);
        assertThat(decoded.getField("schlüssel").getString()).isEqualTo(shortValue);
        assertThat(decoded.getField(nestedKey).getFieldNames()).containsExactly("schlüssel");
        assertThat(decoded.getField(nestedKey).getField("schlüssel").getString())
                .isEqualTo(longValue);
        assertThat(decoded.toJson())
                .isEqualTo(
                        "{\""
                                + "schlüssel"
                                + "\":\""
                                + shortValue
                                + "\",\""
                                + nestedKey
                                + "\":{\""
                                + "schlüssel"
                                + "\":\""
                                + longValue
                                + "\"}}");
    }

    @ParameterizedTest(name = "{0} fields")
    @ValueSource(ints = {1, BINARY_SEARCH_THRESHOLD})
    void testGetFieldWithUnpairedSurrogate(final int size) {
        // UTF-8 has no unpaired surrogates. The builder writes one as '?', and a lookup encodes its
        // key the same way.
        final Variant object =
                objectWithFillerFields(builder, size - 1).add("a\uD800", builder.of(100)).build();

        assertThat(object.getFieldNames()).contains("a?");
        assertThat(object.getField("a\uD800").getInt()).isEqualTo(100);
        assertThat(object.getField("a?").getInt()).isEqualTo(100);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("objectsInBothKeyOrders")
    void testGetFieldWithBinarySearch(final String keyOrder, final Variant object) {
        assertThat(object.getField("é").getInt()).isEqualTo(100);
        assertThat(object.getField("中").getInt()).isEqualTo(200);
        assertThat(object.getField(BMP_KEY).getInt()).isEqualTo(300);
        assertThat(object.getField(SUPPLEMENTARY_KEY).getInt()).isEqualTo(400);
        assertThat(object.getField("\uE001")).isNull();
    }

    private static Stream<Arguments> objectsInBothKeyOrders() {
        final BinaryVariantBuilder builder = new BinaryVariantBuilder();
        final BinaryVariant object =
                (BinaryVariant)
                        objectWithFillerFields(builder, BINARY_SEARCH_THRESHOLD - 4)
                                .add("é", builder.of(100))
                                .add("中", builder.of(200))
                                .add(BMP_KEY, builder.of(300))
                                .add(SUPPLEMENTARY_KEY, builder.of(400))
                                .build();
        // The other keys sort before the last two in both orders, so swapping the last two fields
        // turns UTF-8 byte order into UTF-16 order.
        return Stream.of(
                Arguments.of("UTF-8 byte order", object),
                Arguments.of("UTF-16 order", swapLastTwoFields(object)));
    }

    @Test
    void testGetFieldWithEmptyKey() {
        final Variant object =
                builder.object().add("", builder.of(1)).add("b", builder.of(2)).build();

        assertThat(object.getField("").getInt()).isEqualTo(1);
        assertThat(object.getField("b").getInt()).isEqualTo(2);
    }

    @Test
    void testGetFieldWithMalformedUtf8Key() {
        final BinaryVariant object =
                (BinaryVariant) builder.object().add("a", builder.of(1)).build();
        // The key is the last metadata byte. 0xFF never occurs in UTF-8, so no lookup key encodes
        // to it, even though getFieldNames decodes it to U+FFFD.
        final byte[] metadata = object.getMetadata().clone();
        metadata[metadata.length - 1] = (byte) 0xFF;
        final Variant malformed = new BinaryVariant(object.getValue(), metadata);

        assertThat(malformed.getFieldNames()).containsExactly("�");
        assertThat(malformed.getField("�")).isNull();
    }

    @Test
    void testGetFieldWithGetterWhenIndexChanges() {
        final List<Variant> objects =
                List.of(
                        builder.object().add("a", builder.of(1)).add("user", builder.of(2)).build(),
                        builder.object().add("a", builder.of(3)).add("user", builder.of(4)).build(),
                        builder.object().add("user", builder.of(5)).build(),
                        builder.object().add("a", builder.of(6)).add("b", builder.of(7)).build(),
                        builder.object().build(),
                        builder.object()
                                .add("a", builder.of(8))
                                .add("user", builder.of(9))
                                .build());

        final VariantFieldGetter userFieldGetter = new VariantFieldGetter("user");
        final List<Integer> actualValues = new ArrayList<>();
        for (final Variant object : objects) {
            final Variant field = userFieldGetter.getField(object);
            actualValues.add(field == null ? null : field.getInt());
        }

        assertThat(actualValues).containsExactly(2, 4, 5, null, null, 9);
    }

    @Test
    void testVariantException() {
        assertThatThrownBy(() -> new BinaryVariant(new byte[0], new byte[0]))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("MALFORMED_VARIANT");

        byte[] meta = new byte[1];
        meta[0] = (byte) 0x02;
        assertThatThrownBy(() -> new BinaryVariant(new byte[1], meta))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("MALFORMED_VARIANT");

        byte[] oversize = new byte[0xFFFFFF + 2];
        meta[0] = (byte) 0x01;
        oversize[0] = (byte) 0x01;
        assertThatThrownBy(() -> new BinaryVariant(oversize, meta))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("VARIANT_CONSTRUCTOR_SIZE_LIMIT");

        assertThatThrownBy(() -> new BinaryVariant(new byte[1], oversize))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("VARIANT_CONSTRUCTOR_SIZE_LIMIT");
    }

    @Test
    void testGetThrowException() {
        Variant variant = builder.of(10f);
        assertThatThrownBy(variant::getDouble)
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("Expected type DOUBLE but got FLOAT");
    }

    @Test
    void testUuidGetThrowException() {
        // Reading a UUID from a non-UUID variant, and reading another type from a UUID variant,
        // must both fail with a type exception.
        assertThatThrownBy(builder.of(10)::getUuid)
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("Expected type UUID but got INT");

        assertThatThrownBy(builder.of(UUID.randomUUID())::getString)
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("Expected type STRING but got UUID");

        // A UUID header followed by fewer than 16 bytes is malformed and must be rejected
        byte[] truncated = {
            BinaryVariantUtil.primitiveHeader(BinaryVariantUtil.UUID), 0x00, 0x01, 0x02, 0x03
        };
        assertThatThrownBy(() -> BinaryVariantUtil.getUuid(truncated, 0))
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("MALFORMED_VARIANT");
    }

    @Test
    void testJavaSerialization() throws Exception {
        Variant variant =
                builder.object()
                        .add("i", builder.of(1))
                        .add("nested", builder.array().add(builder.of("v")).build())
                        .build();
        assertThat(CommonTestUtils.createCopySerializable(variant)).isEqualTo(variant);

        // a sub-variant is addressed by a position into the value binary of the enclosing document
        Variant subVariant = variant.getField("nested");
        assertThat(((BinaryVariant) subVariant).getPos()).isGreaterThan(0);
        assertThat(CommonTestUtils.createCopySerializable(subVariant)).isEqualTo(subVariant);
    }

    /** Starts an object with {@code count} fields named k00, k01, and so on. */
    private static BinaryVariantBuilder.VariantObjectBuilder objectWithFillerFields(
            final BinaryVariantBuilder builder, final int count) {
        final BinaryVariantBuilder.VariantObjectBuilder object = builder.object();
        for (int i = 0; i < count; i++) {
            object.add(String.format("k%02d", i), builder.of(i));
        }
        return object;
    }

    /** Swaps the ids and offsets of the last two fields, which keeps each value with its key. */
    private static BinaryVariant swapLastTwoFields(final BinaryVariant object) {
        final byte[] value = object.getValue().clone();
        handleObject(
                value,
                0,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    swapBytes(value, idStart + idSize * (size - 2), idSize);
                    swapBytes(value, offsetStart + offsetSize * (size - 2), offsetSize);
                    return null;
                });
        return new BinaryVariant(value, object.getMetadata());
    }

    /** Swaps the {@code length} bytes at {@code start} with the {@code length} bytes after them. */
    private static void swapBytes(final byte[] bytes, final int start, final int length) {
        for (int i = start; i < start + length; i++) {
            final byte b = bytes[i];
            bytes[i] = bytes[i + length];
            bytes[i + length] = b;
        }
    }
}
