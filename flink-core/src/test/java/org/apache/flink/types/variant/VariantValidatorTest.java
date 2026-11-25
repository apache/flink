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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.types.variant.BinaryVariantUtil.SIZE_LIMIT;
import static org.apache.flink.types.variant.BinaryVariantUtil.U32_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.arrayHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.shortStrHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.writeLong;
import static org.apache.flink.util.StringUtils.byteToHexString;
import static org.apache.flink.util.StringUtils.hexStringToByte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

class VariantValidatorTest {

    // Metadata without keys, with the keys "a" and "b", with "b" and "a", and with "a" twice.
    private static final String NO_KEYS = "010000";
    private static final String KEYS_A_B = "01020001026162";
    private static final String KEYS_B_A = "01020001026261";
    private static final String KEYS_A_A = "01020001026161";

    // The messages that several malformed variants share, for a problem at byte 0.
    private static final String SCALAR_PAST =
            "MALFORMED_VARIANT: The value at byte 0 ends past its parent or past the end of the "
                    + "value.";
    private static final String CONTAINER_PAST =
            "MALFORMED_VARIANT: The object or array at byte 0 ends past its parent or past the end "
                    + "of the value.";
    private static final String TIME =
            "MALFORMED_VARIANT: The TIME at byte 0 is not within one day.";
    private static final String DECIMAL =
            "MALFORMED_VARIANT: The DECIMAL at byte 0 has more digits or a larger scale than its "
                    + "width allows.";
    private static final String FIELDS_NOT_SORTED =
            "MALFORMED_VARIANT: The fields of the object at byte 0 are not sorted by key, or a key "
                    + "appears twice.";
    private static final String STRING_NOT_UTF8 =
            "MALFORMED_VARIANT: The string at byte 0 is not valid UTF-8.";
    private static final String METADATA_VERSION =
            "MALFORMED_VARIANT: The metadata is empty or its version is not 1.";
    private static final String METADATA_TRUNCATED =
            "MALFORMED_VARIANT: The metadata ends before its key offsets.";
    private static final String KEY_0_NOT_UTF8 =
            "MALFORMED_VARIANT: Key 0 of the metadata is not valid UTF-8.";
    private static final String KEYS_NOT_SORTED =
            "MALFORMED_VARIANT: The metadata says that its keys are sorted, but key 1 doesn't come "
                    + "after the key before it.";
    // Messages with the byte as %d.
    private static final String UNKNOWN_TYPE_21 =
            "UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT: The value at byte %d has the type id 21, which "
                    + "this version doesn't know.";
    private static final String SHARED_BYTES =
            "MALFORMED_VARIANT: The children of the object or array at byte %d share bytes, so "
                    + "that reading the variant reads more bytes than the value has.";

    // The object {"a": true, "b": true} for KEYS_B_A.
    private static final byte[] OBJECT_A_B = hexStringToByte("020201000001020404");

    private final VariantValidator validator = new VariantValidator();

    @ParameterizedTest(name = "{0}")
    @MethodSource("validVariants")
    void testValidVariant(final String name, final BinaryVariant variant) {
        assertThat(validator.isValid(variant)).isTrue();
        assertThatCode(() -> validator.validate(variant)).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedVariants")
    void testMalformedVariant(
            final String name, final byte[] value, final byte[] metadata, final String message) {
        assertThat(validator.isValid(value, metadata)).isFalse();
        assertThatThrownBy(() -> validator.validate(value, metadata))
                .isInstanceOf(VariantTypeException.class)
                .hasMessage(message);
    }

    @Test
    void testTruncatedVariant() {
        final BinaryVariant variant = everyType();
        final byte[] value = variant.getValue();
        for (int length = 1; length < value.length; length++) {
            assertThat(validator.isValid(Arrays.copyOf(value, length), variant.getMetadata()))
                    .as("first %d of %d bytes", length, value.length)
                    .isFalse();
        }
    }

    @Test
    void testChildVariant() {
        final BinaryVariantBuilder builder = new BinaryVariantBuilder();
        final BinaryVariant object =
                (BinaryVariant)
                        builder.object()
                                .add("good", builder.array().add(builder.of(1)).build())
                                .add("bad", builder.of("abc"))
                                .build();
        final byte[] value = object.getValue().clone();
        // An unknown type id in the header of the "bad" field.
        value[((BinaryVariant) object.getField("bad")).getPos()] = 0x54;
        final BinaryVariant corrupted = new BinaryVariant(value, object.getMetadata());

        assertThat(validator.isValid(corrupted)).isFalse();
        assertThat(validator.isValid((BinaryVariant) corrupted.getField("good"))).isTrue();
        assertThat(validator.isValid((BinaryVariant) corrupted.getField("bad"))).isFalse();

        // A position counts from the start of the variant that is checked.
        final int bad = ((BinaryVariant) object.getField("bad")).getPos();
        assertThatThrownBy(() -> validator.validate(corrupted))
                .hasMessage(String.format(UNKNOWN_TYPE_21, bad));
        assertThatThrownBy(() -> validator.validate((BinaryVariant) corrupted.getField("bad")))
                .hasMessage(String.format(UNKNOWN_TYPE_21, 0));
    }

    @Test
    void testDeeplyNestedVariant() {
        // Deep enough that a recursive check overflows any default stack with StackOverflowError.
        final int depth = 100_000;
        // Encodes [[...[null]...]] from the null outwards, with 4-byte offsets at every level.
        final int levelSize = 2 + 2 * U32_SIZE;
        final byte[] value = new byte[depth * levelSize + 1];
        for (int start = 0; start < value.length - 1; start += levelSize) {
            value[start] = arrayHeader(false, U32_SIZE);
            value[start + 1] = 1;
            writeLong(value, start + 2 + U32_SIZE, value.length - start - levelSize, U32_SIZE);
        }

        assertThat(validator.isValid(value, hexStringToByte(NO_KEYS))).isTrue();
        // The same instance still works after the stack grew for the deep variant.
        assertThat(validator.isValid(everyType())).isTrue();
    }

    @Test
    void testKeysOfManyObjects() {
        // The dictionary lists "b" before "a", so the check compares the keys of the first objects
        // directly and sorts the dictionary for the others.
        final int count = 20;
        final byte[] value = objectsWithKeysBA(count);
        final byte[] metadata = hexStringToByte(KEYS_B_A);
        assertThat(validator.isValid(value, metadata)).isTrue();

        for (final int object : new int[] {0, count - 1}) {
            final int ids = 3 + count + OBJECT_A_B.length * object + 2;
            final byte[] swapped = value.clone();
            swapped[ids] = 0;
            swapped[ids + 1] = 1;
            assertThat(validator.isValid(swapped, metadata))
                    .as("keys of object %d in the wrong order", object)
                    .isFalse();
            final byte[] repeated = value.clone();
            repeated[ids + 1] = 1;
            assertThat(validator.isValid(repeated, metadata))
                    .as("key of object %d twice", object)
                    .isFalse();
        }
        // With "a" under two ids, the ranks must treat both ids as the same key.
        final byte[] keysBAA = hexStringToByte("010300010203626161");
        assertThat(validator.isValid(value, keysBAA)).isTrue();
        final int lastIds = 3 + count + OBJECT_A_B.length * (count - 1) + 2;
        final byte[] twoIds = value.clone();
        twoIds[lastIds] = 1;
        twoIds[lastIds + 1] = 2;
        assertThat(validator.isValid(twoIds, keysBAA)).as("one key under two ids").isFalse();

        // The same instance still works after it sorted a dictionary.
        assertThat(validator.isValid(everyType())).isTrue();
    }

    @Test
    void testCorruptedVariants() {
        final BinaryVariant[] corpus = {everyType(), wideArray(), wideObject()};
        final Random random = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            final BinaryVariant original = corpus[random.nextInt(corpus.length)];
            final boolean corruptMetadata = random.nextInt(5) == 0;
            final byte[] value =
                    corruptMetadata ? original.getValue() : corrupt(original.getValue(), random);
            final byte[] metadata =
                    corruptMetadata
                            ? corrupt(original.getMetadata(), random)
                            : original.getMetadata();
            final boolean valid;
            try {
                valid = validator.isValid(value, metadata);
            } catch (Throwable t) {
                throw new AssertionError(describe(i, value, metadata), t);
            }
            final int iteration = i;
            final Throwable thrown = catchThrowable(() -> validator.validate(value, metadata));
            if (valid) {
                assertThat(thrown).as(() -> describe(iteration, value, metadata)).isNull();
                assertReadable(i, value, metadata);
            } else {
                assertThat(thrown)
                        .as(() -> describe(iteration, value, metadata))
                        .isInstanceOf(VariantTypeException.class);
            }
        }
    }

    /** Reads every part of a variant that the check accepted. */
    private static void assertReadable(
            final int iteration, final byte[] value, final byte[] metadata) {
        final String text;
        try {
            // toString reads every node like the getters, and writes a placeholder for a node
            // that fails to decode instead of throwing.
            text = new BinaryVariant(value, metadata).toString();
        } catch (Throwable t) {
            throw new AssertionError(describe(iteration, value, metadata), t);
        }
        if (text.contains("\"<INVALID>\"") || text.contains("\"<UNKNOWN>\"")) {
            fail(describe(iteration, value, metadata) + " reads as " + text);
        }
    }

    private static String describe(final int iteration, final byte[] value, final byte[] metadata) {
        return String.format(
                "Iteration %d, value %s, metadata %s",
                iteration, byteToHexString(value), byteToHexString(metadata));
    }

    private static byte[] corrupt(final byte[] bytes, final Random random) {
        byte[] result = bytes.clone();
        for (int edits = 1 + random.nextInt(3); edits > 0; edits--) {
            final int pos = random.nextInt(result.length);
            switch (random.nextInt(3)) {
                case 0:
                    result[pos] = (byte) random.nextInt(256);
                    break;
                case 1:
                    result = Arrays.copyOf(result, Math.max(1, pos));
                    break;
                default:
                    {
                        final byte[] longer = new byte[result.length + 1];
                        System.arraycopy(result, 0, longer, 0, pos);
                        longer[pos] = (byte) random.nextInt(256);
                        System.arraycopy(result, pos, longer, pos + 1, result.length - pos);
                        result = longer;
                    }
            }
        }
        return result;
    }

    private static Stream<Arguments> validVariants() {
        final BinaryVariantBuilder builder = new BinaryVariantBuilder();
        return Stream.of(
                Arguments.of("every type, nested", everyType()),
                Arguments.of("array with a 4-byte size", wideArray()),
                Arguments.of("object with a 4-byte size", wideObject()),
                Arguments.of("empty object", builder.object().build()),
                Arguments.of("empty array", builder.array().build()),
                Arguments.of("variant null", builder.ofNull()),
                Arguments.of("NaN", builder.of(Double.NaN)),
                Arguments.of("largest DECIMAL4", variant("2000ffc99a3b", NO_KEYS)),
                Arguments.of("smallest DECIMAL4", variant("2000013665c4", NO_KEYS)),
                Arguments.of("largest DECIMAL8", variant("2400ffff63a7b3b6e00d", NO_KEYS)),
                Arguments.of("DECIMAL8 with scale 18", variant("24120100000000000000", NO_KEYS)),
                Arguments.of(
                        "DECIMAL16 with scale 38",
                        variant("282601000000000000000000000000000000", NO_KEYS)),
                Arguments.of("largest DECIMAL16", builder.of(new BigDecimal("9".repeat(38)))),
                Arguments.of(
                        "smallest DECIMAL16", builder.of(new BigDecimal("-" + "9".repeat(38)))),
                Arguments.of("last TIME of the day", builder.of(LocalTime.MAX)),
                Arguments.of("binary that isn't UTF-8", builder.of(new byte[] {(byte) 0xff})),
                Arguments.of(
                        "keys with code points from U+E000",
                        builder.object()
                                .add("\uD800\uDC00", builder.of(1))
                                .add("\uFFFF", builder.of(2))
                                .build()),
                Arguments.of("sorted_strings with sorted keys", variant("00", "11020001026162")),
                Arguments.of("reserved bit in the metadata header", variant("00", "210000")),
                Arguments.of("reserved bit in an object header", variant("820000", NO_KEYS)),
                Arguments.of("reserved bits in an array header", variant("e30000", NO_KEYS)),
                Arguments.of(
                        "shared bytes within the bound",
                        new BinaryVariant(sharedBytes(3), hexStringToByte(NO_KEYS))),
                Arguments.of(
                        "string in an array of one",
                        new BinaryVariant(sharedString(1), hexStringToByte(NO_KEYS))));
    }

    private static Stream<Arguments> malformedVariants() {
        return Stream.of(
                malformed("unknown type id", "54", NO_KEYS, String.format(UNKNOWN_TYPE_21, 0)),
                malformed("long string past the end", "400500000061626364", NO_KEYS, SCALAR_PAST),
                malformed(
                        "long string longer than 2^31 bytes", "400000008000", NO_KEYS, SCALAR_PAST),
                malformed("long string without a length", "40", NO_KEYS, SCALAR_PAST),
                malformed("long string with a cut-off length", "4005", NO_KEYS, SCALAR_PAST),
                malformed("binary without a length", "3c", NO_KEYS, SCALAR_PAST),
                malformed("TIME before midnight", "44ffffffffffffffff", NO_KEYS, TIME),
                malformed("TIME of a whole day", "440060d71d14000000", NO_KEYS, TIME),
                malformed("DECIMAL4 with 10 digits", "200000ca9a3b", NO_KEYS, DECIMAL),
                malformed("DECIMAL4 with scale 10", "200a01000000", NO_KEYS, DECIMAL),
                malformed("DECIMAL8 with 19 digits", "2400000064a7b3b6e00d", NO_KEYS, DECIMAL),
                malformed("DECIMAL8 with -19 digits", "240000009c584c491ff2", NO_KEYS, DECIMAL),
                malformed(
                        "DECIMAL16 with 39 digits",
                        "28000000000040228a097ac4865aa84c3b4b",
                        NO_KEYS,
                        DECIMAL),
                malformed(
                        "DECIMAL16 with -39 digits",
                        "280000000000c0dd75f6853b79a557b3c4b4",
                        NO_KEYS,
                        DECIMAL),
                malformed(
                        "DECIMAL16 of -2^127",
                        "280000000000000000000000000000000080",
                        NO_KEYS,
                        DECIMAL),
                malformed("DECIMAL8 with scale 19", "24130100000000000000", NO_KEYS, DECIMAL),
                malformed(
                        "DECIMAL16 with scale 39",
                        "282701000000000000000000000000000000",
                        NO_KEYS,
                        DECIMAL),
                malformed(
                        "DECIMAL16 just below -10^38 with a low half of 0",
                        "28000000000000000000853b79a557b3c4b4",
                        NO_KEYS,
                        DECIMAL),
                malformed(
                        "field id past the dictionary",
                        "02010200020c01",
                        KEYS_A_B,
                        "MALFORMED_VARIANT: A field of the object at byte 0 refers to a key that "
                                + "the metadata doesn't have."),
                malformed(
                        "object with unsorted keys",
                        "020201000002040c010c02",
                        KEYS_A_B,
                        FIELDS_NOT_SORTED),
                malformed(
                        "object with a key twice",
                        "020200000002040c010c02",
                        KEYS_A_B,
                        FIELDS_NOT_SORTED),
                malformed(
                        "object with a key under two ids",
                        "020200010002040c010c02",
                        KEYS_A_A,
                        FIELDS_NOT_SORTED),
                malformed(
                        "object keys in UTF-16 order",
                        "020200010002040c010c02",
                        "0102000407f0908080efbfbf",
                        FIELDS_NOT_SORTED),
                malformed(
                        "key with decreasing offsets",
                        "02010100020c01",
                        "01020002016162",
                        "MALFORMED_VARIANT: Key 1 of the metadata ends before it starts or past "
                                + "the end of the metadata."),
                malformed(
                        "element offset past the data",
                        "030102020c01",
                        NO_KEYS,
                        "MALFORMED_VARIANT: A child of the object or array at byte 0 starts past "
                                + "the end of its data."),
                malformed(
                        "element past the data of its array",
                        "030100020d616263",
                        NO_KEYS,
                        "MALFORMED_VARIANT: The value at byte 4 ends past its parent or past the "
                                + "end of the value."),
                malformed("array size past the value", "13ffffff7f", NO_KEYS, CONTAINER_PAST),
                malformed("object size past the value", "42ffffff7f", NO_KEYS, CONTAINER_PAST),
                Arguments.of(
                        "shared bytes beyond the bound",
                        sharedBytes(4),
                        hexStringToByte(NO_KEYS),
                        String.format(SHARED_BYTES, 10)),
                Arguments.of(
                        "string shared beyond the bound",
                        sharedString(2),
                        hexStringToByte(NO_KEYS),
                        String.format(SHARED_BYTES, 0)),
                malformed("short string that isn't UTF-8", "0580", NO_KEYS, STRING_NOT_UTF8),
                malformed("short string with a surrogate", "0deda080", NO_KEYS, STRING_NOT_UTF8),
                malformed("long string that isn't UTF-8", "4001000000ff", NO_KEYS, STRING_NOT_UTF8),
                Arguments.of(
                        "value over 16 MiB",
                        new byte[SIZE_LIMIT + 1],
                        hexStringToByte(NO_KEYS),
                        "VARIANT_CONSTRUCTOR_SIZE_LIMIT: The value or the metadata is larger than "
                                + "16 MiB."),
                Arguments.of(
                        "empty value",
                        new byte[0],
                        hexStringToByte(NO_KEYS),
                        "MALFORMED_VARIANT: The value ends before the variant starts."),
                malformed("empty metadata", "00", "", METADATA_VERSION),
                malformed("metadata version 2", "00", "020000", METADATA_VERSION),
                malformed("metadata without offsets", "00", "0101", METADATA_TRUNCATED),
                malformed(
                        "metadata strings past the end",
                        "00",
                        "0101000561",
                        "MALFORMED_VARIANT: Key 0 of the metadata ends before it starts or past "
                                + "the end of the metadata."),
                malformed(
                        "metadata with 2^30 keys in 5 bytes",
                        "00",
                        "c100000040",
                        METADATA_TRUNCATED),
                malformed(
                        "first key offset not 0",
                        "00",
                        "010101026162",
                        "MALFORMED_VARIANT: The first key offset of the metadata is not 0."),
                malformed("key that isn't UTF-8", "00", "01010002c0af", KEY_0_NOT_UTF8),
                malformed("character split over two keys", "00", "0102000102c3a9", KEY_0_NOT_UTF8),
                malformed(
                        "sorted_strings with unsorted keys",
                        "00",
                        "11020001026261",
                        KEYS_NOT_SORTED),
                malformed(
                        "sorted_strings with a key twice",
                        "00",
                        "11020001026161",
                        KEYS_NOT_SORTED));
    }

    private static Arguments malformed(
            final String name, final String value, final String metadata, final String message) {
        return Arguments.of(name, hexStringToByte(value), hexStringToByte(metadata), message);
    }

    private static BinaryVariant variant(final String value, final String metadata) {
        return new BinaryVariant(hexStringToByte(value), hexStringToByte(metadata));
    }

    /** An object that holds a value of every type, with nested objects and arrays. */
    private static BinaryVariant everyType() {
        final BinaryVariantBuilder builder = new BinaryVariantBuilder();
        return (BinaryVariant)
                builder.object()
                        .add("null", builder.ofNull())
                        .add("true", builder.of(true))
                        .add("false", builder.of(false))
                        .add("tinyint", builder.of((byte) 1))
                        .add("smallint", builder.of((short) 300))
                        .add("int", builder.of(70_000))
                        .add("bigint", builder.of(5_000_000_000L))
                        .add("float", builder.of(1.5f))
                        .add("double", builder.of(2.5d))
                        .add("decimal4", builder.of(new BigDecimal("-1.23")))
                        .add("decimal8", builder.of(new BigDecimal("12345678901.23")))
                        .add("decimal16", builder.of(new BigDecimal("1234567890123456789.01")))
                        .add("short string", builder.of("abc"))
                        .add("long string", builder.of("x".repeat(64)))
                        .add("date", builder.of(LocalDate.of(2026, 11, 23)))
                        .add("time", builder.of(LocalTime.of(8, 30)))
                        .add("timestamp", builder.of(LocalDateTime.of(2026, 11, 23, 8, 30)))
                        .add("timestamp_ltz", builder.of(Instant.ofEpochSecond(1_800_000_000L)))
                        .add(
                                "timestamp_ns",
                                builder.of(LocalDateTime.of(2026, 11, 23, 8, 30, 0, 123_456_789)))
                        .add("timestamp_ltz_ns", builder.of(Instant.ofEpochSecond(1, 123_456_789)))
                        .add("bytes", builder.of(new byte[] {1, 2, 3}))
                        .add("uuid", builder.of(new UUID(1, 2)))
                        .add(
                                "array",
                                builder.array()
                                        .add(builder.of(1))
                                        .add(builder.object().add("a", builder.of("b")).build())
                                        .add(builder.array().build())
                                        .build())
                        .build();
    }

    /** An array of 300 elements, which needs a 4-byte size and 2-byte offsets. */
    private static BinaryVariant wideArray() {
        final BinaryVariantBuilder builder = new BinaryVariantBuilder();
        final BinaryVariantBuilder.VariantArrayBuilder array = builder.array();
        for (int i = 0; i < 300; i++) {
            array.add(builder.of(i));
        }
        return (BinaryVariant) array.build();
    }

    /** An object of 300 fields, which needs a 4-byte size and 2-byte ids and offsets. */
    private static BinaryVariant wideObject() {
        final BinaryVariantBuilder builder = new BinaryVariantBuilder();
        final BinaryVariantBuilder.VariantObjectBuilder object = builder.object();
        for (int i = 0; i < 300; i++) {
            object.add(String.format("k%03d", i), builder.of(i));
        }
        return (BinaryVariant) object.build();
    }

    /**
     * Nests {@code levels} arrays of two elements whose offsets both point to the same child, so
     * that {@code 5 * levels + 1} bytes read as {@code 2^(levels + 1) - 1} values.
     */
    private static byte[] sharedBytes(final int levels) {
        byte[] value = {0x04};
        for (int i = 0; i < levels; i++) {
            final byte[] parent = new byte[5 + value.length];
            parent[0] = arrayHeader(false, 1);
            parent[1] = 2;
            parent[4] = (byte) value.length;
            System.arraycopy(value, 0, parent, 5, value.length);
            value = parent;
        }
        return value;
    }

    /** An array of {@code count} objects {@code {"a": true, "b": true}}, for {@link #KEYS_B_A}. */
    private static byte[] objectsWithKeysBA(final int count) {
        final int dataStart = 3 + count;
        final byte[] value = new byte[dataStart + count * OBJECT_A_B.length];
        value[0] = arrayHeader(false, 1);
        value[1] = (byte) count;
        for (int i = 0; i < count; i++) {
            value[3 + i] = (byte) ((i + 1) * OBJECT_A_B.length);
            System.arraycopy(
                    OBJECT_A_B, 0, value, dataStart + i * OBJECT_A_B.length, OBJECT_A_B.length);
        }
        return value;
    }

    /** An array of {@code elements} elements whose offsets all point to one 60-byte string. */
    private static byte[] sharedString(final int elements) {
        final int dataStart = 3 + elements;
        final byte[] value = new byte[dataStart + 61];
        value[0] = arrayHeader(false, 1);
        value[1] = (byte) elements;
        value[dataStart - 1] = 61;
        value[dataStart] = shortStrHeader(60);
        Arrays.fill(value, dataStart + 1, value.length, (byte) 'x');
        return value;
    }
}
