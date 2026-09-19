/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.operators.sort.SortUtil;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.data.binary.BinaryStringData.blankString;
import static org.apache.flink.table.data.binary.BinaryStringData.fromBytes;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.EMPTY_STRING_ARRAY;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.concat;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.concatWs;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.isEmpty;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.reverse;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.splitByWholeSeparatorPreserveAllTokens;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.substringSQL;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toByte;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toDecimal;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toDouble;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toFloat;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toInt;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toLong;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toShort;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trim;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trimLeft;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trimRight;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test of {@link BinaryStringData}.
 *
 * <p>Tests that depend on the backing memory layout are parameterized over {@link Mode} and must
 * build their strings through {@link #fromString(Mode, String)} to cover all layouts. Tests that
 * are layout-independent are plain {@link Test}s.
 */
class BinaryStringDataTest {

    /** The backing memory layout that {@link #fromString(Mode, String)} produces. */
    enum Mode {
        ONE_SEG,
        MULTI_SEGS,
        STRING,
        RANDOM
    }

    private static BinaryStringData fromString(Mode mode, String str) {
        BinaryStringData string = BinaryStringData.fromString(str);

        if (mode == Mode.RANDOM) {
            mode = new Mode[] {Mode.ONE_SEG, Mode.MULTI_SEGS, Mode.STRING}[new Random().nextInt(3)];
        }

        if (mode == Mode.STRING) {
            return string;
        }
        if (mode == Mode.ONE_SEG || string.getSizeInBytes() < 2) {
            string.ensureMaterialized();
            return string;
        } else {
            int numBytes = string.getSizeInBytes();
            int pad = new Random().nextInt(5);
            int numBytesWithPad = numBytes + pad;
            int segSize = numBytesWithPad / 2 + 1;
            byte[] bytes1 = new byte[segSize];
            byte[] bytes2 = new byte[segSize];
            if (segSize - pad > 0 && numBytes >= segSize - pad) {
                string.getSegments()[0].get(0, bytes1, pad, segSize - pad);
            }
            string.getSegments()[0].get(segSize - pad, bytes2, 0, numBytes - segSize + pad);
            return BinaryStringData.fromAddress(
                    new MemorySegment[] {
                        MemorySegmentFactory.wrap(bytes1), MemorySegmentFactory.wrap(bytes2)
                    },
                    pad,
                    numBytes);
        }
    }

    /** Cases for the string-to-decimal conversion, shared by the string and binary-row tests. */
    private static final class DecimalCase {
        private final String str;
        private final int precision;
        private final int scale;

        private DecimalCase(String str, int precision, int scale) {
            this.str = str;
            this.precision = precision;
            this.scale = scale;
        }
    }

    private static final List<DecimalCase> DECIMAL_CASES =
            List.of(
                    new DecimalCase("12.345", 5, 3),
                    new DecimalCase("-12.345", 5, 3),
                    new DecimalCase("+12345", 5, 0),
                    new DecimalCase("-12345", 5, 0),
                    new DecimalCase("12345.", 5, 0),
                    new DecimalCase("-12345.", 5, 0),
                    new DecimalCase(".12345", 5, 5),
                    new DecimalCase("-.12345", 5, 5),
                    new DecimalCase("+12.345E3", 5, 0),
                    new DecimalCase("-12.345e3", 5, 0),
                    new DecimalCase("12.345e-3", 6, 6),
                    new DecimalCase("-12.345E-3", 6, 6),
                    new DecimalCase("12345E3", 8, 0),
                    new DecimalCase("-12345e3", 8, 0),
                    new DecimalCase("12345e-3", 5, 3),
                    new DecimalCase("-12345E-3", 5, 3),
                    new DecimalCase("+.12345E3", 5, 2),
                    new DecimalCase("-.12345e3", 5, 2),
                    new DecimalCase(".12345e-3", 8, 8),
                    new DecimalCase("-.12345E-3", 8, 8),
                    new DecimalCase("1234512345.1234", 18, 8),
                    new DecimalCase("-1234512345.1234", 18, 8),
                    new DecimalCase("1234512345.1234", 12, 2),
                    new DecimalCase("-1234512345.1234", 12, 2),
                    new DecimalCase("1234512345.1299", 12, 2),
                    new DecimalCase("-1234512345.1299", 12, 2),
                    new DecimalCase("999999999999999999", 18, 0),
                    new DecimalCase("1234512345.1234512345", 20, 10),
                    new DecimalCase("-1234512345.1234512345", 20, 10),
                    new DecimalCase("1234512345.1234512345", 15, 5),
                    new DecimalCase("-1234512345.1234512345", 15, 5),
                    new DecimalCase("12345123451234512345E-10", 20, 10),
                    new DecimalCase("-12345123451234512345E-10", 20, 10),
                    new DecimalCase("12345123451234512345E-10", 15, 5),
                    new DecimalCase("-12345123451234512345E-10", 15, 5),
                    new DecimalCase("999999999999999999999", 21, 0),
                    new DecimalCase("-999999999999999999999", 21, 0),
                    new DecimalCase("0.00000000000000000000123456789123456789", 38, 38),
                    new DecimalCase("-0.00000000000000000000123456789123456789", 38, 38),
                    new DecimalCase("0.00000000000000000000123456789123456789", 29, 29),
                    new DecimalCase("-0.00000000000000000000123456789123456789", 29, 29),
                    new DecimalCase("123456789123E-27", 18, 18),
                    new DecimalCase("-123456789123E-27", 18, 18),
                    new DecimalCase("123456789999E-27", 18, 18),
                    new DecimalCase("-123456789999E-27", 18, 18),
                    new DecimalCase("123456789123456789E-36", 18, 18),
                    new DecimalCase("-123456789123456789E-36", 18, 18),
                    new DecimalCase("123456789999999999E-36", 18, 18),
                    new DecimalCase("-123456789999999999E-36", 18, 18));

    private static Stream<Arguments> decimalCases() {
        return Arrays.stream(Mode.values())
                .flatMap(
                        mode ->
                                DECIMAL_CASES.stream()
                                        .map(c -> arguments(mode, c.str, c.precision, c.scale)));
    }

    /** Pairs every {@link Mode} with each of the given input strings. */
    private static Stream<Arguments> withModes(String... inputs) {
        return Arrays.stream(Mode.values())
                .flatMap(mode -> Arrays.stream(inputs).map(input -> arguments(mode, input)));
    }

    /** Decodes UTF-8 bytes through one of the {@link StringUtf8Utils#decodeUTF8} entry points. */
    @FunctionalInterface
    private interface Utf8Decoder {
        String decode(byte[] bytes);
    }

    private static void checkBasic(Mode mode, String str, int len) {
        BinaryStringData s1 = fromString(mode, str);
        BinaryStringData s2 = fromBytes(str.getBytes(StandardCharsets.UTF_8));
        assertThat(s1.numChars()).isEqualTo(len);
        assertThat(s2.numChars()).isEqualTo(len);

        assertThat(s1).hasToString(str);
        assertThat(s2).hasToString(str);
        assertThat(s2).isEqualTo(s1);

        assertThat(s2.hashCode()).isEqualTo(s1.hashCode());

        assertThat(s1.compareTo(s2)).isZero();

        assertThat(s1.contains(s2)).isTrue();
        assertThat(s2.contains(s1)).isTrue();
        assertThat(s1.startsWith(s1)).isTrue();
        assertThat(s1.endsWith(s1)).isTrue();
    }

    @Nested
    @DisplayName("Basics")
    class Basics {

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void basicTest(Mode mode) {
            checkBasic(mode, "", 0);
            checkBasic(mode, ",", 1);
            checkBasic(mode, "hello", 5);
            checkBasic(mode, "hello world", 11);
            checkBasic(mode, "Flink中文社区", 9);
            checkBasic(mode, "中 文 社 区", 7);

            checkBasic(mode, "¡", 1); // 2 bytes char
            checkBasic(mode, "ку", 2); // 2 * 2 bytes chars
            checkBasic(mode, "︽﹋％", 3); // 3 * 3 bytes chars
            checkBasic(mode, "🤙", 1); // 4 bytes char
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void emptyStringTest(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(fromString(mode, "")).isEqualTo(empty);
            assertThat(fromBytes(new byte[0])).isEqualTo(empty);
            assertThat(empty.numChars()).isZero();
            assertThat(empty.getSizeInBytes()).isZero();
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void testIsEmpty(Mode mode) {
            assertThat(isEmpty(fromString(mode, ""))).isTrue();
            assertThat(isEmpty(BinaryStringData.fromBytes(new byte[] {}))).isTrue();
            assertThat(isEmpty(fromString(mode, "hello"))).isFalse();
            assertThat(isEmpty(BinaryStringData.fromBytes("hello".getBytes()))).isFalse();
            assertThat(isEmpty(fromString(mode, "中文"))).isFalse();
            assertThat(isEmpty(BinaryStringData.fromBytes("中文".getBytes()))).isFalse();
            assertThat(isEmpty(new BinaryStringData())).isTrue();
        }

        @Test
        void testLazy() {
            String javaStr = "haha";
            BinaryStringData str = BinaryStringData.fromString(javaStr);
            str.ensureMaterialized();

            // check reference same.
            assertThat(str.toString()).isSameAs(javaStr);
        }
    }

    @Nested
    @DisplayName("Comparison")
    class Comparison {

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void compareTo(Mode mode) {
            assertThat(fromString(mode, "   ").compareTo(blankString(3))).isZero();
            assertThat(fromString(mode, "").compareTo(fromString(mode, "a"))).isLessThan(0);
            assertThat(fromString(mode, "abc").compareTo(fromString(mode, "ABC"))).isGreaterThan(0);
            assertThat(fromString(mode, "abc0").compareTo(fromString(mode, "abc")))
                    .isGreaterThan(0);
            assertThat(fromString(mode, "abcabcabc").compareTo(fromString(mode, "abcabcabc")))
                    .isZero();
            assertThat(fromString(mode, "aBcabcabc").compareTo(fromString(mode, "Abcabcabc")))
                    .isGreaterThan(0);
            assertThat(fromString(mode, "Abcabcabc").compareTo(fromString(mode, "abcabcabC")))
                    .isLessThan(0);
            assertThat(fromString(mode, "abcabcabc").compareTo(fromString(mode, "abcabcabC")))
                    .isGreaterThan(0);

            assertThat(fromString(mode, "abc").compareTo(fromString(mode, "世界"))).isLessThan(0);
            assertThat(fromString(mode, "你好").compareTo(fromString(mode, "世界"))).isGreaterThan(0);
            assertThat(fromString(mode, "你好123").compareTo(fromString(mode, "你好122")))
                    .isGreaterThan(0);

            MemorySegment segment1 = MemorySegmentFactory.allocateUnpooledSegment(1024);
            MemorySegment segment2 = MemorySegmentFactory.allocateUnpooledSegment(1024);
            SortUtil.putStringNormalizedKey(fromString(mode, "abcabcabc"), segment1, 0, 9);
            SortUtil.putStringNormalizedKey(fromString(mode, "abcabcabC"), segment2, 0, 9);
            assertThat(segment1.compare(segment2, 0, 0, 9)).isGreaterThan(0);
            SortUtil.putStringNormalizedKey(fromString(mode, "abcab"), segment1, 0, 9);
            assertThat(segment1.compare(segment2, 0, 0, 9)).isLessThan(0);
        }

        @Test
        void testMultiSegments() {

            // prepare
            MemorySegment[] segments1 = new MemorySegment[2];
            segments1[0] = MemorySegmentFactory.wrap(new byte[10]);
            segments1[1] = MemorySegmentFactory.wrap(new byte[10]);
            segments1[0].put(5, "abcde".getBytes(UTF_8), 0, 5);
            segments1[1].put(0, "aaaaa".getBytes(UTF_8), 0, 5);

            MemorySegment[] segments2 = new MemorySegment[2];
            segments2[0] = MemorySegmentFactory.wrap(new byte[5]);
            segments2[1] = MemorySegmentFactory.wrap(new byte[5]);
            segments2[0].put(0, "abcde".getBytes(UTF_8), 0, 5);
            segments2[1].put(0, "b".getBytes(UTF_8), 0, 1);

            // test go ahead both
            BinaryStringData binaryString1 = BinaryStringData.fromAddress(segments1, 5, 10);
            BinaryStringData binaryString2 = BinaryStringData.fromAddress(segments2, 0, 6);
            assertThat(binaryString1).hasToString("abcdeaaaaa");
            assertThat(binaryString2).hasToString("abcdeb");
            assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);

            // test needCompare == len
            binaryString1 = BinaryStringData.fromAddress(segments1, 5, 5);
            binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
            assertThat(binaryString1).hasToString("abcde");
            assertThat(binaryString2).hasToString("abcde");
            assertThat(binaryString1.compareTo(binaryString2)).isZero();

            // test find the first segment of this string
            binaryString1 = BinaryStringData.fromAddress(segments1, 10, 5);
            binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
            assertThat(binaryString1).hasToString("aaaaa");
            assertThat(binaryString2).hasToString("abcde");
            assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);
            assertThat(binaryString2.compareTo(binaryString1)).isEqualTo(1);

            // test go ahead single
            segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(new byte[10])};
            segments2[0].put(4, "abcdeb".getBytes(UTF_8), 0, 6);
            binaryString1 = BinaryStringData.fromAddress(segments1, 5, 10);
            binaryString2 = BinaryStringData.fromAddress(segments2, 4, 6);
            assertThat(binaryString1).hasToString("abcdeaaaaa");
            assertThat(binaryString2).hasToString("abcdeb");
            assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);
            assertThat(binaryString2.compareTo(binaryString1)).isEqualTo(1);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void testEmptyString(Mode mode) {
            BinaryStringData str2 = fromString(mode, "hahahahah");
            BinaryStringData str3;
            MemorySegment[] segments = new MemorySegment[2];
            segments[0] = MemorySegmentFactory.wrap(new byte[10]);
            segments[1] = MemorySegmentFactory.wrap(new byte[10]);
            str3 = BinaryStringData.fromAddress(segments, 15, 0);

            assertThat(BinaryStringData.EMPTY_UTF8.compareTo(str2)).isLessThan(0);
            assertThat(str2.compareTo(BinaryStringData.EMPTY_UTF8)).isGreaterThan(0);

            assertThat(BinaryStringData.EMPTY_UTF8.compareTo(str3)).isZero();
            assertThat(str3.compareTo(BinaryStringData.EMPTY_UTF8)).isZero();

            assertThat(str2).isNotEqualTo(BinaryStringData.EMPTY_UTF8);
            assertThat(BinaryStringData.EMPTY_UTF8).isNotEqualTo(str2);

            assertThat(str3).isEqualTo(BinaryStringData.EMPTY_UTF8);
            assertThat(BinaryStringData.EMPTY_UTF8).isEqualTo(str3);
        }
    }

    @Nested
    @DisplayName("Search")
    class Search {

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void contains(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(empty.contains(empty)).isTrue();
            assertThat(fromString(mode, "hello").contains(fromString(mode, "ello"))).isTrue();
            assertThat(fromString(mode, "hello").contains(fromString(mode, "vello"))).isFalse();
            assertThat(fromString(mode, "hello").contains(fromString(mode, "hellooo"))).isFalse();
            assertThat(fromString(mode, "大千世界").contains(fromString(mode, "千世界"))).isTrue();
            assertThat(fromString(mode, "大千世界").contains(fromString(mode, "世千"))).isFalse();
            assertThat(fromString(mode, "大千世界").contains(fromString(mode, "大千世界好"))).isFalse();
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void startsWith(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(empty.startsWith(empty)).isTrue();
            assertThat(fromString(mode, "hello").startsWith(fromString(mode, "hell"))).isTrue();
            assertThat(fromString(mode, "hello").startsWith(fromString(mode, "ell"))).isFalse();
            assertThat(fromString(mode, "hello").startsWith(fromString(mode, "hellooo"))).isFalse();
            assertThat(fromString(mode, "数据砖头").startsWith(fromString(mode, "数据"))).isTrue();
            assertThat(fromString(mode, "大千世界").startsWith(fromString(mode, "千"))).isFalse();
            assertThat(fromString(mode, "大千世界").startsWith(fromString(mode, "大千世界好"))).isFalse();
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void endsWith(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(empty.endsWith(empty)).isTrue();
            assertThat(fromString(mode, "hello").endsWith(fromString(mode, "ello"))).isTrue();
            assertThat(fromString(mode, "hello").endsWith(fromString(mode, "ellov"))).isFalse();
            assertThat(fromString(mode, "hello").endsWith(fromString(mode, "hhhello"))).isFalse();
            assertThat(fromString(mode, "大千世界").endsWith(fromString(mode, "世界"))).isTrue();
            assertThat(fromString(mode, "大千世界").endsWith(fromString(mode, "世"))).isFalse();
            assertThat(fromString(mode, "数据砖头").endsWith(fromString(mode, "我的数据砖头"))).isFalse();
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void indexOf(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(empty.indexOf(empty, 0)).isZero();
            assertThat(empty.indexOf(fromString(mode, "l"), 0)).isEqualTo(-1);
            assertThat(fromString(mode, "hello").indexOf(empty, 0)).isZero();
            assertThat(fromString(mode, "hello").indexOf(fromString(mode, "l"), 0)).isEqualTo(2);
            assertThat(fromString(mode, "hello").indexOf(fromString(mode, "l"), 3)).isEqualTo(3);
            assertThat(fromString(mode, "hello").indexOf(fromString(mode, "a"), 0)).isEqualTo(-1);
            assertThat(fromString(mode, "hello").indexOf(fromString(mode, "ll"), 0)).isEqualTo(2);
            assertThat(fromString(mode, "hello").indexOf(fromString(mode, "ll"), 4)).isEqualTo(-1);
            assertThat(fromString(mode, "数据砖头").indexOf(fromString(mode, "据砖"), 0)).isEqualTo(1);
            assertThat(fromString(mode, "数据砖头").indexOf(fromString(mode, "数"), 3)).isEqualTo(-1);
            assertThat(fromString(mode, "数据砖头").indexOf(fromString(mode, "数"), 0)).isZero();
            assertThat(fromString(mode, "数据砖头").indexOf(fromString(mode, "头"), 0)).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Manipulation")
    class Manipulation {

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void concatTest(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(concat()).isEqualTo(empty);
            assertThat(concat((BinaryStringData) null)).isNull();
            assertThat(concat(empty)).isEqualTo(empty);
            assertThat(concat(fromString(mode, "ab"))).isEqualTo(fromString(mode, "ab"));
            assertThat(concat(fromString(mode, "a"), fromString(mode, "b")))
                    .isEqualTo(fromString(mode, "ab"));
            assertThat(concat(fromString(mode, "a"), fromString(mode, "b"), fromString(mode, "c")))
                    .isEqualTo(fromString(mode, "abc"));
            assertThat(concat(fromString(mode, "a"), null, fromString(mode, "c"))).isNull();
            assertThat(concat(fromString(mode, "a"), null, null)).isNull();
            assertThat(concat(null, null, null)).isNull();
            assertThat(concat(fromString(mode, "数据"), fromString(mode, "砖头")))
                    .isEqualTo(fromString(mode, "数据砖头"));
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void concatWsTest(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            // Returns empty if the separator is null
            assertThat(concatWs(null, (BinaryStringData) null)).isNull();
            assertThat(concatWs(null, fromString(mode, "a"))).isNull();

            // If separator is null, concatWs should skip all null inputs and never return null.
            BinaryStringData sep = fromString(mode, "哈哈");
            assertThat(concatWs(sep, empty)).isEqualTo(empty);
            assertThat(concatWs(sep, fromString(mode, "ab"))).isEqualTo(fromString(mode, "ab"));
            assertThat(concatWs(sep, fromString(mode, "a"), fromString(mode, "b")))
                    .isEqualTo(fromString(mode, "a哈哈b"));
            assertThat(
                            concatWs(
                                    sep,
                                    fromString(mode, "a"),
                                    fromString(mode, "b"),
                                    fromString(mode, "c")))
                    .isEqualTo(fromString(mode, "a哈哈b哈哈c"));
            assertThat(concatWs(sep, fromString(mode, "a"), null, fromString(mode, "c")))
                    .isEqualTo(fromString(mode, "a哈哈c"));
            assertThat(concatWs(sep, fromString(mode, "a"), null, null))
                    .isEqualTo(fromString(mode, "a"));
            assertThat(concatWs(sep, null, null, null)).isEqualTo(empty);
            assertThat(concatWs(sep, fromString(mode, "数据"), fromString(mode, "砖头")))
                    .isEqualTo(fromString(mode, "数据哈哈砖头"));
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void substring(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(fromString(mode, "hello").substring(0, 0)).isEqualTo(empty);
            assertThat(fromString(mode, "hello").substring(1, 3)).isEqualTo(fromString(mode, "el"));
            assertThat(fromString(mode, "数据砖头").substring(0, 1)).isEqualTo(fromString(mode, "数"));
            assertThat(fromString(mode, "数据砖头").substring(1, 3)).isEqualTo(fromString(mode, "据砖"));
            assertThat(fromString(mode, "数据砖头").substring(3, 5)).isEqualTo(fromString(mode, "头"));
            assertThat(fromString(mode, "ߵ梷").substring(0, 2)).isEqualTo(fromString(mode, "ߵ梷"));
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void trims(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(fromString(mode, "1").trim()).isEqualTo(fromString(mode, "1"));

            assertThat(fromString(mode, "  hello ").trim()).isEqualTo(fromString(mode, "hello"));
            assertThat(trimLeft(fromString(mode, "  hello ")))
                    .isEqualTo(fromString(mode, "hello "));
            assertThat(trimRight(fromString(mode, "  hello ")))
                    .isEqualTo(fromString(mode, "  hello"));

            assertThat(trim(fromString(mode, "  hello "), false, false, fromString(mode, " ")))
                    .isEqualTo(fromString(mode, "  hello "));
            assertThat(trim(fromString(mode, "  hello "), true, true, fromString(mode, " ")))
                    .isEqualTo(fromString(mode, "hello"));
            assertThat(trim(fromString(mode, "  hello "), true, false, fromString(mode, " ")))
                    .isEqualTo(fromString(mode, "hello "));
            assertThat(trim(fromString(mode, "  hello "), false, true, fromString(mode, " ")))
                    .isEqualTo(fromString(mode, "  hello"));
            assertThat(trim(fromString(mode, "xxxhellox"), true, true, fromString(mode, "x")))
                    .isEqualTo(fromString(mode, "hello"));

            assertThat(trim(fromString(mode, "xxxhellox"), fromString(mode, "xoh")))
                    .isEqualTo(fromString(mode, "ell"));

            assertThat(trimLeft(fromString(mode, "xxxhellox"), fromString(mode, "xoh")))
                    .isEqualTo(fromString(mode, "ellox"));

            assertThat(trimRight(fromString(mode, "xxxhellox"), fromString(mode, "xoh")))
                    .isEqualTo(fromString(mode, "xxxhell"));

            assertThat(empty.trim()).isEqualTo(empty);
            assertThat(fromString(mode, "  ").trim()).isEqualTo(empty);
            assertThat(trimLeft(fromString(mode, "  "))).isEqualTo(empty);
            assertThat(trimRight(fromString(mode, "  "))).isEqualTo(empty);

            assertThat(fromString(mode, "  数据砖头 ").trim()).isEqualTo(fromString(mode, "数据砖头"));
            assertThat(trimLeft(fromString(mode, "  数据砖头 "))).isEqualTo(fromString(mode, "数据砖头 "));
            assertThat(trimRight(fromString(mode, "  数据砖头 ")))
                    .isEqualTo(fromString(mode, "  数据砖头"));

            assertThat(fromString(mode, "数据砖头").trim()).isEqualTo(fromString(mode, "数据砖头"));
            assertThat(trimLeft(fromString(mode, "数据砖头"))).isEqualTo(fromString(mode, "数据砖头"));
            assertThat(trimRight(fromString(mode, "数据砖头"))).isEqualTo(fromString(mode, "数据砖头"));

            assertThat(trim(fromString(mode, "年年岁岁, 岁岁年年"), fromString(mode, "年岁 ")))
                    .isEqualTo(fromString(mode, ","));
            assertThat(trimLeft(fromString(mode, "年年岁岁, 岁岁年年"), fromString(mode, "年岁 ")))
                    .isEqualTo(fromString(mode, ", 岁岁年年"));
            assertThat(trimRight(fromString(mode, "年年岁岁, 岁岁年年"), fromString(mode, "年岁 ")))
                    .isEqualTo(fromString(mode, "年年岁岁,"));

            char[] charsLessThan0x20 = new char[10];
            Arrays.fill(charsLessThan0x20, (char) (' ' - 1));
            String stringStartingWithSpace =
                    new String(charsLessThan0x20) + "hello" + new String(charsLessThan0x20);
            assertThat(fromString(mode, stringStartingWithSpace).trim())
                    .isEqualTo(fromString(mode, stringStartingWithSpace));
            assertThat(trimLeft(fromString(mode, stringStartingWithSpace)))
                    .isEqualTo(fromString(mode, stringStartingWithSpace));
            assertThat(trimRight(fromString(mode, stringStartingWithSpace)))
                    .isEqualTo(fromString(mode, stringStartingWithSpace));
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void testSqlSubstring(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(substringSQL(fromString(mode, "hello"), 2))
                    .isEqualTo(fromString(mode, "ello"));
            assertThat(substringSQL(fromString(mode, "hello"), 2, 3))
                    .isEqualTo(fromString(mode, "ell"));
            assertThat(substringSQL(empty, 2, 3)).isEqualTo(empty);
            assertThat(substringSQL(fromString(mode, "hello"), 0, -1)).isNull();
            assertThat(substringSQL(fromString(mode, "hello"), 10)).isEqualTo(empty);
            assertThat(substringSQL(fromString(mode, "hello"), 0, 3))
                    .isEqualTo(fromString(mode, "hel"));
            assertThat(substringSQL(fromString(mode, "hello"), -2, 3))
                    .isEqualTo(fromString(mode, "lo"));
            assertThat(substringSQL(fromString(mode, "hello"), -100, 3)).isEqualTo(empty);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void reverseTest(Mode mode) {
            BinaryStringData empty = fromString(mode, "");
            assertThat(reverse(fromString(mode, "hello"))).isEqualTo(fromString(mode, "olleh"));
            assertThat(reverse(fromString(mode, "中国"))).isEqualTo(fromString(mode, "国中"));
            assertThat(reverse(fromString(mode, "hello, 中国")))
                    .isEqualTo(fromString(mode, "国中 ,olleh"));
            assertThat(reverse(empty)).isEqualTo(empty);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void testSplit(Mode mode) {
            assertThat(
                            splitByWholeSeparatorPreserveAllTokens(
                                    fromString(mode, ""), fromString(mode, "")))
                    .isEqualTo(EMPTY_STRING_ARRAY);
            assertThat(splitByWholeSeparatorPreserveAllTokens(fromString(mode, "ab de fg"), null))
                    .isEqualTo(
                            new BinaryStringData[] {
                                fromString(mode, "ab"),
                                fromString(mode, "de"),
                                fromString(mode, "fg")
                            });
            assertThat(splitByWholeSeparatorPreserveAllTokens(fromString(mode, "ab   de fg"), null))
                    .isEqualTo(
                            new BinaryStringData[] {
                                fromString(mode, "ab"),
                                fromString(mode, ""),
                                fromString(mode, ""),
                                fromString(mode, "de"),
                                fromString(mode, "fg")
                            });
            assertThat(
                            splitByWholeSeparatorPreserveAllTokens(
                                    fromString(mode, "ab:cd:ef"), fromString(mode, ":")))
                    .isEqualTo(
                            new BinaryStringData[] {
                                fromString(mode, "ab"),
                                fromString(mode, "cd"),
                                fromString(mode, "ef")
                            });
            assertThat(
                            splitByWholeSeparatorPreserveAllTokens(
                                    fromString(mode, "ab-!-cd-!-ef"), fromString(mode, "-!-")))
                    .isEqualTo(
                            new BinaryStringData[] {
                                fromString(mode, "ab"),
                                fromString(mode, "cd"),
                                fromString(mode, "ef")
                            });
        }
    }

    @Nested
    @DisplayName("Conversion")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Conversion {

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void toIntegralTypes(Mode mode) {
            // Test to integer.
            assertThat(toByte(fromString(mode, "123"))).isEqualTo(Byte.parseByte("123"));
            assertThat(toByte(fromString(mode, "+123"))).isEqualTo(Byte.parseByte("123"));
            assertThat(toByte(fromString(mode, "-123"))).isEqualTo(Byte.parseByte("-123"));

            assertThat(toShort(fromString(mode, "123"))).isEqualTo(Short.parseShort("123"));
            assertThat(toShort(fromString(mode, "+123"))).isEqualTo(Short.parseShort("123"));
            assertThat(toShort(fromString(mode, "-123"))).isEqualTo(Short.parseShort("-123"));

            assertThat(toInt(fromString(mode, "123"))).isEqualTo(Integer.parseInt("123"));
            assertThat(toInt(fromString(mode, "+123"))).isEqualTo(Integer.parseInt("123"));
            assertThat(toInt(fromString(mode, "-123"))).isEqualTo(Integer.parseInt("-123"));

            assertThat(toLong(fromString(mode, "1234567890")))
                    .isEqualTo(Long.parseLong("1234567890"));
            assertThat(toLong(fromString(mode, "+1234567890")))
                    .isEqualTo(Long.parseLong("+1234567890"));
            assertThat(toLong(fromString(mode, "-1234567890")))
                    .isEqualTo(Long.parseLong("-1234567890"));

            // Test decimal string to integer.
            assertThat(toInt(fromString(mode, "123.456789"))).isEqualTo(Integer.parseInt("123"));
            assertThat(toLong(fromString(mode, "123.456789"))).isEqualTo(Long.parseLong("123"));

            // Test negative cases.
            assertThatThrownBy(() -> toInt(fromString(mode, "1a3.456789")))
                    .isInstanceOf(NumberFormatException.class);
            assertThatThrownBy(() -> toInt(fromString(mode, "123.a56789")))
                    .isInstanceOf(NumberFormatException.class);
        }

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("positiveInfinityCases")
        void castsPositiveInfinity(Mode mode, String input) {
            assertThat(toFloat(fromString(mode, input))).isEqualTo(Float.POSITIVE_INFINITY);
            assertThat(toDouble(fromString(mode, input))).isEqualTo(Double.POSITIVE_INFINITY);
        }

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("negativeInfinityCases")
        void castsNegativeInfinity(Mode mode, String input) {
            assertThat(toFloat(fromString(mode, input))).isEqualTo(Float.NEGATIVE_INFINITY);
            assertThat(toDouble(fromString(mode, input))).isEqualTo(Double.NEGATIVE_INFINITY);
        }

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("nanCases")
        void castsNaN(Mode mode, String input) {
            assertThat(toFloat(fromString(mode, input))).isNaN();
            assertThat(toDouble(fromString(mode, input))).isNaN();
        }

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("invalidApproximateCases")
        void rejectsInvalidApproximate(Mode mode, String input) {
            assertThatThrownBy(() -> toFloat(fromString(mode, input)))
                    .isInstanceOf(NumberFormatException.class);
            assertThatThrownBy(() -> toDouble(fromString(mode, input)))
                    .isInstanceOf(NumberFormatException.class);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void parsesOrdinaryDouble(Mode mode) {
            assertThat(toDouble(fromString(mode, "1.5"))).isEqualTo(1.5);
        }

        @Test
        void toNumericFromBinaryRow() {
            BinaryRowData row = new BinaryRowData(20);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, BinaryStringData.fromString("1"));
            writer.writeString(1, BinaryStringData.fromString("123"));
            writer.writeString(2, BinaryStringData.fromString("12345"));
            writer.writeString(3, BinaryStringData.fromString("123456789"));
            writer.complete();

            assertThat(toByte(((BinaryStringData) row.getString(0))))
                    .isEqualTo(Byte.parseByte("1"));
            assertThat(toShort(((BinaryStringData) row.getString(1))))
                    .isEqualTo(Short.parseShort("123"));
            assertThat(toInt(((BinaryStringData) row.getString(2))))
                    .isEqualTo(Integer.parseInt("12345"));
            assertThat(toLong(((BinaryStringData) row.getString(3))))
                    .isEqualTo(Long.parseLong("123456789"));
        }

        @ParameterizedTest(name = "{0}: {1} -> DECIMAL({2},{3})")
        @MethodSource("org.apache.flink.table.data.BinaryStringDataTest#decimalCases")
        void toDecimalFromString(Mode mode, String str, int precision, int scale) {
            assertThat(toDecimal(fromString(mode, str), precision, scale))
                    .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(str), precision, scale));
        }

        @ParameterizedTest(name = "{0} -> DECIMAL({1},{2})")
        @MethodSource("decimalRowCases")
        void toDecimalFromBinaryRow(String str, int precision, int scale) {
            BinaryRowData row = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, BinaryStringData.fromString(str));
            writer.complete();
            assertThat(toDecimal((BinaryStringData) row.getString(0), precision, scale))
                    .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(str), precision, scale));
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(Mode.class)
        void testToUpperLowerCase(Mode mode) {
            assertThat(fromString(mode, "我是中国人").toLowerCase())
                    .isEqualTo(fromString(mode, "我是中国人"));
            assertThat(fromString(mode, "我是中国人").toUpperCase())
                    .isEqualTo(fromString(mode, "我是中国人"));

            assertThat(fromString(mode, "aBcDeFg").toLowerCase())
                    .isEqualTo(fromString(mode, "abcdefg"));
            assertThat(fromString(mode, "aBcDeFg").toUpperCase())
                    .isEqualTo(fromString(mode, "ABCDEFG"));

            assertThat(fromString(mode, "!@#$%^*").toLowerCase())
                    .isEqualTo(fromString(mode, "!@#$%^*"));
            assertThat(fromString(mode, "!@#$%^*").toLowerCase())
                    .isEqualTo(fromString(mode, "!@#$%^*"));
            // Test composite in BinaryRowData.
            BinaryRowData row = new BinaryRowData(20);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, BinaryStringData.fromString("a"));
            writer.writeString(1, BinaryStringData.fromString("我是中国人"));
            writer.writeString(3, BinaryStringData.fromString("aBcDeFg"));
            writer.writeString(5, BinaryStringData.fromString("!@#$%^*"));
            writer.complete();

            assertThat(((BinaryStringData) row.getString(0)).toUpperCase())
                    .isEqualTo(fromString(mode, "A"));
            assertThat(((BinaryStringData) row.getString(1)).toUpperCase())
                    .isEqualTo(fromString(mode, "我是中国人"));
            assertThat(((BinaryStringData) row.getString(1)).toLowerCase())
                    .isEqualTo(fromString(mode, "我是中国人"));
            assertThat(((BinaryStringData) row.getString(3)).toUpperCase())
                    .isEqualTo(fromString(mode, "ABCDEFG"));
            assertThat(((BinaryStringData) row.getString(3)).toLowerCase())
                    .isEqualTo(fromString(mode, "abcdefg"));
            assertThat(((BinaryStringData) row.getString(5)).toUpperCase())
                    .isEqualTo(fromString(mode, "!@#$%^*"));
            assertThat(((BinaryStringData) row.getString(5)).toLowerCase())
                    .isEqualTo(fromString(mode, "!@#$%^*"));
        }

        private Stream<Arguments> decimalRowCases() {
            return DECIMAL_CASES.stream().map(c -> arguments(c.str, c.precision, c.scale));
        }

        private Stream<Arguments> positiveInfinityCases() {
            return withModes("Infinity", "infinity", "INF", "inf", "+inf");
        }

        private Stream<Arguments> negativeInfinityCases() {
            return withModes("-Infinity", "-infinity", "-INF", "  -inf  ", "  -infinity  ");
        }

        private Stream<Arguments> nanCases() {
            return withModes("NaN", "nan", "NAN");
        }

        private Stream<Arguments> invalidApproximateCases() {
            return withModes("-nan", "-   Infinity");
        }
    }

    @Nested
    @DisplayName("Encoding")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Encoding {

        @Test
        void testEncodeWithIllegalCharacter() {

            // This char array contains illegal characters, such as the lone surrogate 55357.
            // The JDK replaces such characters with '?' when encoding to UTF-8, which
            // StringUtf8Utils#encodeUTF8 should follow.
            char[] chars =
                    new char[] {
                        20122, 40635, 124, 38271, 34966, 124, 36830, 34915, 35033, 124, 55357, 124,
                        56407
                    };

            String str = new String(chars);

            assertThat(StringUtf8Utils.encodeUTF8(str)).isEqualTo(str.getBytes(UTF_8));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("utf8Decoders")
        void testDecodeWithIllegalUtf8Bytes(String name, Utf8Decoder decoder) {

            // illegal utf-8 bytes
            byte[] bytes =
                    new byte[] {
                        (byte) 20122,
                        (byte) 40635,
                        124,
                        (byte) 38271,
                        (byte) 34966,
                        124,
                        (byte) 36830,
                        (byte) 34915,
                        (byte) 35033,
                        124,
                        (byte) 55357,
                        124,
                        (byte) 56407
                    };

            String str = new String(bytes, StandardCharsets.UTF_8);
            assertThat(decoder.decode(bytes)).isEqualTo(str);
        }

        @ParameterizedTest
        @ValueSource(
                ints = {
                    0x80,
                    0x9F,
                    0xBF, // Skip continuation bytes
                    0xC0,
                    0xC2, // 0xC0..0xC1 - disallowed in UTF-8
                    // 0xF5..0xFF - disallowed in UTF-8
                    0xF5,
                    0xF6,
                    0xF7,
                    0xF8,
                    0xF9,
                    0xFA,
                    0xFB,
                    0xFC,
                    0xFD,
                    0xFE,
                    0xFF
                })
        void skipWrongFirstByte(int wrongFirstByte) {
            assertThat(fromBytes(new byte[] {(byte) wrongFirstByte}).numChars()).isEqualTo(1);
        }

        private Stream<Arguments> utf8Decoders() {
            return Stream.of(
                    arguments(
                            "byte[]",
                            (Utf8Decoder)
                                    bytes -> StringUtf8Utils.decodeUTF8(bytes, 0, bytes.length)),
                    arguments(
                            "segment",
                            (Utf8Decoder)
                                    bytes ->
                                            StringUtf8Utils.decodeUTF8(
                                                    MemorySegmentFactory.wrap(bytes),
                                                    0,
                                                    bytes.length)),
                    arguments(
                            "segment at offset",
                            (Utf8Decoder)
                                    bytes -> {
                                        byte[] padded = new byte[bytes.length + 5];
                                        System.arraycopy(bytes, 0, padded, 5, bytes.length);
                                        return StringUtf8Utils.decodeUTF8(
                                                MemorySegmentFactory.wrap(padded), 5, bytes.length);
                                    }));
        }
    }
}
