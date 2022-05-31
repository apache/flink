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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.data.binary.BinaryStringData.blankString;
import static org.apache.flink.table.data.binary.BinaryStringData.fromBytes;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.EMPTY_STRING_ARRAY;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.concat;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.concatWs;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.keyValue;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.reverse;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.splitByWholeSeparatorPreserveAllTokens;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.substringSQL;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toByte;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toDecimal;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toInt;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toLong;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.toShort;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trim;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trimLeft;
import static org.apache.flink.table.data.binary.BinaryStringDataUtil.trimRight;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test of {@link BinaryStringData}.
 *
 * <p>Caution that you must construct a string by {@link #fromString} to cover all the test cases.
 */
@RunWith(Parameterized.class)
public class BinaryStringDataTest {

    private BinaryStringData empty = fromString("");

    private final Mode mode;

    public BinaryStringDataTest(Mode mode) {
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Mode> getVarSeg() {
        return Arrays.asList(Mode.ONE_SEG, Mode.MULTI_SEGS, Mode.STRING, Mode.RANDOM);
    }

    private enum Mode {
        ONE_SEG,
        MULTI_SEGS,
        STRING,
        RANDOM
    }

    private BinaryStringData fromString(String str) {
        BinaryStringData string = BinaryStringData.fromString(str);

        Mode mode = this.mode;

        if (mode == Mode.RANDOM) {
            int rnd = new Random().nextInt(3);
            if (rnd == 0) {
                mode = Mode.ONE_SEG;
            } else if (rnd == 1) {
                mode = Mode.MULTI_SEGS;
            } else if (rnd == 2) {
                mode = Mode.STRING;
            }
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

    private void checkBasic(String str, int len) {
        BinaryStringData s1 = fromString(str);
        BinaryStringData s2 = fromBytes(str.getBytes(StandardCharsets.UTF_8));
        assertThat(len).isEqualTo(s1.numChars());
        assertThat(len).isEqualTo(s2.numChars());

        assertThat(str).isEqualTo(s1.toString());
        assertThat(str).isEqualTo(s2.toString());
        assertThat(s2).isEqualTo(s1);

        assertThat(s2.hashCode()).isEqualTo(s1.hashCode());

        assertThat(s1.compareTo(s2)).isEqualTo(0);

        assertThat(s1.contains(s2)).isTrue();
        assertThat(s2.contains(s1)).isTrue();
        assertThat(s1.startsWith(s1)).isTrue();
        assertThat(s1.endsWith(s1)).isTrue();
    }

    @Test
    public void basicTest() {
        checkBasic("", 0);
        checkBasic(",", 1);
        checkBasic("hello", 5);
        checkBasic("hello world", 11);
        checkBasic("Flink中文社区", 9);
        checkBasic("中 文 社 区", 7);

        checkBasic("¡", 1); // 2 bytes char
        checkBasic("ку", 2); // 2 * 2 bytes chars
        checkBasic("︽﹋％", 3); // 3 * 3 bytes chars
        checkBasic("\uD83E\uDD19", 1); // 4 bytes char
    }

    @Test
    public void emptyStringTest() {
        assertThat(fromString("")).isEqualTo(empty);
        assertThat(fromBytes(new byte[0])).isEqualTo(empty);
        assertThat(empty.numChars()).isEqualTo(0);
        assertThat(empty.getSizeInBytes()).isEqualTo(0);
    }

    @Test
    public void compareTo() {
        assertThat(fromString("   ").compareTo(blankString(3))).isEqualTo(0);
        assertThat(fromString("").compareTo(fromString("a"))).isLessThan(0);
        assertThat(fromString("abc").compareTo(fromString("ABC"))).isGreaterThan(0);
        assertThat(fromString("abc0").compareTo(fromString("abc"))).isGreaterThan(0);
        assertThat(fromString("abcabcabc").compareTo(fromString("abcabcabc"))).isEqualTo(0);
        assertThat(fromString("aBcabcabc").compareTo(fromString("Abcabcabc"))).isGreaterThan(0);
        assertThat(fromString("Abcabcabc").compareTo(fromString("abcabcabC"))).isLessThan(0);
        assertThat(fromString("abcabcabc").compareTo(fromString("abcabcabC"))).isGreaterThan(0);

        assertThat(fromString("abc").compareTo(fromString("世界"))).isLessThan(0);
        assertThat(fromString("你好").compareTo(fromString("世界"))).isGreaterThan(0);
        assertThat(fromString("你好123").compareTo(fromString("你好122"))).isGreaterThan(0);

        MemorySegment segment1 = MemorySegmentFactory.allocateUnpooledSegment(1024);
        MemorySegment segment2 = MemorySegmentFactory.allocateUnpooledSegment(1024);
        SortUtil.putStringNormalizedKey(fromString("abcabcabc"), segment1, 0, 9);
        SortUtil.putStringNormalizedKey(fromString("abcabcabC"), segment2, 0, 9);
        assertThat(segment1.compare(segment2, 0, 0, 9)).isGreaterThan(0);
        SortUtil.putStringNormalizedKey(fromString("abcab"), segment1, 0, 9);
        assertThat(segment1.compare(segment2, 0, 0, 9)).isLessThan(0);
    }

    @Test
    public void testMultiSegments() {

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
        assertThat(binaryString1.toString()).isEqualTo("abcdeaaaaa");
        assertThat(binaryString2.toString()).isEqualTo("abcdeb");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);

        // test needCompare == len
        binaryString1 = BinaryStringData.fromAddress(segments1, 5, 5);
        binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
        assertThat(binaryString1.toString()).isEqualTo("abcde");
        assertThat(binaryString2.toString()).isEqualTo("abcde");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(0);

        // test find the first segment of this string
        binaryString1 = BinaryStringData.fromAddress(segments1, 10, 5);
        binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
        assertThat(binaryString1.toString()).isEqualTo("aaaaa");
        assertThat(binaryString2.toString()).isEqualTo("abcde");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);
        assertThat(binaryString2.compareTo(binaryString1)).isEqualTo(1);

        // test go ahead single
        segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(new byte[10])};
        segments2[0].put(4, "abcdeb".getBytes(UTF_8), 0, 6);
        binaryString1 = BinaryStringData.fromAddress(segments1, 5, 10);
        binaryString2 = BinaryStringData.fromAddress(segments2, 4, 6);
        assertThat(binaryString1.toString()).isEqualTo("abcdeaaaaa");
        assertThat(binaryString2.toString()).isEqualTo("abcdeb");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);
        assertThat(binaryString2.compareTo(binaryString1)).isEqualTo(1);
    }

    @Test
    public void concatTest() {
        assertThat(concat()).isEqualTo(empty);
        assertThat(concat((BinaryStringData) null)).isNull();
        assertThat(concat(empty)).isEqualTo(empty);
        assertThat(concat(fromString("ab"))).isEqualTo(fromString("ab"));
        assertThat(concat(fromString("a"), fromString("b"))).isEqualTo(fromString("ab"));
        assertThat(concat(fromString("a"), fromString("b"), fromString("c")))
                .isEqualTo(fromString("abc"));
        assertThat(concat(fromString("a"), null, fromString("c"))).isNull();
        assertThat(concat(fromString("a"), null, null)).isNull();
        assertThat(concat(null, null, null)).isNull();
        assertThat(concat(fromString("数据"), fromString("砖头"))).isEqualTo(fromString("数据砖头"));
    }

    @Test
    public void concatWsTest() {
        // Returns empty if the separator is null
        assertThat(concatWs(null, (BinaryStringData) null)).isNull();
        assertThat(concatWs(null, fromString("a"))).isNull();

        // If separator is null, concatWs should skip all null inputs and never return null.
        BinaryStringData sep = fromString("哈哈");
        assertThat(concatWs(sep, empty)).isEqualTo(empty);
        assertThat(concatWs(sep, fromString("ab"))).isEqualTo(fromString("ab"));
        assertThat(concatWs(sep, fromString("a"), fromString("b"))).isEqualTo(fromString("a哈哈b"));
        assertThat(concatWs(sep, fromString("a"), fromString("b"), fromString("c")))
                .isEqualTo(fromString("a哈哈b哈哈c"));
        assertThat(concatWs(sep, fromString("a"), null, fromString("c")))
                .isEqualTo(fromString("a哈哈c"));
        assertThat(concatWs(sep, fromString("a"), null, null)).isEqualTo(fromString("a"));
        assertThat(concatWs(sep, null, null, null)).isEqualTo(empty);
        assertThat(concatWs(sep, fromString("数据"), fromString("砖头")))
                .isEqualTo(fromString("数据哈哈砖头"));
    }

    @Test
    public void contains() {
        assertThat(empty.contains(empty)).isTrue();
        assertThat(fromString("hello").contains(fromString("ello"))).isTrue();
        assertThat(fromString("hello").contains(fromString("vello"))).isFalse();
        assertThat(fromString("hello").contains(fromString("hellooo"))).isFalse();
        assertThat(fromString("大千世界").contains(fromString("千世界"))).isTrue();
        assertThat(fromString("大千世界").contains(fromString("世千"))).isFalse();
        assertThat(fromString("大千世界").contains(fromString("大千世界好"))).isFalse();
    }

    @Test
    public void startsWith() {
        assertThat(empty.startsWith(empty)).isTrue();
        assertThat(fromString("hello").startsWith(fromString("hell"))).isTrue();
        assertThat(fromString("hello").startsWith(fromString("ell"))).isFalse();
        assertThat(fromString("hello").startsWith(fromString("hellooo"))).isFalse();
        assertThat(fromString("数据砖头").startsWith(fromString("数据"))).isTrue();
        assertThat(fromString("大千世界").startsWith(fromString("千"))).isFalse();
        assertThat(fromString("大千世界").startsWith(fromString("大千世界好"))).isFalse();
    }

    @Test
    public void endsWith() {
        assertThat(empty.endsWith(empty)).isTrue();
        assertThat(fromString("hello").endsWith(fromString("ello"))).isTrue();
        assertThat(fromString("hello").endsWith(fromString("ellov"))).isFalse();
        assertThat(fromString("hello").endsWith(fromString("hhhello"))).isFalse();
        assertThat(fromString("大千世界").endsWith(fromString("世界"))).isTrue();
        assertThat(fromString("大千世界").endsWith(fromString("世"))).isFalse();
        assertThat(fromString("数据砖头").endsWith(fromString("我的数据砖头"))).isFalse();
    }

    @Test
    public void substring() {
        assertThat(fromString("hello").substring(0, 0)).isEqualTo(empty);
        assertThat(fromString("hello").substring(1, 3)).isEqualTo(fromString("el"));
        assertThat(fromString("数据砖头").substring(0, 1)).isEqualTo(fromString("数"));
        assertThat(fromString("数据砖头").substring(1, 3)).isEqualTo(fromString("据砖"));
        assertThat(fromString("数据砖头").substring(3, 5)).isEqualTo(fromString("头"));
        assertThat(fromString("ߵ梷").substring(0, 2)).isEqualTo(fromString("ߵ梷"));
    }

    @Test
    public void trims() {
        assertThat(fromString("1").trim()).isEqualTo(fromString("1"));

        assertThat(fromString("  hello ").trim()).isEqualTo(fromString("hello"));
        assertThat(trimLeft(fromString("  hello "))).isEqualTo(fromString("hello "));
        assertThat(trimRight(fromString("  hello "))).isEqualTo(fromString("  hello"));

        assertThat(trim(fromString("  hello "), false, false, fromString(" ")))
                .isEqualTo(fromString("  hello "));
        assertThat(trim(fromString("  hello "), true, true, fromString(" ")))
                .isEqualTo(fromString("hello"));
        assertThat(trim(fromString("  hello "), true, false, fromString(" ")))
                .isEqualTo(fromString("hello "));
        assertThat(trim(fromString("  hello "), false, true, fromString(" ")))
                .isEqualTo(fromString("  hello"));
        assertThat(trim(fromString("xxxhellox"), true, true, fromString("x")))
                .isEqualTo(fromString("hello"));

        assertThat(trim(fromString("xxxhellox"), fromString("xoh"))).isEqualTo(fromString("ell"));

        assertThat(trimLeft(fromString("xxxhellox"), fromString("xoh")))
                .isEqualTo(fromString("ellox"));

        assertThat(trimRight(fromString("xxxhellox"), fromString("xoh")))
                .isEqualTo(fromString("xxxhell"));

        assertThat(empty.trim()).isEqualTo(empty);
        assertThat(fromString("  ").trim()).isEqualTo(empty);
        assertThat(trimLeft(fromString("  "))).isEqualTo(empty);
        assertThat(trimRight(fromString("  "))).isEqualTo(empty);

        assertThat(fromString("  数据砖头 ").trim()).isEqualTo(fromString("数据砖头"));
        assertThat(trimLeft(fromString("  数据砖头 "))).isEqualTo(fromString("数据砖头 "));
        assertThat(trimRight(fromString("  数据砖头 "))).isEqualTo(fromString("  数据砖头"));

        assertThat(fromString("数据砖头").trim()).isEqualTo(fromString("数据砖头"));
        assertThat(trimLeft(fromString("数据砖头"))).isEqualTo(fromString("数据砖头"));
        assertThat(trimRight(fromString("数据砖头"))).isEqualTo(fromString("数据砖头"));

        assertThat(trim(fromString("年年岁岁, 岁岁年年"), fromString("年岁 "))).isEqualTo(fromString(","));
        assertThat(trimLeft(fromString("年年岁岁, 岁岁年年"), fromString("年岁 ")))
                .isEqualTo(fromString(", 岁岁年年"));
        assertThat(trimRight(fromString("年年岁岁, 岁岁年年"), fromString("年岁 ")))
                .isEqualTo(fromString("年年岁岁,"));

        char[] charsLessThan0x20 = new char[10];
        Arrays.fill(charsLessThan0x20, (char) (' ' - 1));
        String stringStartingWithSpace =
                new String(charsLessThan0x20) + "hello" + new String(charsLessThan0x20);
        assertThat(fromString(stringStartingWithSpace).trim())
                .isEqualTo(fromString(stringStartingWithSpace));
        assertThat(trimLeft(fromString(stringStartingWithSpace)))
                .isEqualTo(fromString(stringStartingWithSpace));
        assertThat(trimRight(fromString(stringStartingWithSpace)))
                .isEqualTo(fromString(stringStartingWithSpace));
    }

    @Test
    public void testSqlSubstring() {
        assertThat(substringSQL(fromString("hello"), 2)).isEqualTo(fromString("ello"));
        assertThat(substringSQL(fromString("hello"), 2, 3)).isEqualTo(fromString("ell"));
        assertThat(substringSQL(empty, 2, 3)).isEqualTo(empty);
        assertThat(substringSQL(fromString("hello"), 0, -1)).isNull();
        assertThat(substringSQL(fromString("hello"), 10)).isEqualTo(empty);
        assertThat(substringSQL(fromString("hello"), 0, 3)).isEqualTo(fromString("hel"));
        assertThat(substringSQL(fromString("hello"), -2, 3)).isEqualTo(fromString("lo"));
        assertThat(substringSQL(fromString("hello"), -100, 3)).isEqualTo(empty);
    }

    @Test
    public void reverseTest() {
        assertThat(reverse(fromString("hello"))).isEqualTo(fromString("olleh"));
        assertThat(reverse(fromString("中国"))).isEqualTo(fromString("国中"));
        assertThat(reverse(fromString("hello, 中国"))).isEqualTo(fromString("国中 ,olleh"));
        assertThat(reverse(empty)).isEqualTo(empty);
    }

    @Test
    public void indexOf() {
        assertThat(empty.indexOf(empty, 0)).isEqualTo(0);
        assertThat(empty.indexOf(fromString("l"), 0)).isEqualTo(-1);
        assertThat(fromString("hello").indexOf(empty, 0)).isEqualTo(0);
        assertThat(fromString("hello").indexOf(fromString("l"), 0)).isEqualTo(2);
        assertThat(fromString("hello").indexOf(fromString("l"), 3)).isEqualTo(3);
        assertThat(fromString("hello").indexOf(fromString("a"), 0)).isEqualTo(-1);
        assertThat(fromString("hello").indexOf(fromString("ll"), 0)).isEqualTo(2);
        assertThat(fromString("hello").indexOf(fromString("ll"), 4)).isEqualTo(-1);
        assertThat(fromString("数据砖头").indexOf(fromString("据砖"), 0)).isEqualTo(1);
        assertThat(fromString("数据砖头").indexOf(fromString("数"), 3)).isEqualTo(-1);
        assertThat(fromString("数据砖头").indexOf(fromString("数"), 0)).isEqualTo(0);
        assertThat(fromString("数据砖头").indexOf(fromString("头"), 0)).isEqualTo(3);
    }

    @Test
    public void testToNumeric() {
        // Test to integer.
        assertThat(toByte(fromString("123"))).isEqualTo(Byte.parseByte("123"));
        assertThat(toByte(fromString("+123"))).isEqualTo(Byte.parseByte("123"));
        assertThat(toByte(fromString("-123"))).isEqualTo(Byte.parseByte("-123"));

        assertThat(toShort(fromString("123"))).isEqualTo(Short.parseShort("123"));
        assertThat(toShort(fromString("+123"))).isEqualTo(Short.parseShort("123"));
        assertThat(toShort(fromString("-123"))).isEqualTo(Short.parseShort("-123"));

        assertThat(toInt(fromString("123"))).isEqualTo(Integer.parseInt("123"));
        assertThat(toInt(fromString("+123"))).isEqualTo(Integer.parseInt("123"));
        assertThat(toInt(fromString("-123"))).isEqualTo(Integer.parseInt("-123"));

        assertThat(toLong(fromString("1234567890"))).isEqualTo(Long.parseLong("1234567890"));
        assertThat(toLong(fromString("+1234567890"))).isEqualTo(Long.parseLong("+1234567890"));
        assertThat(toLong(fromString("-1234567890"))).isEqualTo(Long.parseLong("-1234567890"));

        // Test decimal string to integer.
        assertThat(toInt(fromString("123.456789"))).isEqualTo(Integer.parseInt("123"));
        assertThat(toLong(fromString("123.456789"))).isEqualTo(Long.parseLong("123"));

        // Test negative cases.
        assertThatThrownBy(() -> toInt(fromString("1a3.456789")))
                .isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> toInt(fromString("123.a56789")))
                .isInstanceOf(NumberFormatException.class);

        // Test composite in BinaryRowData.
        BinaryRowData row = new BinaryRowData(20);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryStringData.fromString("1"));
        writer.writeString(1, BinaryStringData.fromString("123"));
        writer.writeString(2, BinaryStringData.fromString("12345"));
        writer.writeString(3, BinaryStringData.fromString("123456789"));
        writer.complete();

        assertThat(toByte(((BinaryStringData) row.getString(0)))).isEqualTo(Byte.parseByte("1"));
        assertThat(toShort(((BinaryStringData) row.getString(1))))
                .isEqualTo(Short.parseShort("123"));
        assertThat(toInt(((BinaryStringData) row.getString(2))))
                .isEqualTo(Integer.parseInt("12345"));
        assertThat(toLong(((BinaryStringData) row.getString(3))))
                .isEqualTo(Long.parseLong("123456789"));
    }

    @Test
    public void testToUpperLowerCase() {
        assertThat(fromString("我是中国人").toLowerCase()).isEqualTo(fromString("我是中国人"));
        assertThat(fromString("我是中国人").toUpperCase()).isEqualTo(fromString("我是中国人"));

        assertThat(fromString("aBcDeFg").toLowerCase()).isEqualTo(fromString("abcdefg"));
        assertThat(fromString("aBcDeFg").toUpperCase()).isEqualTo(fromString("ABCDEFG"));

        assertThat(fromString("!@#$%^*").toLowerCase()).isEqualTo(fromString("!@#$%^*"));
        assertThat(fromString("!@#$%^*").toLowerCase()).isEqualTo(fromString("!@#$%^*"));
        // Test composite in BinaryRowData.
        BinaryRowData row = new BinaryRowData(20);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryStringData.fromString("a"));
        writer.writeString(1, BinaryStringData.fromString("我是中国人"));
        writer.writeString(3, BinaryStringData.fromString("aBcDeFg"));
        writer.writeString(5, BinaryStringData.fromString("!@#$%^*"));
        writer.complete();

        assertThat(((BinaryStringData) row.getString(0)).toUpperCase()).isEqualTo(fromString("A"));
        assertThat(((BinaryStringData) row.getString(1)).toUpperCase())
                .isEqualTo(fromString("我是中国人"));
        assertThat(((BinaryStringData) row.getString(1)).toLowerCase())
                .isEqualTo(fromString("我是中国人"));
        assertThat(((BinaryStringData) row.getString(3)).toUpperCase())
                .isEqualTo(fromString("ABCDEFG"));
        assertThat(((BinaryStringData) row.getString(3)).toLowerCase())
                .isEqualTo(fromString("abcdefg"));
        assertThat(((BinaryStringData) row.getString(5)).toUpperCase())
                .isEqualTo(fromString("!@#$%^*"));
        assertThat(((BinaryStringData) row.getString(5)).toLowerCase())
                .isEqualTo(fromString("!@#$%^*"));
    }

    @Test
    public void testToDecimal() {
        class DecimalTestData {
            private String str;
            private int precision, scale;

            private DecimalTestData(String str, int precision, int scale) {
                this.str = str;
                this.precision = precision;
                this.scale = scale;
            }
        }

        DecimalTestData[] data = {
            new DecimalTestData("12.345", 5, 3),
            new DecimalTestData("-12.345", 5, 3),
            new DecimalTestData("+12345", 5, 0),
            new DecimalTestData("-12345", 5, 0),
            new DecimalTestData("12345.", 5, 0),
            new DecimalTestData("-12345.", 5, 0),
            new DecimalTestData(".12345", 5, 5),
            new DecimalTestData("-.12345", 5, 5),
            new DecimalTestData("+12.345E3", 5, 0),
            new DecimalTestData("-12.345e3", 5, 0),
            new DecimalTestData("12.345e-3", 6, 6),
            new DecimalTestData("-12.345E-3", 6, 6),
            new DecimalTestData("12345E3", 8, 0),
            new DecimalTestData("-12345e3", 8, 0),
            new DecimalTestData("12345e-3", 5, 3),
            new DecimalTestData("-12345E-3", 5, 3),
            new DecimalTestData("+.12345E3", 5, 2),
            new DecimalTestData("-.12345e3", 5, 2),
            new DecimalTestData(".12345e-3", 8, 8),
            new DecimalTestData("-.12345E-3", 8, 8),
            new DecimalTestData("1234512345.1234", 18, 8),
            new DecimalTestData("-1234512345.1234", 18, 8),
            new DecimalTestData("1234512345.1234", 12, 2),
            new DecimalTestData("-1234512345.1234", 12, 2),
            new DecimalTestData("1234512345.1299", 12, 2),
            new DecimalTestData("-1234512345.1299", 12, 2),
            new DecimalTestData("999999999999999999", 18, 0),
            new DecimalTestData("1234512345.1234512345", 20, 10),
            new DecimalTestData("-1234512345.1234512345", 20, 10),
            new DecimalTestData("1234512345.1234512345", 15, 5),
            new DecimalTestData("-1234512345.1234512345", 15, 5),
            new DecimalTestData("12345123451234512345E-10", 20, 10),
            new DecimalTestData("-12345123451234512345E-10", 20, 10),
            new DecimalTestData("12345123451234512345E-10", 15, 5),
            new DecimalTestData("-12345123451234512345E-10", 15, 5),
            new DecimalTestData("999999999999999999999", 21, 0),
            new DecimalTestData("-999999999999999999999", 21, 0),
            new DecimalTestData("0.00000000000000000000123456789123456789", 38, 38),
            new DecimalTestData("-0.00000000000000000000123456789123456789", 38, 38),
            new DecimalTestData("0.00000000000000000000123456789123456789", 29, 29),
            new DecimalTestData("-0.00000000000000000000123456789123456789", 29, 29),
            new DecimalTestData("123456789123E-27", 18, 18),
            new DecimalTestData("-123456789123E-27", 18, 18),
            new DecimalTestData("123456789999E-27", 18, 18),
            new DecimalTestData("-123456789999E-27", 18, 18),
            new DecimalTestData("123456789123456789E-36", 18, 18),
            new DecimalTestData("-123456789123456789E-36", 18, 18),
            new DecimalTestData("123456789999999999E-36", 18, 18),
            new DecimalTestData("-123456789999999999E-36", 18, 18)
        };

        for (DecimalTestData d : data) {
            assertThat(toDecimal(fromString(d.str), d.precision, d.scale))
                    .isEqualTo(
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(d.str), d.precision, d.scale));
        }

        BinaryRowData row = new BinaryRowData(data.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < data.length; i++) {
            writer.writeString(i, BinaryStringData.fromString(data[i].str));
        }
        writer.complete();
        for (int i = 0; i < data.length; i++) {
            DecimalTestData d = data[i];
            assertThat(toDecimal((BinaryStringData) row.getString(i), d.precision, d.scale))
                    .isEqualTo(
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(d.str), d.precision, d.scale));
        }
    }

    @Test
    public void testEmptyString() {
        BinaryStringData str2 = fromString("hahahahah");
        BinaryStringData str3;
        {
            MemorySegment[] segments = new MemorySegment[2];
            segments[0] = MemorySegmentFactory.wrap(new byte[10]);
            segments[1] = MemorySegmentFactory.wrap(new byte[10]);
            str3 = BinaryStringData.fromAddress(segments, 15, 0);
        }

        assertThat(BinaryStringData.EMPTY_UTF8.compareTo(str2)).isLessThan(0);
        assertThat(str2.compareTo(BinaryStringData.EMPTY_UTF8)).isGreaterThan(0);

        assertThat(BinaryStringData.EMPTY_UTF8.compareTo(str3)).isEqualTo(0);
        assertThat(str3.compareTo(BinaryStringData.EMPTY_UTF8)).isEqualTo(0);

        assertThat(str2).isNotEqualTo(BinaryStringData.EMPTY_UTF8);
        assertThat(BinaryStringData.EMPTY_UTF8).isNotEqualTo(str2);

        assertThat(str3).isEqualTo(BinaryStringData.EMPTY_UTF8);
        assertThat(BinaryStringData.EMPTY_UTF8).isEqualTo(str3);
    }

    @Test
    public void testEncodeWithIllegalCharacter() throws UnsupportedEncodingException {

        // Tis char array has some illegal character, such as 55357
        // the jdk ignores theses character and cast them to '?'
        // which StringUtf8Utils'encodeUTF8 should follow
        char[] chars =
                new char[] {
                    20122, 40635, 124, 38271, 34966, 124, 36830, 34915, 35033, 124, 55357, 124,
                    56407
                };

        String str = new String(chars);

        assertThat(StringUtf8Utils.encodeUTF8(str)).isEqualTo(str.getBytes("UTF-8"));
    }

    @Test
    public void testKeyValue() {
        assertThat(
                        keyValue(
                                fromString("k1:v1|k2:v2"),
                                fromString("|").byteAt(0),
                                fromString(":").byteAt(0),
                                fromString("k3")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1:v1|k2:v2|"),
                                fromString("|").byteAt(0),
                                fromString(":").byteAt(0),
                                fromString("k3")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("|k1:v1|k2:v2|"),
                                fromString("|").byteAt(0),
                                fromString(":").byteAt(0),
                                fromString("k3")))
                .isNull();
        String tab = org.apache.commons.lang3.StringEscapeUtils.unescapeJava("\t");
        assertThat(
                        keyValue(
                                fromString("k1:v1" + tab + "k2:v2"),
                                fromString("\t").byteAt(0),
                                fromString(":").byteAt(0),
                                fromString("k2")))
                .isEqualTo(fromString("v2"));
        assertThat(
                        keyValue(
                                fromString("k1:v1|k2:v2"),
                                fromString("|").byteAt(0),
                                fromString(":").byteAt(0),
                                null))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1=v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k2")))
                .isEqualTo(fromString("v2"));
        assertThat(
                        keyValue(
                                fromString("|k1=v1|k2=v2|"),
                                fromString("|").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k2")))
                .isEqualTo(fromString("v2"));
        assertThat(
                        keyValue(
                                fromString("k1=v1||k2=v2"),
                                fromString("|").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k2")))
                .isEqualTo(fromString("v2"));
        assertThat(
                        keyValue(
                                fromString("k1=v1;k2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k2")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k1")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k=1=v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k=")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1==v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k1")))
                .isEqualTo(fromString("=v1"));
        assertThat(
                        keyValue(
                                fromString("k1==v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k1=")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1=v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k1=")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1k1=v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k1")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1=v1;k2=v2"),
                                fromString(";").byteAt(0),
                                fromString("=").byteAt(0),
                                fromString("k1k1k1k1k1k1k1k1k1k1")))
                .isNull();
        assertThat(
                        keyValue(
                                fromString("k1:v||k2:v2"),
                                fromString("|").byteAt(0),
                                fromString(":").byteAt(0),
                                fromString("k2")))
                .isEqualTo(fromString("v2"));
        assertThat(
                        keyValue(
                                fromString("k1:v||k2:v2"),
                                fromString("|").byteAt(0),
                                fromString(":").byteAt(0),
                                fromString("k2")))
                .isEqualTo(fromString("v2"));
    }

    @Test
    public void testDecodeWithIllegalUtf8Bytes() throws UnsupportedEncodingException {

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
        assertThat(StringUtf8Utils.decodeUTF8(bytes, 0, bytes.length)).isEqualTo(str);
        assertThat(StringUtf8Utils.decodeUTF8(MemorySegmentFactory.wrap(bytes), 0, bytes.length))
                .isEqualTo(str);

        byte[] newBytes = new byte[bytes.length + 5];
        System.arraycopy(bytes, 0, newBytes, 5, bytes.length);
        assertThat(StringUtf8Utils.decodeUTF8(MemorySegmentFactory.wrap(newBytes), 5, bytes.length))
                .isEqualTo(str);
    }

    @Test
    public void skipWrongFirstByte() {
        int[] wrongFirstBytes = {
            0x80,
            0x9F,
            0xBF, // Skip Continuation bytes
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
        };
        byte[] c = new byte[1];

        for (int wrongFirstByte : wrongFirstBytes) {
            c[0] = (byte) wrongFirstByte;
            assertThat(1).isEqualTo(fromBytes(c).numChars());
        }
    }

    @Test
    public void testSplit() {
        assertThat(splitByWholeSeparatorPreserveAllTokens(fromString(""), fromString("")))
                .isEqualTo(EMPTY_STRING_ARRAY);
        assertThat(splitByWholeSeparatorPreserveAllTokens(fromString("ab de fg"), null))
                .isEqualTo(
                        new BinaryStringData[] {
                            fromString("ab"), fromString("de"), fromString("fg")
                        });
        assertThat(splitByWholeSeparatorPreserveAllTokens(fromString("ab   de fg"), null))
                .isEqualTo(
                        new BinaryStringData[] {
                            fromString("ab"),
                            fromString(""),
                            fromString(""),
                            fromString("de"),
                            fromString("fg")
                        });
        assertThat(splitByWholeSeparatorPreserveAllTokens(fromString("ab:cd:ef"), fromString(":")))
                .isEqualTo(
                        new BinaryStringData[] {
                            fromString("ab"), fromString("cd"), fromString("ef")
                        });
        assertThat(
                        splitByWholeSeparatorPreserveAllTokens(
                                fromString("ab-!-cd-!-ef"), fromString("-!-")))
                .isEqualTo(
                        new BinaryStringData[] {
                            fromString("ab"), fromString("cd"), fromString("ef")
                        });
    }

    @Test
    public void testLazy() {
        String javaStr = "haha";
        BinaryStringData str = BinaryStringData.fromString(javaStr);
        str.ensureMaterialized();

        // check reference same.
        assertThat(javaStr).isSameAs(str.toString());
    }
}
