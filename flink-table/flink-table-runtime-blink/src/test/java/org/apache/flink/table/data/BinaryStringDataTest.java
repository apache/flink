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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test of {@link BinaryStringData}.
 *
 * <p>Caution that you must construct a string by {@link #fromString} to cover all the
 * test cases.
 *
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
		ONE_SEG, MULTI_SEGS, STRING, RANDOM
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
				string.getSegments()[0].get(
						0, bytes1, pad, segSize - pad);
			}
			string.getSegments()[0].get(segSize - pad, bytes2, 0, numBytes - segSize + pad);
			return BinaryStringData.fromAddress(
					new MemorySegment[] {
							MemorySegmentFactory.wrap(bytes1),
							MemorySegmentFactory.wrap(bytes2)
					}, pad, numBytes);
		}
	}

	private void checkBasic(String str, int len) {
		BinaryStringData s1 = fromString(str);
		BinaryStringData s2 = fromBytes(str.getBytes(StandardCharsets.UTF_8));
		assertEquals(s1.numChars(), len);
		assertEquals(s2.numChars(), len);

		assertEquals(s1.toString(), str);
		assertEquals(s2.toString(), str);
		assertEquals(s1, s2);

		assertEquals(s1.hashCode(), s2.hashCode());

		assertEquals(0, s1.compareTo(s2));

		assertTrue(s1.contains(s2));
		assertTrue(s2.contains(s1));
		assertTrue(s1.startsWith(s1));
		assertTrue(s1.endsWith(s1));
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
		assertEquals(empty, fromString(""));
		assertEquals(empty, fromBytes(new byte[0]));
		assertEquals(0, empty.numChars());
		assertEquals(0, empty.getSizeInBytes());
	}

	@Test
	public void compareTo() {
		assertEquals(0, fromString("   ").compareTo(blankString(3)));
		assertTrue(fromString("").compareTo(fromString("a")) < 0);
		assertTrue(fromString("abc").compareTo(fromString("ABC")) > 0);
		assertTrue(fromString("abc0").compareTo(fromString("abc")) > 0);
		assertEquals(0, fromString("abcabcabc").compareTo(fromString("abcabcabc")));
		assertTrue(fromString("aBcabcabc").compareTo(fromString("Abcabcabc")) > 0);
		assertTrue(fromString("Abcabcabc").compareTo(fromString("abcabcabC")) < 0);
		assertTrue(fromString("abcabcabc").compareTo(fromString("abcabcabC")) > 0);

		assertTrue(fromString("abc").compareTo(fromString("世界")) < 0);
		assertTrue(fromString("你好").compareTo(fromString("世界")) > 0);
		assertTrue(fromString("你好123").compareTo(fromString("你好122")) > 0);

		MemorySegment segment1 = MemorySegmentFactory.allocateUnpooledSegment(1024);
		MemorySegment segment2 = MemorySegmentFactory.allocateUnpooledSegment(1024);
		SortUtil.putStringNormalizedKey(fromString("abcabcabc"), segment1, 0, 9);
		SortUtil.putStringNormalizedKey(fromString("abcabcabC"), segment2, 0, 9);
		assertTrue(segment1.compare(segment2, 0, 0, 9) > 0);
		SortUtil.putStringNormalizedKey(fromString("abcab"), segment1, 0, 9);
		assertTrue(segment1.compare(segment2, 0, 0, 9) < 0);
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
		assertEquals("abcdeaaaaa", binaryString1.toString());
		assertEquals("abcdeb", binaryString2.toString());
		assertEquals(-1, binaryString1.compareTo(binaryString2));

		// test needCompare == len
		binaryString1 = BinaryStringData.fromAddress(segments1, 5, 5);
		binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
		assertEquals("abcde", binaryString1.toString());
		assertEquals("abcde", binaryString2.toString());
		assertEquals(0, binaryString1.compareTo(binaryString2));

		// test find the first segment of this string
		binaryString1 = BinaryStringData.fromAddress(segments1, 10, 5);
		binaryString2 = BinaryStringData.fromAddress(segments2, 0, 5);
		assertEquals("aaaaa", binaryString1.toString());
		assertEquals("abcde", binaryString2.toString());
		assertEquals(-1, binaryString1.compareTo(binaryString2));
		assertEquals(1, binaryString2.compareTo(binaryString1));

		// test go ahead single
		segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(new byte[10])};
		segments2[0].put(4, "abcdeb".getBytes(UTF_8), 0, 6);
		binaryString1 = BinaryStringData.fromAddress(segments1, 5, 10);
		binaryString2 = BinaryStringData.fromAddress(segments2, 4, 6);
		assertEquals("abcdeaaaaa", binaryString1.toString());
		assertEquals("abcdeb", binaryString2.toString());
		assertEquals(-1, binaryString1.compareTo(binaryString2));
		assertEquals(1, binaryString2.compareTo(binaryString1));

	}

	@Test
	public void concatTest() {
		assertEquals(empty, concat());
		assertEquals(null, concat((BinaryStringData) null));
		assertEquals(empty, concat(empty));
		assertEquals(fromString("ab"), concat(fromString("ab")));
		assertEquals(fromString("ab"), concat(fromString("a"), fromString("b")));
		assertEquals(fromString("abc"), concat(fromString("a"), fromString("b"), fromString("c")));
		assertEquals(null, concat(fromString("a"), null, fromString("c")));
		assertEquals(null, concat(fromString("a"), null, null));
		assertEquals(null, concat(null, null, null));
		assertEquals(fromString("数据砖头"), concat(fromString("数据"), fromString("砖头")));
	}

	@Test
	public void concatWsTest() {
		// Returns empty if the separator is null
		assertEquals(null, concatWs(null, (BinaryStringData) null));
		assertEquals(null, concatWs(null, fromString("a")));

		// If separator is null, concatWs should skip all null inputs and never return null.
		BinaryStringData sep = fromString("哈哈");
		assertEquals(
			empty,
			concatWs(sep, empty));
		assertEquals(
			fromString("ab"),
			concatWs(sep, fromString("ab")));
		assertEquals(
			fromString("a哈哈b"),
			concatWs(sep, fromString("a"), fromString("b")));
		assertEquals(
			fromString("a哈哈b哈哈c"),
			concatWs(sep, fromString("a"), fromString("b"), fromString("c")));
		assertEquals(
			fromString("a哈哈c"),
			concatWs(sep, fromString("a"), null, fromString("c")));
		assertEquals(
			fromString("a"),
			concatWs(sep, fromString("a"), null, null));
		assertEquals(
			empty,
			concatWs(sep, null, null, null));
		assertEquals(
			fromString("数据哈哈砖头"),
			concatWs(sep, fromString("数据"), fromString("砖头")));
	}

	@Test
	public void contains() {
		assertTrue(empty.contains(empty));
		assertTrue(fromString("hello").contains(fromString("ello")));
		assertFalse(fromString("hello").contains(fromString("vello")));
		assertFalse(fromString("hello").contains(fromString("hellooo")));
		assertTrue(fromString("大千世界").contains(fromString("千世界")));
		assertFalse(fromString("大千世界").contains(fromString("世千")));
		assertFalse(fromString("大千世界").contains(fromString("大千世界好")));
	}

	@Test
	public void startsWith() {
		assertTrue(empty.startsWith(empty));
		assertTrue(fromString("hello").startsWith(fromString("hell")));
		assertFalse(fromString("hello").startsWith(fromString("ell")));
		assertFalse(fromString("hello").startsWith(fromString("hellooo")));
		assertTrue(fromString("数据砖头").startsWith(fromString("数据")));
		assertFalse(fromString("大千世界").startsWith(fromString("千")));
		assertFalse(fromString("大千世界").startsWith(fromString("大千世界好")));
	}

	@Test
	public void endsWith() {
		assertTrue(empty.endsWith(empty));
		assertTrue(fromString("hello").endsWith(fromString("ello")));
		assertFalse(fromString("hello").endsWith(fromString("ellov")));
		assertFalse(fromString("hello").endsWith(fromString("hhhello")));
		assertTrue(fromString("大千世界").endsWith(fromString("世界")));
		assertFalse(fromString("大千世界").endsWith(fromString("世")));
		assertFalse(fromString("数据砖头").endsWith(fromString("我的数据砖头")));
	}

	@Test
	public void substring() {
		assertEquals(empty, fromString("hello").substring(0, 0));
		assertEquals(fromString("el"), fromString("hello").substring(1, 3));
		assertEquals(fromString("数"), fromString("数据砖头").substring(0, 1));
		assertEquals(fromString("据砖"), fromString("数据砖头").substring(1, 3));
		assertEquals(fromString("头"), fromString("数据砖头").substring(3, 5));
		assertEquals(fromString("ߵ梷"), fromString("ߵ梷").substring(0, 2));
	}

	@Test
	public void trims() {
		assertEquals(fromString("1"), fromString("1").trim());

		assertEquals(fromString("hello"), fromString("  hello ").trim());
		assertEquals(fromString("hello "), trimLeft(fromString("  hello ")));
		assertEquals(fromString("  hello"), trimRight(fromString("  hello ")));

		assertEquals(fromString("  hello "),
				trim(fromString("  hello "), false, false, fromString(" ")));
		assertEquals(fromString("hello"),
				trim(fromString("  hello "), true, true, fromString(" ")));
		assertEquals(fromString("hello "),
				trim(fromString("  hello "), true, false, fromString(" ")));
		assertEquals(fromString("  hello"),
				trim(fromString("  hello "), false, true, fromString(" ")));
		assertEquals(fromString("hello"),
				trim(fromString("xxxhellox"), true, true, fromString("x")));

		assertEquals(fromString("ell"),
				trim(fromString("xxxhellox"), fromString("xoh")));

		assertEquals(fromString("ellox"),
				trimLeft(fromString("xxxhellox"), fromString("xoh")));

		assertEquals(fromString("xxxhell"),
				trimRight(fromString("xxxhellox"), fromString("xoh")));

		assertEquals(empty, empty.trim());
		assertEquals(empty, fromString("  ").trim());
		assertEquals(empty, trimLeft(fromString("  ")));
		assertEquals(empty, trimRight(fromString("  ")));

		assertEquals(fromString("数据砖头"), fromString("  数据砖头 ").trim());
		assertEquals(fromString("数据砖头 "), trimLeft(fromString("  数据砖头 ")));
		assertEquals(fromString("  数据砖头"), trimRight(fromString("  数据砖头 ")));

		assertEquals(fromString("数据砖头"), fromString("数据砖头").trim());
		assertEquals(fromString("数据砖头"), trimLeft(fromString("数据砖头")));
		assertEquals(fromString("数据砖头"), trimRight(fromString("数据砖头")));

		assertEquals(fromString(","), trim(fromString("年年岁岁, 岁岁年年"), fromString("年岁 ")));
		assertEquals(fromString(", 岁岁年年"),
				trimLeft(fromString("年年岁岁, 岁岁年年"), fromString("年岁 ")));
		assertEquals(fromString("年年岁岁,"),
				trimRight(fromString("年年岁岁, 岁岁年年"), fromString("年岁 ")));

		char[] charsLessThan0x20 = new char[10];
		Arrays.fill(charsLessThan0x20, (char) (' ' - 1));
		String stringStartingWithSpace =
			new String(charsLessThan0x20) + "hello" + new String(charsLessThan0x20);
		assertEquals(fromString(stringStartingWithSpace), fromString(stringStartingWithSpace).trim());
		assertEquals(fromString(stringStartingWithSpace),
				trimLeft(fromString(stringStartingWithSpace)));
		assertEquals(fromString(stringStartingWithSpace),
				trimRight(fromString(stringStartingWithSpace)));
	}

	@Test
	public void testSqlSubstring() {
		assertEquals(fromString("ello"), substringSQL(fromString("hello"), 2));
		assertEquals(fromString("ell"), substringSQL(fromString("hello"), 2, 3));
		assertEquals(empty, substringSQL(empty, 2, 3));
		assertNull(substringSQL(fromString("hello"), 0, -1));
		assertEquals(empty, substringSQL(fromString("hello"), 10));
		assertEquals(fromString("hel"), substringSQL(fromString("hello"), 0, 3));
		assertEquals(fromString("lo"), substringSQL(fromString("hello"), -2, 3));
		assertEquals(empty, substringSQL(fromString("hello"), -100, 3));
	}

	@Test
	public void reverseTest() {
		assertEquals(fromString("olleh"), reverse(fromString("hello")));
		assertEquals(fromString("国中"), reverse(fromString("中国")));
		assertEquals(fromString("国中 ,olleh"), reverse(fromString("hello, 中国")));
		assertEquals(empty, reverse(empty));
	}

	@Test
	public void indexOf() {
		assertEquals(0, empty.indexOf(empty, 0));
		assertEquals(-1, empty.indexOf(fromString("l"), 0));
		assertEquals(0, fromString("hello").indexOf(empty, 0));
		assertEquals(2, fromString("hello").indexOf(fromString("l"), 0));
		assertEquals(3, fromString("hello").indexOf(fromString("l"), 3));
		assertEquals(-1, fromString("hello").indexOf(fromString("a"), 0));
		assertEquals(2, fromString("hello").indexOf(fromString("ll"), 0));
		assertEquals(-1, fromString("hello").indexOf(fromString("ll"), 4));
		assertEquals(1, fromString("数据砖头").indexOf(fromString("据砖"), 0));
		assertEquals(-1, fromString("数据砖头").indexOf(fromString("数"), 3));
		assertEquals(0, fromString("数据砖头").indexOf(fromString("数"), 0));
		assertEquals(3, fromString("数据砖头").indexOf(fromString("头"), 0));
	}

	@Test
	public void testToNumeric() {
		// Test to integer.
		assertEquals(Byte.valueOf("123"), toByte(fromString("123")));
		assertEquals(Byte.valueOf("123"), toByte(fromString("+123")));
		assertEquals(Byte.valueOf("-123"), toByte(fromString("-123")));

		assertEquals(Short.valueOf("123"), toShort(fromString("123")));
		assertEquals(Short.valueOf("123"), toShort(fromString("+123")));
		assertEquals(Short.valueOf("-123"), toShort(fromString("-123")));

		assertEquals(Integer.valueOf("123"), toInt(fromString("123")));
		assertEquals(Integer.valueOf("123"), toInt(fromString("+123")));
		assertEquals(Integer.valueOf("-123"), toInt(fromString("-123")));

		assertEquals(Long.valueOf("1234567890"),
				toLong(fromString("1234567890")));
		assertEquals(Long.valueOf("+1234567890"),
				toLong(fromString("+1234567890")));
		assertEquals(Long.valueOf("-1234567890"),
				toLong(fromString("-1234567890")));

		// Test decimal string to integer.
		assertEquals(Integer.valueOf("123"), toInt(fromString("123.456789")));
		assertEquals(Long.valueOf("123"), toLong(fromString("123.456789")));

		// Test negative cases.
		assertNull(toInt(fromString("1a3.456789")));
		assertNull(toInt(fromString("123.a56789")));

		// Test composite in BinaryRowData.
		BinaryRowData row = new BinaryRowData(20);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryStringData.fromString("1"));
		writer.writeString(1, BinaryStringData.fromString("123"));
		writer.writeString(2, BinaryStringData.fromString("12345"));
		writer.writeString(3, BinaryStringData.fromString("123456789"));
		writer.complete();

		assertEquals(Byte.valueOf("1"), toByte(((BinaryStringData) row.getString(0))));
		assertEquals(Short.valueOf("123"), toShort(((BinaryStringData) row.getString(1))));
		assertEquals(Integer.valueOf("12345"), toInt(((BinaryStringData) row.getString(2))));
		assertEquals(Long.valueOf("123456789"), toLong(((BinaryStringData) row.getString(3))));
	}

	@Test
	public void testToUpperLowerCase() {
		assertEquals(fromString("我是中国人"),
			fromString("我是中国人").toLowerCase());
		assertEquals(fromString("我是中国人"),
			fromString("我是中国人").toUpperCase());

		assertEquals(fromString("abcdefg"),
			fromString("aBcDeFg").toLowerCase());
		assertEquals(fromString("ABCDEFG"),
			fromString("aBcDeFg").toUpperCase());

		assertEquals(fromString("!@#$%^*"),
			fromString("!@#$%^*").toLowerCase());
		assertEquals(fromString("!@#$%^*"),
			fromString("!@#$%^*").toLowerCase());
		// Test composite in BinaryRowData.
		BinaryRowData row = new BinaryRowData(20);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryStringData.fromString("a"));
		writer.writeString(1, BinaryStringData.fromString("我是中国人"));
		writer.writeString(3, BinaryStringData.fromString("aBcDeFg"));
		writer.writeString(5, BinaryStringData.fromString("!@#$%^*"));
		writer.complete();

		assertEquals(fromString("A"), ((BinaryStringData) row.getString(0)).toUpperCase());
		assertEquals(fromString("我是中国人"), ((BinaryStringData) row.getString(1)).toUpperCase());
		assertEquals(fromString("我是中国人"), ((BinaryStringData) row.getString(1)).toLowerCase());
		assertEquals(fromString("ABCDEFG"), ((BinaryStringData) row.getString(3)).toUpperCase());
		assertEquals(fromString("abcdefg"), ((BinaryStringData) row.getString(3)).toLowerCase());
		assertEquals(fromString("!@#$%^*"), ((BinaryStringData) row.getString(5)).toUpperCase());
		assertEquals(fromString("!@#$%^*"), ((BinaryStringData) row.getString(5)).toLowerCase());
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
			assertEquals(
				DecimalData.fromBigDecimal(new BigDecimal(d.str), d.precision, d.scale),
				toDecimal(fromString(d.str), d.precision, d.scale));
		}

		BinaryRowData row = new BinaryRowData(data.length);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		for (int i = 0; i < data.length; i++) {
			writer.writeString(i, BinaryStringData.fromString(data[i].str));
		}
		writer.complete();
		for (int i = 0; i < data.length; i++) {
			DecimalTestData d = data[i];
			assertEquals(
				DecimalData.fromBigDecimal(new BigDecimal(d.str), d.precision, d.scale),
					toDecimal((BinaryStringData) row.getString(i), d.precision, d.scale));
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

		assertTrue(BinaryStringData.EMPTY_UTF8.compareTo(str2) < 0);
		assertTrue(str2.compareTo(BinaryStringData.EMPTY_UTF8) > 0);

		assertEquals(0, BinaryStringData.EMPTY_UTF8.compareTo(str3));
		assertEquals(0, str3.compareTo(BinaryStringData.EMPTY_UTF8));

		assertNotEquals(BinaryStringData.EMPTY_UTF8, str2);
		assertNotEquals(str2, BinaryStringData.EMPTY_UTF8);

		assertEquals(BinaryStringData.EMPTY_UTF8, str3);
		assertEquals(str3, BinaryStringData.EMPTY_UTF8);
	}

	@Test
	public void testEncodeWithIllegalCharacter() throws UnsupportedEncodingException {

		// Tis char array has some illegal character, such as 55357
		// the jdk ignores theses character and cast them to '?'
		// which StringUtf8Utils'encodeUTF8 should follow
		char[] chars = new char[] { 20122, 40635, 124, 38271, 34966,
			124, 36830, 34915, 35033, 124, 55357, 124, 56407 };

		String str = new String(chars);

		assertArrayEquals(
			str.getBytes("UTF-8"),
			StringUtf8Utils.encodeUTF8(str)
		);

	}

	@Test
	public void testKeyValue() {
		assertNull(keyValue(fromString("k1:v1|k2:v2"),
			fromString("|").byteAt(0),
			fromString(":").byteAt(0),
			fromString("k3")));
		assertNull(keyValue(fromString("k1:v1|k2:v2|"),
			fromString("|").byteAt(0),
			fromString(":").byteAt(0),
			fromString("k3")));
		assertNull(keyValue(fromString("|k1:v1|k2:v2|"),
			fromString("|").byteAt(0),
			fromString(":").byteAt(0),
			fromString("k3")));
		String tab = org.apache.commons.lang3.StringEscapeUtils.unescapeJava("\t");
		assertEquals(fromString("v2"),
				keyValue(fromString("k1:v1" + tab + "k2:v2"),
				fromString("\t").byteAt(0),
				fromString(":").byteAt(0),
				fromString("k2")));
		assertNull(keyValue(fromString("k1:v1|k2:v2"),
			fromString("|").byteAt(0),
			fromString(":").byteAt(0),
			null));
		assertEquals(fromString("v2"),
				keyValue(fromString("k1=v1;k2=v2"),
				fromString(";").byteAt(0),
				fromString("=").byteAt(0),
				fromString("k2")));
		assertEquals(fromString("v2"),
				keyValue(fromString("|k1=v1|k2=v2|"),
				fromString("|").byteAt(0),
				fromString("=").byteAt(0),
				fromString("k2")));
		assertEquals(fromString("v2"),
				keyValue(fromString("k1=v1||k2=v2"),
				fromString("|").byteAt(0),
				fromString("=").byteAt(0),
				fromString("k2")));
		assertNull(keyValue(fromString("k1=v1;k2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0),
			fromString("k2")));
		assertNull(keyValue(fromString("k1;k2=v2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0),
			fromString("k1")));
		assertNull(keyValue(fromString("k=1=v1;k2=v2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0),
			fromString("k=")));
		assertEquals(fromString("=v1"),
				keyValue(fromString("k1==v1;k2=v2"),
				fromString(";").byteAt(0),
				fromString("=").byteAt(0), fromString("k1")));
		assertNull(keyValue(fromString("k1==v1;k2=v2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0), fromString("k1=")));
		assertNull(keyValue(fromString("k1=v1;k2=v2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0),
			fromString("k1=")));
		assertNull(keyValue(fromString("k1k1=v1;k2=v2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0),
			fromString("k1")));
		assertNull(keyValue(fromString("k1=v1;k2=v2"),
			fromString(";").byteAt(0),
			fromString("=").byteAt(0),
			fromString("k1k1k1k1k1k1k1k1k1k1")));
		assertEquals(fromString("v2"),
				keyValue(fromString("k1:v||k2:v2"),
				fromString("|").byteAt(0),
				fromString(":").byteAt(0),
				fromString("k2")));
		assertEquals(fromString("v2"),
				keyValue(fromString("k1:v||k2:v2"),
				fromString("|").byteAt(0),
				fromString(":").byteAt(0),
				fromString("k2")));
	}

	@Test
	public void testDecodeWithIllegalUtf8Bytes() throws UnsupportedEncodingException {

		// illegal utf-8 bytes
		byte[] bytes = new byte[] {(byte) 20122, (byte) 40635, 124, (byte) 38271, (byte) 34966,
			124, (byte) 36830, (byte) 34915, (byte) 35033, 124, (byte) 55357, 124, (byte) 56407 };

		String str = new String(bytes, StandardCharsets.UTF_8);
		assertEquals(str, StringUtf8Utils.decodeUTF8(bytes, 0, bytes.length));
		assertEquals(str, StringUtf8Utils.decodeUTF8(MemorySegmentFactory.wrap(bytes), 0, bytes.length));

		byte[] newBytes = new byte[bytes.length + 5];
		System.arraycopy(bytes, 0, newBytes, 5, bytes.length);
		assertEquals(str, StringUtf8Utils.decodeUTF8(MemorySegmentFactory.wrap(newBytes), 5, bytes.length));
	}

	@Test
	public void skipWrongFirstByte() {
		int[] wrongFirstBytes = {
			0x80, 0x9F, 0xBF, // Skip Continuation bytes
			0xC0, 0xC2, // 0xC0..0xC1 - disallowed in UTF-8
			// 0xF5..0xFF - disallowed in UTF-8
			0xF5, 0xF6, 0xF7, 0xF8, 0xF9,
			0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF
		};
		byte[] c = new byte[1];

		for (int wrongFirstByte : wrongFirstBytes) {
			c[0] = (byte) wrongFirstByte;
			assertEquals(fromBytes(c).numChars(), 1);
		}
	}

	@Test
	public void testSplit() {
		assertArrayEquals(EMPTY_STRING_ARRAY,
				splitByWholeSeparatorPreserveAllTokens(fromString(""), fromString("")));
		assertArrayEquals(new BinaryStringData[] {fromString("ab"), fromString("de"), fromString("fg")},
				splitByWholeSeparatorPreserveAllTokens(fromString("ab de fg"), null));
		assertArrayEquals(new BinaryStringData[] {fromString("ab"), fromString(""), fromString(""),
						fromString("de"), fromString("fg")},
				splitByWholeSeparatorPreserveAllTokens(fromString("ab   de fg"), null));
		assertArrayEquals(new BinaryStringData[] {fromString("ab"), fromString("cd"), fromString("ef")},
				splitByWholeSeparatorPreserveAllTokens(fromString("ab:cd:ef"), fromString(":")));
		assertArrayEquals(new BinaryStringData[] {fromString("ab"), fromString("cd"), fromString("ef")},
				splitByWholeSeparatorPreserveAllTokens(fromString("ab-!-cd-!-ef"), fromString("-!-")));
	}

	@Test
	public void testLazy() {
		String javaStr = "haha";
		BinaryStringData str = BinaryStringData.fromString(javaStr);
		str.ensureMaterialized();

		// check reference same.
		assertSame(str.toString(), javaStr);
	}
}
