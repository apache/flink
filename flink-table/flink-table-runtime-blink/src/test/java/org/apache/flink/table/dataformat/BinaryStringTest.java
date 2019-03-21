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

package org.apache.flink.table.dataformat;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.sort.SortUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.dataformat.BinaryString.blankString;
import static org.apache.flink.table.dataformat.BinaryString.fromBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test of {@link BinaryString}.
 *
 * <p>Caution that you must construct a string by {@link #fromString} to cover all the
 * test cases.
 *
 */
@RunWith(Parameterized.class)
public class BinaryStringTest {

	private BinaryString empty = fromString("");

	private final Mode mode;

	public BinaryStringTest(Mode mode) {
		this.mode = mode;
	}

	@Parameterized.Parameters(name = "{0}")
	public static List<Mode> getVarSeg() {
		return Arrays.asList(Mode.ONE_SEG, Mode.MULTI_SEGS, Mode.STRING, Mode.RANDOM);
	}

	private enum Mode {
		ONE_SEG, MULTI_SEGS, STRING, RANDOM
	}

	private BinaryString fromString(String str) {
		BinaryString string = BinaryString.fromString(str);

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
			return BinaryString.fromAddress(
					new MemorySegment[] {
							MemorySegmentFactory.wrap(bytes1),
							MemorySegmentFactory.wrap(bytes2)
					}, pad, numBytes);
		}
	}

	private void checkBasic(String str, int len) {
		BinaryString s1 = fromString(str);
		BinaryString s2 = fromBytes(str.getBytes(StandardCharsets.UTF_8));
		// assertEquals(s1.numChars(), len);
		// assertEquals(s2.numChars(), len);

		assertEquals(s1.toString(), str);
		assertEquals(s2.toString(), str);
		assertEquals(s1, s2);

		assertEquals(s1.hashCode(), s2.hashCode());

		assertEquals(0, s1.compareTo(s2));

		// assertTrue(s1.contains(s2));
		// assertTrue(s2.contains(s1));
		// assertTrue(s1.startsWith(s1));
		// assertTrue(s1.endsWith(s1));
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
		// assertEquals(0, empty.numChars());
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
		BinaryString binaryString1 = BinaryString.fromAddress(segments1, 5, 10);
		BinaryString binaryString2 = BinaryString.fromAddress(segments2, 0, 6);
		assertEquals("abcdeaaaaa", binaryString1.toString());
		assertEquals("abcdeb", binaryString2.toString());
		assertEquals(-1, binaryString1.compareTo(binaryString2));

		// test needCompare == len
		binaryString1 = BinaryString.fromAddress(segments1, 5, 5);
		binaryString2 = BinaryString.fromAddress(segments2, 0, 5);
		assertEquals("abcde", binaryString1.toString());
		assertEquals("abcde", binaryString2.toString());
		assertEquals(0, binaryString1.compareTo(binaryString2));

		// test find the first segment of this string
		binaryString1 = BinaryString.fromAddress(segments1, 10, 5);
		binaryString2 = BinaryString.fromAddress(segments2, 0, 5);
		assertEquals("aaaaa", binaryString1.toString());
		assertEquals("abcde", binaryString2.toString());
		assertEquals(-1, binaryString1.compareTo(binaryString2));
		assertEquals(1, binaryString2.compareTo(binaryString1));

		// test go ahead single
		segments2 = new MemorySegment[]{MemorySegmentFactory.wrap(new byte[10])};
		segments2[0].put(4, "abcdeb".getBytes(UTF_8), 0, 6);
		binaryString1 = BinaryString.fromAddress(segments1, 5, 10);
		binaryString2 = BinaryString.fromAddress(segments2, 4, 6);
		assertEquals("abcdeaaaaa", binaryString1.toString());
		assertEquals("abcdeb", binaryString2.toString());
		assertEquals(-1, binaryString1.compareTo(binaryString2));
		assertEquals(1, binaryString2.compareTo(binaryString1));

	}

	@Test
	public void testEmptyString() {
		BinaryString str2 = fromString("hahahahah");
		BinaryString str3 = new BinaryString(null);
		{
			MemorySegment[] segments = new MemorySegment[2];
			segments[0] = MemorySegmentFactory.wrap(new byte[10]);
			segments[1] = MemorySegmentFactory.wrap(new byte[10]);
			str3.pointTo(segments, 15, 0);
		}

		assertTrue(BinaryString.EMPTY_UTF8.compareTo(str2) < 0);
		assertTrue(str2.compareTo(BinaryString.EMPTY_UTF8) > 0);

		assertTrue(BinaryString.EMPTY_UTF8.compareTo(str3) == 0);
		assertTrue(str3.compareTo(BinaryString.EMPTY_UTF8) == 0);

		assertFalse(BinaryString.EMPTY_UTF8.equals(str2));
		assertFalse(str2.equals(BinaryString.EMPTY_UTF8));

		assertTrue(BinaryString.EMPTY_UTF8.equals(str3));
		assertTrue(str3.equals(BinaryString.EMPTY_UTF8));
	}

	@Test
	public void testLazy() {
		String javaStr = "haha";
		BinaryString str = BinaryString.fromString(javaStr);
		str.ensureMaterialized();

		// check reference same.
		assertTrue(str.toString() == javaStr);
	}
}
