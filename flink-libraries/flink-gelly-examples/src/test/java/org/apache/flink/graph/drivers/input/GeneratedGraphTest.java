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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToChar;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToCharValue;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToString;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToUnsignedByte;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToUnsignedByteValue;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToUnsignedInt;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToLong;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToUnsignedShort;
import org.apache.flink.graph.drivers.input.GeneratedGraph.LongValueToUnsignedShortValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GeneratedGraphTest {

	private TranslateFunction<LongValue, ByteValue> byteValueTranslator = new LongValueToUnsignedByteValue();
	private TranslateFunction<LongValue, Byte> byteTranslator = new LongValueToUnsignedByte();
	private TranslateFunction<LongValue, ShortValue> shortValueTranslator = new LongValueToUnsignedShortValue();
	private TranslateFunction<LongValue, Short> shortTranslator = new LongValueToUnsignedShort();
	private TranslateFunction<LongValue, CharValue> charValueTranslator = new LongValueToCharValue();
	private TranslateFunction<LongValue, Character> charTranslator = new LongValueToChar();
	private TranslateFunction<LongValue, Integer> intTranslator = new LongValueToUnsignedInt();
	private TranslateFunction<LongValue, Long> longTranslator = new LongValueToLong();
	private TranslateFunction<LongValue, String> stringTranslator = new LongValueToString();

	private ByteValue byteValue = new ByteValue();
	private ShortValue shortValue = new ShortValue();
	private CharValue charValue = new CharValue();

	// ByteValue

	@Test
	public void testByteValueTranslation() throws Exception {
		assertEquals(new ByteValue((byte) 0), byteValueTranslator.translate(new LongValue(0L), byteValue));
		assertEquals(new ByteValue(Byte.MIN_VALUE), byteValueTranslator.translate(new LongValue((long) Byte.MAX_VALUE + 1), byteValue));
		assertEquals(new ByteValue((byte) -1), byteValueTranslator.translate(new LongValue((1L << 8) - 1), byteValue));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testByteValueTranslationUpperOutOfRange() throws Exception {
		byteValueTranslator.translate(new LongValue(1L << 8), byteValue);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testByteValueTranslationLowerOutOfRange() throws Exception {
		byteValueTranslator.translate(new LongValue(-1), byteValue);
	}

	// Byte

	@Test
	public void testByteTranslation() throws Exception {
		assertEquals(Byte.valueOf((byte) 0), byteTranslator.translate(new LongValue(0L), null));
		assertEquals(Byte.valueOf(Byte.MIN_VALUE), byteTranslator.translate(new LongValue((long) Byte.MAX_VALUE + 1), null));
		assertEquals(Byte.valueOf((byte) -1), byteTranslator.translate(new LongValue((1L << 8) - 1), null));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testByteTranslationUpperOutOfRange() throws Exception {
		byteTranslator.translate(new LongValue(1L << 8), null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testByteTranslationLowerOutOfRange() throws Exception {
		byteTranslator.translate(new LongValue(-1), null);
	}

	// ShortValue

	@Test
	public void testShortValueTranslation() throws Exception {
		assertEquals(new ShortValue((short) 0), shortValueTranslator.translate(new LongValue(0L), shortValue));
		assertEquals(new ShortValue(Short.MIN_VALUE), shortValueTranslator.translate(new LongValue((long) Short.MAX_VALUE + 1), shortValue));
		assertEquals(new ShortValue((short) -1), shortValueTranslator.translate(new LongValue((1L << 16) - 1), shortValue));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testShortValueTranslationUpperOutOfRange() throws Exception {
		shortValueTranslator.translate(new LongValue(1L << 16), shortValue);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testShortValueTranslationLowerOutOfRange() throws Exception {
		shortValueTranslator.translate(new LongValue(-1), shortValue);
	}

	// Short

	@Test
	public void testShortTranslation() throws Exception {
		assertEquals(Short.valueOf((short) 0), shortTranslator.translate(new LongValue(0L), null));
		assertEquals(Short.valueOf(Short.MIN_VALUE), shortTranslator.translate(new LongValue((long) Short.MAX_VALUE + 1), null));
		assertEquals(Short.valueOf((short) -1), shortTranslator.translate(new LongValue((1L << 16) - 1), null));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testShortTranslationUpperOutOfRange() throws Exception {
		shortTranslator.translate(new LongValue(1L << 16), null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testShortTranslationLowerOutOfRange() throws Exception {
		shortTranslator.translate(new LongValue(-1), null);
	}

	// CharValue

	@Test
	public void testCharValueTranslation() throws Exception {
		assertEquals(new CharValue((char) 0), charValueTranslator.translate(new LongValue(0L), charValue));
		assertEquals(new CharValue(Character.MAX_VALUE), charValueTranslator.translate(new LongValue((long) Character.MAX_VALUE), charValue));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testCharValueTranslationUpperOutOfRange() throws Exception {
		charValueTranslator.translate(new LongValue(1L << 16), charValue);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testCharValueTranslationLowerOutOfRange() throws Exception {
		charValueTranslator.translate(new LongValue(-1), charValue);
	}

	// Character

	@Test
	public void testCharacterTranslation() throws Exception {
		assertEquals(Character.valueOf((char) 0), charTranslator.translate(new LongValue(0L), null));
		assertEquals(Character.valueOf(Character.MAX_VALUE), charTranslator.translate(new LongValue((long) Character.MAX_VALUE), null));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testCharacterTranslationUpperOutOfRange() throws Exception {
		charTranslator.translate(new LongValue(1L << 16), null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testCharacterTranslationLowerOutOfRange() throws Exception {
		charTranslator.translate(new LongValue(-1), null);
	}

	// Integer

	@Test
	public void testIntegerTranslation() throws Exception {
		assertEquals(Integer.valueOf(0), intTranslator.translate(new LongValue(0L), null));
		assertEquals(Integer.valueOf(Integer.MIN_VALUE), intTranslator.translate(new LongValue((long) Integer.MAX_VALUE + 1), null));
		assertEquals(Integer.valueOf(-1), intTranslator.translate(new LongValue((1L << 32) - 1), null));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testIntegerTranslationUpperOutOfRange() throws Exception {
		intTranslator.translate(new LongValue(1L << 32), null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testIntegerTranslationLowerOutOfRange() throws Exception {
		intTranslator.translate(new LongValue(-1), null);
	}

	// Long

	@Test
	public void testLongTranslation() throws Exception {
		assertEquals(Long.valueOf(0L), longTranslator.translate(new LongValue(0L), null));
		assertEquals(Long.valueOf(Long.MIN_VALUE), longTranslator.translate(new LongValue(Long.MIN_VALUE), null));
		assertEquals(Long.valueOf(Long.MAX_VALUE), longTranslator.translate(new LongValue(Long.MAX_VALUE), null));
	}

	// String

	@Test
	public void testStringTranslation() throws Exception {
		assertEquals("0", stringTranslator.translate(new LongValue(0L), null));
		assertEquals("-9223372036854775808", stringTranslator.translate(new LongValue(Long.MIN_VALUE), null));
		assertEquals("9223372036854775807", stringTranslator.translate(new LongValue(Long.MAX_VALUE), null));
	}
}
