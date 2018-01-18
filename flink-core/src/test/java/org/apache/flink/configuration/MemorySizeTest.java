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

package org.apache.flink.configuration;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link MemorySize} class.
 */
public class MemorySizeTest {

	@Test
	public void testUnitConversion() {
		final MemorySize zero = new MemorySize(0);
		assertEquals(0, zero.getBytes());
		assertEquals(0, zero.getKibiBytes());
		assertEquals(0, zero.getMebiBytes());
		assertEquals(0, zero.getGibiBytes());
		assertEquals(0, zero.getTebiBytes());

		final MemorySize bytes = new MemorySize(955);
		assertEquals(955, bytes.getBytes());
		assertEquals(0, bytes.getKibiBytes());
		assertEquals(0, bytes.getMebiBytes());
		assertEquals(0, bytes.getGibiBytes());
		assertEquals(0, bytes.getTebiBytes());

		final MemorySize kilos = new MemorySize(18500);
		assertEquals(18500, kilos.getBytes());
		assertEquals(18, kilos.getKibiBytes());
		assertEquals(0, kilos.getMebiBytes());
		assertEquals(0, kilos.getGibiBytes());
		assertEquals(0, kilos.getTebiBytes());

		final MemorySize megas = new MemorySize(15 * 1024 * 1024);
		assertEquals(15_728_640, megas.getBytes());
		assertEquals(15_360, megas.getKibiBytes());
		assertEquals(15, megas.getMebiBytes());
		assertEquals(0, megas.getGibiBytes());
		assertEquals(0, megas.getTebiBytes());

		final MemorySize teras = new MemorySize(2L * 1024 * 1024 * 1024 * 1024 + 10);
		assertEquals(2199023255562L, teras.getBytes());
		assertEquals(2147483648L, teras.getKibiBytes());
		assertEquals(2097152, teras.getMebiBytes());
		assertEquals(2048, teras.getGibiBytes());
		assertEquals(2, teras.getTebiBytes());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid() {
		new MemorySize(-1);
	}

	@Test
	public void testStandardUtils() throws IOException {
		final MemorySize size = new MemorySize(1234567890L);
		final MemorySize cloned = CommonTestUtils.createCopySerializable(size);

		assertEquals(size, cloned);
		assertEquals(size.hashCode(), cloned.hashCode());
		assertEquals(size.toString(), cloned.toString());
	}

	@Test
	public void testParseBytes() {
		assertEquals(1234, MemorySize.parseBytes("1234"));
		assertEquals(1234, MemorySize.parseBytes("1234b"));
		assertEquals(1234, MemorySize.parseBytes("1234 b"));
		assertEquals(1234, MemorySize.parseBytes("1234bytes"));
		assertEquals(1234, MemorySize.parseBytes("1234 bytes"));
	}

	@Test
	public void testParseKibiBytes() {
		assertEquals(667766, MemorySize.parse("667766k").getKibiBytes());
		assertEquals(667766, MemorySize.parse("667766 k").getKibiBytes());
		assertEquals(667766, MemorySize.parse("667766kb").getKibiBytes());
		assertEquals(667766, MemorySize.parse("667766 kb").getKibiBytes());
		assertEquals(667766, MemorySize.parse("667766kibibytes").getKibiBytes());
		assertEquals(667766, MemorySize.parse("667766 kibibytes").getKibiBytes());
	}

	@Test
	public void testParseMebiBytes() {
		assertEquals(7657623, MemorySize.parse("7657623m").getMebiBytes());
		assertEquals(7657623, MemorySize.parse("7657623 m").getMebiBytes());
		assertEquals(7657623, MemorySize.parse("7657623mb").getMebiBytes());
		assertEquals(7657623, MemorySize.parse("7657623 mb").getMebiBytes());
		assertEquals(7657623, MemorySize.parse("7657623mebibytes").getMebiBytes());
		assertEquals(7657623, MemorySize.parse("7657623 mebibytes").getMebiBytes());
	}

	@Test
	public void testParseGibiBytes() {
		assertEquals(987654, MemorySize.parse("987654g").getGibiBytes());
		assertEquals(987654, MemorySize.parse("987654 g").getGibiBytes());
		assertEquals(987654, MemorySize.parse("987654gb").getGibiBytes());
		assertEquals(987654, MemorySize.parse("987654 gb").getGibiBytes());
		assertEquals(987654, MemorySize.parse("987654gibibytes").getGibiBytes());
		assertEquals(987654, MemorySize.parse("987654 gibibytes").getGibiBytes());
	}

	@Test
	public void testParseTebiBytes() {
		assertEquals(1234567, MemorySize.parse("1234567t").getTebiBytes());
		assertEquals(1234567, MemorySize.parse("1234567 t").getTebiBytes());
		assertEquals(1234567, MemorySize.parse("1234567tb").getTebiBytes());
		assertEquals(1234567, MemorySize.parse("1234567 tb").getTebiBytes());
		assertEquals(1234567, MemorySize.parse("1234567tebibytes").getTebiBytes());
		assertEquals(1234567, MemorySize.parse("1234567 tebibytes").getTebiBytes());
	}

	@Test
	public void testUpperCase() {
		assertEquals(1L, MemorySize.parse("1 B").getBytes());
		assertEquals(1L, MemorySize.parse("1 K").getKibiBytes());
		assertEquals(1L, MemorySize.parse("1 M").getMebiBytes());
		assertEquals(1L, MemorySize.parse("1 G").getGibiBytes());
		assertEquals(1L, MemorySize.parse("1 T").getTebiBytes());
	}

	@Test
	public void testTrimBeforeParse() {
		assertEquals(155L, MemorySize.parseBytes("      155      "));
		assertEquals(155L, MemorySize.parseBytes("      155      bytes   "));
	}

	@Test
	public void testParseInvalid() {
		// null
		try {
			MemorySize.parseBytes(null);
			fail("exception expected");
		} catch (NullPointerException ignored) {}

		// empty
		try {
			MemorySize.parseBytes("");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {}

		// blank
		try {
			MemorySize.parseBytes("     ");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {}

		// no number
		try {
			MemorySize.parseBytes("foobar or fubar or foo bazz");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {}

		// wrong unit
		try {
			MemorySize.parseBytes("16 gjah");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {}

		// multiple numbers
		try {
			MemorySize.parseBytes("16 16 17 18 bytes");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {}

		// negative number
		try {
			MemorySize.parseBytes("-100 bytes");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseNumberOverflow() {
		MemorySize.parseBytes("100000000000000000000000000000000 bytes");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseNumberTimeUnitOverflow() {
		MemorySize.parseBytes("100000000000000 tb");
	}
}
