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

package org.apache.flink.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TimeUtils}.
 */
public class TimeUtilsTest {

	@Test
	public void testParseDurationMillis() {
		assertEquals(1234, TimeUtils.parseDuration("1234").toMillis());
		assertEquals(1234, TimeUtils.parseDuration("1234ms").toMillis());
		assertEquals(1234, TimeUtils.parseDuration("1234 ms").toMillis());
	}

	@Test
	public void testParseDurationSeconds() {
		assertEquals(667766, TimeUtils.parseDuration("667766s").getSeconds());
		assertEquals(667766, TimeUtils.parseDuration("667766 s").getSeconds());
	}

	@Test
	public void testParseDurationMinutes() {
		assertEquals(7657623, TimeUtils.parseDuration("7657623min").toMinutes());
		assertEquals(7657623, TimeUtils.parseDuration("7657623 min").toMinutes());
	}

	@Test
	public void testParseDurationHours() {
		assertEquals(987654, TimeUtils.parseDuration("987654h").toHours());
		assertEquals(987654, TimeUtils.parseDuration("987654 h").toHours());
	}

	@Test
	public void testParseDurationUpperCase() {
		assertEquals(1L, TimeUtils.parseDuration("1 MS").toMillis());
		assertEquals(1L, TimeUtils.parseDuration("1 S").getSeconds());
		assertEquals(1L, TimeUtils.parseDuration("1 MIN").toMinutes());
		assertEquals(1L, TimeUtils.parseDuration("1 H").toHours());
	}

	@Test
	public void testParseDurationTrim() {
		assertEquals(155L, TimeUtils.parseDuration("      155      ").toMillis());
		assertEquals(155L, TimeUtils.parseDuration("      155      ms   ").toMillis());
	}

	@Test
	public void testParseDurationInvalid() {
		// null
		try {
			TimeUtils.parseDuration(null);
			fail("exception expected");
		} catch (NullPointerException ignored) {
		}

		// empty
		try {
			TimeUtils.parseDuration("");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// blank
		try {
			TimeUtils.parseDuration("     ");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// no number
		try {
			TimeUtils.parseDuration("foobar or fubar or foo bazz");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// wrong unit
		try {
			TimeUtils.parseDuration("16 gjah");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// multiple numbers
		try {
			TimeUtils.parseDuration("16 16 17 18 ms");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// negative number
		try {
			TimeUtils.parseDuration("-100 ms");
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseDurationNumberOverflow() {
		TimeUtils.parseDuration("100000000000000000000000000000000 ms");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseDurationNumberTimeUnitOverflow() {
		TimeUtils.parseDuration("100000000000000000 h");
	}
}
