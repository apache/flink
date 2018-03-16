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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link StringUtils}.
 */
public class StringUtilsTest extends TestLogger {

	@Test
	public void testControlCharacters() {
		String testString = "\b \t \n \f \r default";
		String controlString = StringUtils.showControlCharacters(testString);
		assertEquals("\\b \\t \\n \\f \\r default", controlString);
	}

	@Test
	public void testArrayToString() {
		double[] array = {1.0};
		String controlString = StringUtils.arrayToString(array);
		assertEquals("[1.0]", controlString);
	}

	@Test
	public void testStringToHexArray() {
		String hex = "019f314a";
		byte[] hexArray = StringUtils.hexStringToByte(hex);
		byte[] expectedArray = new byte[]{1, -97, 49, 74 };
		assertArrayEquals(expectedArray, hexArray);
	}

	@Test
	public void testHexArrayToString() {
		byte[] byteArray = new byte[]{1, -97, 49, 74 };
		String hex = StringUtils.byteToHexString(byteArray);
		assertEquals("019f314a", hex);
	}
}
