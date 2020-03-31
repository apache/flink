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

package org.apache.flink.table.client.cli;

import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link CliUtils}.
 */
public class CliUtilsTest {

	@Test
	public void testArrayToString() {
		Row row = new Row(4);
		row.setField(0, new int[]{1, 2});
		row.setField(1, new Integer[]{3, 4});
		row.setField(2, new Object[]{new int[]{5, 6}, new int[]{7, 8}});
		row.setField(3, new Integer[][]{new Integer[]{9, 10}, new Integer[]{11, 12}});
		assertEquals("[[1, 2], [3, 4], [[5, 6], [7, 8]], [[9, 10], [11, 12]]]", Arrays.toString(CliUtils.rowToString(row)));
	}

	@Test
	public void testCharFullWidth() {
		char[] chars = new char[] {'A', 'a', ',', '中', '，', 'こ'};
		boolean[] expected = new boolean[] {false, false, false, true, true, true};

		for (int i = 0; i < chars.length; i++) {
			assertEquals(expected[i], CliUtils.isFullWidth(Character.codePointAt(chars, i)));
		}
	}

	@Test
	public void testStringDisplayWidth() {
		List<String> data = Arrays.asList(
			"abcdefg,12345,ABC",
			"to be or not to be that's a question.",
			"这是一段中文",
			"これは日本語をテストするための文です");
		int[] expected = new int[] {17, 37, 12, 36};

		for (int i = 0; i < data.size(); i++) {
			assertEquals(expected[i], CliUtils.getStringDisplayWidth(data.get(i)));
		}
	}
}
