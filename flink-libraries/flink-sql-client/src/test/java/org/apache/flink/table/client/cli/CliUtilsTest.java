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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link CliUtils}.
 */
public class CliUtilsTest {

	@Test
	public void testRowToString() throws IOException {
		Row result = new Row(10);
		result.setField(0, null);
		result.setField(1, "String");
		result.setField(2, 'c');
		result.setField(3, false);
		result.setField(4, 12345.67f);
		result.setField(5, 12345.67d);
		result.setField(6, 12345L);
		result.setField(7, java.sql.Date.valueOf("2018-11-12"));
		result.setField(8, new int[]{1, 2});
		result.setField(9, new Row(2));
		String expected = "[null, String, c, false, "
			+ "12345.67, 12345.67, 12345, 2018-11-12, [1, 2], null,null]";
		assertEquals(expected, Arrays.toString(CliUtils.rowToString(result)));
	}

	@Test
	public void testDeepToString() {
		int[][][] array = new int[][][] {{{1, 2}}, {{10, 20, 30, 40}}, {{22, 33, 44, 55, 66}}};
		assertEquals("[[[1, 2]], [[10, 20, 30, 40]], [[22, 33, 44, 55, 66]]]",
			CliUtils.deepToString(array));

		Map arrayInsideMap = new HashMap() {{
			put(new Object[]{1, 2, 3}, new Object[]{5, 6, 7, 8});
		}};
		assertEquals("{[1, 2, 3]=[5, 6, 7, 8]}",
			CliUtils.deepToString(arrayInsideMap));

		Map arrayInsideMapWithNulls = new HashMap() {{
			put(new Object[]{1, null, 2, 3, null}, new Object[]{5, null, 6, 7});
		}};
		assertEquals(CliUtils.deepToString(arrayInsideMapWithNulls),
			"{[1, null, 2, 3, null]=[5, null, 6, 7]}");

		//could be actual in case of multisets
		Collection collection = new ArrayList() {{
			add(1);
			add(123);
			add(null);
		}};
		assertEquals("[1, 123, null]", CliUtils.deepToString(collection));
		Object[] collectionInsideArray = new Object[] {
			new ArrayList<Integer>() {{
				add(1);
				add(null);
				add(3);
			}},
			new ArrayList<Integer>() {{
				add(null);
				add(2);
			}}
		};
		assertEquals("[[1, null, 3], [null, 2]]",
			CliUtils.deepToString(collectionInsideArray));
		Object[] arrayInsideCollectionInsideArray = new Object[] {
			new ArrayList<int[]>() {{
				add(new int[]{1});
				add(new int[]{2, 5, 8});
			}},
			new ArrayList<int[]>() {{
				add(new int[]{3, 8, 13});
				add(new int[]{11, 16, 21});
			}}
		};
		assertEquals("[[[1], [2, 5, 8]], [[3, 8, 13], [11, 16, 21]]]",
			CliUtils.deepToString(arrayInsideCollectionInsideArray));
	}
}
