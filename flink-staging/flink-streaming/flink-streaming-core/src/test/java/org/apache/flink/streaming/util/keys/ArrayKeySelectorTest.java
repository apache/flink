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

package org.apache.flink.streaming.util.keys;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static org.junit.Assert.*;

public class ArrayKeySelectorTest {

	@Test
	public void testObjectArrays() {
		try {
			Object[] array1 = { "a", "b", "c", "d", "e" };
			Object[] array2 = { "v", "w", "x", "y", "z" };
			
			KeySelectorUtil.ArrayKeySelector<Object[]> singleFieldSelector = new KeySelectorUtil.ArrayKeySelector<>(1);
			
			assertEquals(new Tuple1<>("b"), singleFieldSelector.getKey(array1));
			assertEquals(new Tuple1<>("w"), singleFieldSelector.getKey(array2));

			KeySelectorUtil.ArrayKeySelector<Object[]> twoFieldsSelector = new KeySelectorUtil.ArrayKeySelector<>(3, 0);
			
			assertEquals(new Tuple2<>("d", "a"), twoFieldsSelector.getKey(array1));
			assertEquals(new Tuple2<>("y", "v"), twoFieldsSelector.getKey(array2));
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPrimitiveArrays() {
		try {
			int[] array1 = { 1, 2, 3, 4, 5 };
			int[] array2 = { -5, -4, -3, -2, -1, 0 };

			KeySelectorUtil.ArrayKeySelector<int[]> singleFieldSelector = new KeySelectorUtil.ArrayKeySelector<>(1);

			assertEquals(new Tuple1<>(2), singleFieldSelector.getKey(array1));
			assertEquals(new Tuple1<>(-4), singleFieldSelector.getKey(array2));

			KeySelectorUtil.ArrayKeySelector<int[]> twoFieldsSelector = new KeySelectorUtil.ArrayKeySelector<>(3, 0);

			assertEquals(new Tuple2<>(4, 1), twoFieldsSelector.getKey(array1));
			assertEquals(new Tuple2<>(-2, -5), twoFieldsSelector.getKey(array2));

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
