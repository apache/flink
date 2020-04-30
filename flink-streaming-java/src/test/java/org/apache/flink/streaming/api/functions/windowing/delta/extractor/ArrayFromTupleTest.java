/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Tuple} to {@code Array}.
 */
public class ArrayFromTupleTest {

	private String[] testStrings;

	@Before
	public void init() {
		testStrings = new String[Tuple.MAX_ARITY];
		for (int i = 0; i < Tuple.MAX_ARITY; i++) {
			testStrings[i] = Integer.toString(i);
		}
	}

	@Test
	public void testConvertFromTupleToArray() throws InstantiationException, IllegalAccessException {
		for (int i = 0; i < Tuple.MAX_ARITY; i++) {
			Tuple currentTuple = (Tuple) CLASSES[i].newInstance();
			String[] currentArray = new String[i + 1];
			for (int j = 0; j <= i; j++) {
				currentTuple.setField(testStrings[j], j);
				currentArray[j] = testStrings[j];
			}
			arrayEqualityCheck(currentArray, new ArrayFromTuple().extract(currentTuple));
		}
	}

	@Test
	public void testUserSpecifiedOrder() throws InstantiationException, IllegalAccessException {
		Tuple currentTuple = (Tuple) CLASSES[Tuple.MAX_ARITY - 1].newInstance();
		for (int i = 0; i < Tuple.MAX_ARITY; i++) {
			currentTuple.setField(testStrings[i], i);
		}

		String[] expected = { testStrings[5], testStrings[3], testStrings[6], testStrings[7],
				testStrings[0] };
		arrayEqualityCheck(expected, new ArrayFromTuple(5, 3, 6, 7, 0).extract(currentTuple));

		String[] expected2 = { testStrings[0], testStrings[Tuple.MAX_ARITY - 1] };
		arrayEqualityCheck(expected2,
				new ArrayFromTuple(0, Tuple.MAX_ARITY - 1).extract(currentTuple));

		String[] expected3 = { testStrings[Tuple.MAX_ARITY - 1], testStrings[0] };
		arrayEqualityCheck(expected3,
				new ArrayFromTuple(Tuple.MAX_ARITY - 1, 0).extract(currentTuple));

		String[] expected4 = { testStrings[13], testStrings[4], testStrings[5], testStrings[4],
				testStrings[2], testStrings[8], testStrings[6], testStrings[2], testStrings[8],
				testStrings[3], testStrings[5], testStrings[2], testStrings[16], testStrings[4],
				testStrings[3], testStrings[2], testStrings[6], testStrings[4], testStrings[7],
				testStrings[4], testStrings[2], testStrings[8], testStrings[7], testStrings[2] };
		arrayEqualityCheck(expected4, new ArrayFromTuple(13, 4, 5, 4, 2, 8, 6, 2, 8, 3, 5, 2, 16,
				4, 3, 2, 6, 4, 7, 4, 2, 8, 7, 2).extract(currentTuple));
	}

	private void arrayEqualityCheck(Object[] array1, Object[] array2) {
		assertEquals("The result arrays must have the same length", array1.length, array2.length);
		for (int i = 0; i < array1.length; i++) {
			assertEquals("Unequal fields at position " + i, array1[i], array2[i]);
		}
	}

	private static final Class<?>[] CLASSES = new Class<?>[] { Tuple1.class, Tuple2.class,
			Tuple3.class, Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class,
			Tuple9.class, Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class,
			Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class,
			Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class, Tuple23.class,
			Tuple24.class, Tuple25.class };
}
