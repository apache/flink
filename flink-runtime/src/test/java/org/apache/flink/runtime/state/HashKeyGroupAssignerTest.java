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

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HashKeyGroupAssignerTest extends TestLogger {

	/**
	 * Makes sure that the setup method sets the number of key groups correctly.
	 */
	@Test
	public void testSetupMethod() {
		int numberKeyGroups = 42;

		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner = new HashKeyGroupAssigner<>();

		hashKeyGroupAssigner.setup(numberKeyGroups);

		assertEquals(numberKeyGroups, hashKeyGroupAssigner.getNumberKeyGroups());
	}

	/**
	 * Makes sure that the hash key group assigner cannot be set up with a negative number of key
	 * groups.
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testSetupMethodWithNegativeNumberKeyGroups() {
		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner = new HashKeyGroupAssigner<>();

		hashKeyGroupAssigner.setup(-42);

		fail("Setting up the HashKeyGroupAssigner with a negative number of key groups should " +
			"cause an exception to be thrown.");
	}

	/**
	 * Makes sure that the HashKeyGroupAssigner uses the Murmur hash algorithm to calculate the
	 * the key group assignments.
	 */
	@Test
	public void testKeyGroupIndexCalculation() {
		int numElements = 42;
		int numberKeyGroups = 13;
		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner = new HashKeyGroupAssigner<>(numberKeyGroups);

		Set<Integer> input = new HashSet<>();
		Set<Tuple2<Integer, Integer>> actual = new HashSet<>();
		Set<Tuple2<Integer, Integer>> expected = new HashSet<>();

		for (int i = 0; i < numElements; i++) {
			input.add(i);
		}

		for (Integer element: input) {
			int keyGroupIndex = MathUtils.murmurHash(element.hashCode()) % numberKeyGroups;
			expected.add(Tuple2.of(keyGroupIndex, element));
			actual.add(Tuple2.of(hashKeyGroupAssigner.getKeyGroupIndex(element), element));
		}

		assertEquals(expected, actual);
	}

	/**
	 * Makes sure that a null object is mapped to the 0 key group.
	 */
	@Test
	public void testNullKeyGroupAssignment() {
		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner = new HashKeyGroupAssigner<>(42);

		assertEquals(0, hashKeyGroupAssigner.getKeyGroupIndex(null));
	}
}
