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

package org.apache.flink.streaming.api.windowing.policy;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class MultiEvictionPolicyTest {

	private final List<Integer> tuples = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

	private final CloneableEvictionPolicy<Integer> evictionPolicy1 = new CountEvictionPolicy<Integer>(
			7, 3);
	private final CloneableEvictionPolicy<Integer> evictionPolicy2 = new CountEvictionPolicy<Integer>(
			3, 1);
	private final CloneableEvictionPolicy<Integer> evictionPolicy3 = new CountEvictionPolicy<Integer>(
			5, 2);

	private final CloneableEvictionPolicy<Integer> activeEvictionPolicy1 = new ActiveCloneableEvictionPolicyWrapper<Integer>(
			evictionPolicy1);
	private final CloneableEvictionPolicy<Integer> activeEvictionPolicy2 = new ActiveCloneableEvictionPolicyWrapper<Integer>(
			evictionPolicy2);
	private final CloneableEvictionPolicy<Integer> activeEvictionPolicy3 = new ActiveCloneableEvictionPolicyWrapper<Integer>(
			evictionPolicy3);

	// From policies specified above the expected output is:
	// 1.: 0000000300
	// 2.: 0001111111
	// 3.: 0000020202
	private final Integer[] maxResult = { 0, 0, 0, 1, 1, 2, 1, 3, 1, 2 };
	private final Integer[] minResult = { 0, 0, 0, 0, 0, 0, 0, 1, 0, 0 };
	private final Integer[] sumResult = { 0, 0, 0, 1, 1, 3, 1, 6, 1, 3 };
	private final Integer[] priorityResult = { 0, 0, 0, 1, 1, 1, 1, 3, 1, 1 };

	/*
	 * Test cases for not active policies
	 */

	@Test
	public void notActiveEvictionMAXStrategyTest() {
		runNotActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.MAX, maxResult);
	}

	@Test
	public void notActiveEvictionMINStrategyTest() {
		runNotActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.MIN, minResult);
	}

	@Test
	public void notActiveEvictionSUMStrategyTest() {
		runNotActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.SUM, sumResult);
	}

	@Test
	public void notActiveEvictionPRIORITYStrategyTest() {
		runNotActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.PRIORITY, priorityResult);
	}

	/*
	 * Test cases for active policies
	 */

	@Test
	public void activeEvictionMAXStrategyTest() {
		runActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.MAX, maxResult);
	}

	@Test
	public void activeEvictionMINStrategyTest() {
		runActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.MIN, minResult);
	}

	@Test
	public void activeEvictionSUMStrategyTest() {
		runActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.SUM, sumResult);
	}

	@Test
	public void activeEvictionPRIORITYStrategyTest() {
		runActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy.PRIORITY, priorityResult);
	}

	/**
	 * Helper method: It runs the test with the given input using the not active
	 * policies and applies the strategy defined in the parameter.
	 * 
	 * @param strategy
	 *            the eviction strategy to be used
	 * @param expectedResult
	 *            the result we expect
	 */
	private void runNotActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy strategy,
			Integer[] expectedResult) {
		@SuppressWarnings("unchecked")
		MultiEvictionPolicy<Integer> multiEviction = new MultiEvictionPolicy<Integer>(strategy,
				evictionPolicy1.clone(), evictionPolicy2.clone(), evictionPolicy3.clone());

		List<Integer> result = new LinkedList<Integer>();

		int buffersize = 0;
		for (Integer tuple : tuples) {
			// The buffer size should not matter, but we keep it for the case of
			// later policy changes.
			// The trigger does not matter. Always set it to false.
			int eviction = multiEviction.notifyEviction(tuple, false, buffersize);
			buffersize -= eviction;
			result.add(eviction);

			if (buffersize < 0) {
				buffersize = 0;
			}

			buffersize++;
		}

		arrayEqualityCheck(expectedResult, result.toArray());
	}

	/**
	 * Helper method: It runs the test with the given input using the active
	 * policies and applies the strategy defined in the parameter.
	 * 
	 * @param strategy
	 *            the eviction strategy to be used
	 * @param expectedResult
	 *            the result we expect
	 */
	private void runActiveEvictionTest(MultiEvictionPolicy.EvictionStrategy strategy,
			Integer[] expectedResult) {
		@SuppressWarnings("unchecked")
		MultiEvictionPolicy<Integer> multiEviction = new MultiEvictionPolicy<Integer>(strategy,
				activeEvictionPolicy1.clone(), activeEvictionPolicy2.clone(),
				activeEvictionPolicy3.clone());

		List<Integer> result = new LinkedList<Integer>();

		int buffersize = 0;
		for (Integer tuple : tuples) {
			// The buffer size should not matter, but we keep it for the case of
			// later policy changes.
			// The trigger does not matter. Always set it to false.
			int eviction = multiEviction.notifyEvictionWithFakeElement(tuple, buffersize);
			buffersize -= eviction;
			result.add(eviction);

			if (buffersize < 0) {
				buffersize = 0;
			}

			buffersize++;
		}

		arrayEqualityCheck(expectedResult, result.toArray());
	}

	private void arrayEqualityCheck(Object[] array1, Object[] array2) {
		assertEquals(
				"The result arrays must have the same length. (Expected: " + Arrays.asList(array1)
						+ "; Actual: " + Arrays.asList(array2) + ")", array1.length, array2.length);
		for (int i = 0; i < array1.length; i++) {
			assertEquals("Unequal fields at position " + i + "(Expected: " + Arrays.asList(array1)
					+ "; Actual: " + Arrays.asList(array2) + ")", array1[i], array2[i]);
		}
	}

}
