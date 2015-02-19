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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.junit.Test;

public class MultiTriggerPolicyTest {

	/**
	 * This test covers all regular notify call. It takes no fake elements into
	 * account.
	 */
	@Test
	public void testWithoutActivePolicies() {
		List<Integer> tuples = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
				16);

		TriggerPolicy<Integer> firstPolicy = new CountTriggerPolicy<Integer>(3);
		TriggerPolicy<Integer> secondPolicy = new CountTriggerPolicy<Integer>(5);
		TriggerPolicy<Integer> thirdPolicy = new CountTriggerPolicy<Integer>(8);
		@SuppressWarnings("unchecked")
		TriggerPolicy<Integer> multiTrigger = new MultiTriggerPolicy<Integer>(firstPolicy,
				secondPolicy, thirdPolicy);

		// From above policies the expected output is (first element is 0):
		// first: 3, 6, 9, 12, 15,...
		// second: 5, 10, 15,...
		// third: 8, 16, 24,...
		// combination: 3,5,6,8,9,10,12,15,16
		List<Integer> expectedResult = Arrays.asList(3, 5, 6, 8, 9, 10, 12, 15, 16);
		List<Integer> actualResult = new LinkedList<Integer>();

		for (int i = 0; i < tuples.size(); i++) {
			if (multiTrigger.notifyTrigger(tuples.get(i))) {
				actualResult.add(i);
			}
		}

		// check equal sizes
		assertTrue("The expected result list and the actual result list must have the same size,"
				+ " but they are different. (expected: " + expectedResult.size() + "; actual: "
				+ actualResult.size() + "). Actual result is: " + actualResult
				+ " Expected result is: " + expectedResult,
				expectedResult.size() == actualResult.size());

		// check equal elements within result list/expected list
		for (int i = 0; i < expectedResult.size(); i++) {
			assertTrue("The actual and the expected result does not match at position " + i
					+ ". (expected: " + expectedResult.get(i) + "; actual: " + actualResult.get(i)
					+ "). Actual result is: " + actualResult + " Expected result is: "
					+ expectedResult, expectedResult.get(i) == actualResult.get(i));
		}
	}

	/**
	 * This test covers the pre-notify calls to active policies. I takes no
	 * regular notify into account.
	 */
	@Test
	public void testWithActivePolicies() {

		// create some test data
		Integer[] times = { 1, 3, 20, 26 };

		// create a timestamp
		@SuppressWarnings("serial")
		Timestamp<Integer> timeStamp = new Timestamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

		};

		// create policy
		TimeTriggerPolicy<Integer> firstPolicy = new TimeTriggerPolicy<Integer>(5,
				new TimestampWrapper<Integer>(timeStamp, 0));
		TimeTriggerPolicy<Integer> secondPolicy = new TimeTriggerPolicy<Integer>(10,
				new TimestampWrapper<Integer>(timeStamp, 0));
		TimeTriggerPolicy<Integer> thirdPolicy = new TimeTriggerPolicy<Integer>(22,
				new TimestampWrapper<Integer>(timeStamp, 0));
		@SuppressWarnings("unchecked")
		MultiTriggerPolicy<Integer> multiTrigger = new MultiTriggerPolicy<Integer>(firstPolicy,
				secondPolicy, thirdPolicy);

		// expected result
		// Long[][] result1 = { {}, {}, { 4L, 9L, 14L, 19L }, { 24L } };
		// Long[][] result2 = { {}, {}, { 9L, 19L }, { } };
		// Long[][] result3 = { {}, {}, { }, { 21L } };
		Long[][] result = { {}, {}, { 4L, 9L, 14L, 19L, 9L, 19L }, { 24L, 21L } };

		// call policy
		for (int i = 0; i < times.length; i++) {
			arrayEqualityCheck(result[i], multiTrigger.preNotifyTrigger(times[i]));
			multiTrigger.notifyTrigger(times[i]);
		}
	}

	/**
	 * This test verifies, that nestet active trigger runnables are started
	 * correctly.
	 */
	@Test
	public void testActiveTriggerRunnables() {
		TriggerPolicy<Integer> firstPolicy = new ActiveTriggerWithRunnable(1);
		TriggerPolicy<Integer> secondPolicy = new ActiveTriggerWithRunnable(2);
		TriggerPolicy<Integer> thirdPolicy = new ActiveTriggerWithRunnable(3);
		@SuppressWarnings("unchecked")
		ActiveTriggerPolicy<Integer> multiTrigger = new MultiTriggerPolicy<Integer>(firstPolicy,
				secondPolicy, thirdPolicy);

		MyCallbackClass cb = new MyCallbackClass();
		Runnable runnable = multiTrigger.createActiveTriggerRunnable(cb);
		new Thread(runnable).start();

		assertTrue("Even after 10000ms not all active policy runnables were started.",
				cb.check(10000, 1, 2, 3));
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

	/**
	 * This helper class is used to simulate active triggers which produce own
	 * runnables.
	 */
	@SuppressWarnings("serial")
	private class ActiveTriggerWithRunnable implements ActiveTriggerPolicy<Integer> {

		int id;

		public ActiveTriggerWithRunnable(int id) {
			this.id = id;
		}

		@Override
		public boolean notifyTrigger(Integer datapoint) {
			// This method is not uses for any test case
			return false;
		}

		@Override
		public Object[] preNotifyTrigger(Integer datapoint) {
			// This method is not used for any test case
			return null;
		}

		@Override
		public Runnable createActiveTriggerRunnable(final ActiveTriggerCallback callback) {
			return new Runnable() {
				@Override
				public void run() {
					callback.sendFakeElement(id);
				}
			};
		}
	}

	/**
	 * This callback class is used to checked whether all nested policy runnable
	 * started up.
	 */
	private class MyCallbackClass implements ActiveTriggerCallback {

		List<Integer> received = new LinkedList<Integer>();

		@Override
		public void sendFakeElement(Object datapoint) {
			received.add((Integer) datapoint);
		}

		public boolean check(int timeout, int... ids) {
			int totalTime = 0;

			while (totalTime <= timeout) {
				boolean result = true;
				for (int id : ids) {
					if (!received.contains(id)) {
						result = false;
					}
				}

				if (result) {
					return true;
				} else {
					try {
						Thread.sleep(1000);
						totalTime += 1000;
					} catch (InterruptedException e) {
						// ignore it here
					}
				}
			}
			return false;
		}

	}
}