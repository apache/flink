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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class WindowInvokableTest {

	/**
	 * Test case equal to {@link WindowReduceInvokableTest}
	 */
	@Test
	public void testWindowInvokableWithTimePolicy() {

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		inputs.add(3);
		inputs.add(4);
		inputs.add(5);
		inputs.add(10);
		inputs.add(11);
		inputs.add(11);
		// 1,2,3,4-3,4,5,6-5,6,7,8-7,8,9,10-9,10,11
		// 12-12-5-10-32

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(12);
		expected.add(12);
		expected.add(5);
		expected.add(10);
		expected.add(32);

		Timestamp<Integer> myTimeStamp = new Timestamp<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}
		};

		ReduceFunction<Integer> myReduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		};

		LinkedList<TriggerPolicy<Integer>> triggers = new LinkedList<TriggerPolicy<Integer>>();
		// Trigger every 2 time units but delay the first trigger by 2 (First
		// trigger after 4, then every 2)
		triggers.add(new TimeTriggerPolicy<Integer>(2L, new TimestampWrapper<Integer>(myTimeStamp,
				1), 2L));

		LinkedList<EvictionPolicy<Integer>> evictions = new LinkedList<EvictionPolicy<Integer>>();
		// Always delete all elements older then 4
		evictions.add(new TimeEvictionPolicy<Integer>(4L, new TimestampWrapper<Integer>(
				myTimeStamp, 1)));

		WindowInvokable<Integer, Integer> invokable = new WindowReduceInvokable<Integer>(
				myReduceFunction, triggers, evictions);

		ArrayList<Integer> result = new ArrayList<Integer>();
		for (Integer t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}

		assertEquals(expected, result);
	}

	/**
	 * Test case equal to {@link BatchReduceTest}
	 */
	@Test
	public void testWindowInvokableWithCountPolicy() {

		List<Integer> inputs = new ArrayList<Integer>();
		for (Integer i = 1; i <= 10; i++) {
			inputs.add(i);
		}

		ReduceFunction<Integer> myReduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		};

		/*
		 * The following setup reassembles the batch size 3 and the slide size 2
		 * of the BatchReduceInvokable.
		 */
		LinkedList<TriggerPolicy<Integer>> triggers = new LinkedList<TriggerPolicy<Integer>>();
		// Trigger on every 2nd element, but the first time after the 3rd
		triggers.add(new CountTriggerPolicy<Integer>(2, -1));

		LinkedList<EvictionPolicy<Integer>> evictions = new LinkedList<EvictionPolicy<Integer>>();
		// On every 2nd element, remove the oldest 2 elements, but the first
		// time after the 3rd element
		evictions.add(new CountEvictionPolicy<Integer>(2, 2, -1));

		WindowInvokable<Integer, Integer> invokable = new WindowReduceInvokable<Integer>(
				myReduceFunction, triggers, evictions);

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(6);
		expected.add(12);
		expected.add(18);
		expected.add(24);
		expected.add(19);
		List<Integer> result = new ArrayList<Integer>();
		for (Integer t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}
		assertEquals(expected, result);

		/*
		 * Begin test part 2
		 */

		List<Integer> inputs2 = new ArrayList<Integer>();
		inputs2.add(1);
		inputs2.add(2);
		inputs2.add(-5); // changed this value to make sure it is excluded from
							// the result
		inputs2.add(-3);
		inputs2.add(-4);

		myReduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				if (value1 <= value2) {
					return value1;
				} else {
					return value2;
				}
			};
		};

		/*
		 * The following setup reassembles the batch size 2 and the slide size 3
		 * of the BatchReduceInvokable.
		 */
		triggers = new LinkedList<TriggerPolicy<Integer>>();
		// Trigger after every 3rd element, but the first time after the 2nd
		triggers.add(new CountTriggerPolicy<Integer>(3, 1));

		evictions = new LinkedList<EvictionPolicy<Integer>>();
		// On every 3rd element, remove the oldest 3 elements, but the first
		// time after on the 5th element
		evictions.add(new CountEvictionPolicy<Integer>(3, 3, -1));

		WindowInvokable<Integer, Integer> invokable2 = new WindowReduceInvokable<Integer>(
				myReduceFunction, triggers, evictions);

		List<Integer> expected2 = new ArrayList<Integer>();
		expected2.add(1);
		expected2.add(-4);

		result = new ArrayList<Integer>();
		for (Integer t : MockContext.createAndExecute(invokable2, inputs2)) {
			result.add(t);
		}

		assertEquals(expected2, result);

	}

	@Test
	public void testWindowInvokableWithMultiplePolicies() {
		LinkedList<TriggerPolicy<Integer>> triggers = new LinkedList<TriggerPolicy<Integer>>();
		triggers.add(new CountTriggerPolicy<Integer>(2));
		triggers.add(new CountTriggerPolicy<Integer>(3));

		LinkedList<EvictionPolicy<Integer>> evictions = new LinkedList<EvictionPolicy<Integer>>();
		evictions.add(new CountEvictionPolicy<Integer>(2, 2));
		evictions.add(new CountEvictionPolicy<Integer>(3, 3));

		List<Integer> inputs = new ArrayList<Integer>();
		for (Integer i = 1; i <= 10; i++) {
			inputs.add(i);
		}
		/**
		 * <code>
		 * VAL: 1,2,3,4,5,6,7,8,9,10
		 * TR1:   |   |   |   |   |
		 * TR2:     |     |     |
		 * EV1:   2   2   2   2   2
		 * EV2:     3     3     3
		 * </code>
		 */

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(3);
		expected.add(3);
		expected.add(4);
		expected.add(11);
		expected.add(15);
		expected.add(9);
		expected.add(10);

		ReduceFunction<Integer> myReduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		};

		WindowInvokable<Integer, Integer> invokable = new WindowReduceInvokable<Integer>(
				myReduceFunction, triggers, evictions);

		ArrayList<Integer> result = new ArrayList<Integer>();
		for (Integer t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}

		assertEquals(expected, result);
	}

}
