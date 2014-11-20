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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.ActiveCloneableEvictionPolicyWrapper;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.util.MockInvokable;
import org.apache.flink.streaming.util.keys.TupleKeySelector;
import org.junit.Test;

public class GroupedWindowingInvokableTest {

	/**
	 * Test for not active distributed triggers with single field
	 */
	@Test
	public void testGroupedWindowingInvokableDistributedTriggerSimple() {
		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(1);
		inputs.add(5);
		inputs.add(5);
		inputs.add(5);
		inputs.add(1);
		inputs.add(1);
		inputs.add(5);
		inputs.add(1);
		inputs.add(5);

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(15);
		expected.add(3);
		expected.add(3);
		expected.add(15);

		LinkedList<CloneableTriggerPolicy<Integer>> triggers = new LinkedList<CloneableTriggerPolicy<Integer>>();
		// Trigger on every 2nd element, but the first time after the 3rd
		triggers.add(new CountTriggerPolicy<Integer>(2, -1));

		LinkedList<CloneableEvictionPolicy<Integer>> evictions = new LinkedList<CloneableEvictionPolicy<Integer>>();
		// On every 2nd element, remove the oldest 2 elements, but the first
		// time after the 3rd element
		evictions.add(new CountEvictionPolicy<Integer>(2, 2, -1));

		LinkedList<TriggerPolicy<Integer>> centralTriggers = new LinkedList<TriggerPolicy<Integer>>();

		GroupedWindowingInvokable<Integer> invokable = new GroupedWindowingInvokable<Integer>(
				new ReduceFunction<Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer reduce(Integer value1, Integer value2) throws Exception {
						return value1 + value2;
					}
				}, new KeySelector<Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Integer value) {
						return value;
					}
				}, triggers, evictions, centralTriggers);

		List<Tuple2<Integer, String[]>> result = MockInvokable.createAndExecute(invokable, inputs);

		List<Integer> actual = new LinkedList<Integer>();
		for (Tuple2<Integer, String[]> current : result) {
			actual.add(current.f0);
		}

		assertEquals(new HashSet<Integer>(expected), new HashSet<Integer>(actual));
		assertEquals(expected.size(), actual.size());
	}

	/**
	 * Test for non active distributed triggers with separated key field
	 */
	@Test
	public void testGroupedWindowingInvokableDistributedTriggerComplex() {
		List<Tuple2<Integer, String>> inputs2 = new ArrayList<Tuple2<Integer, String>>();
		inputs2.add(new Tuple2<Integer, String>(1, "a"));
		inputs2.add(new Tuple2<Integer, String>(0, "b"));
		inputs2.add(new Tuple2<Integer, String>(2, "a"));
		inputs2.add(new Tuple2<Integer, String>(-1, "a"));
		inputs2.add(new Tuple2<Integer, String>(-2, "a"));
		inputs2.add(new Tuple2<Integer, String>(10, "a"));
		inputs2.add(new Tuple2<Integer, String>(2, "b"));
		inputs2.add(new Tuple2<Integer, String>(1, "a"));

		List<Tuple2<Integer, String>> expected2 = new ArrayList<Tuple2<Integer, String>>();
		expected2.add(new Tuple2<Integer, String>(-1, "a"));
		expected2.add(new Tuple2<Integer, String>(-2, "a"));
		expected2.add(new Tuple2<Integer, String>(0, "b"));

		LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>> triggers = new LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>>();
		// Trigger on every 2nd element, but the first time after the 3rd
		triggers.add(new CountTriggerPolicy<Tuple2<Integer, String>>(3));

		LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>> evictions = new LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>>();
		// On every 2nd element, remove the oldest 2 elements, but the first
		// time after the 3rd element
		evictions.add(new TumblingEvictionPolicy<Tuple2<Integer, String>>());

		LinkedList<TriggerPolicy<Tuple2<Integer, String>>> centralTriggers = new LinkedList<TriggerPolicy<Tuple2<Integer, String>>>();

		GroupedWindowingInvokable<Tuple2<Integer, String>> invokable2 = new GroupedWindowingInvokable<Tuple2<Integer, String>>(
				new ReduceFunction<Tuple2<Integer, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1,
							Tuple2<Integer, String> value2) throws Exception {
						if (value1.f0 <= value2.f0) {
							return value1;
						} else {
							return value2;
						}
					}
				}, new TupleKeySelector<Tuple2<Integer, String>>(1), triggers, evictions,
				centralTriggers);

		List<Tuple2<Tuple2<Integer, String>, String[]>> result = MockInvokable.createAndExecute(
				invokable2, inputs2);

		List<Tuple2<Integer, String>> actual2 = new LinkedList<Tuple2<Integer, String>>();
		for (Tuple2<Tuple2<Integer, String>, String[]> current : result) {
			actual2.add(current.f0);
		}

		assertEquals(new HashSet<Tuple2<Integer, String>>(expected2),
				new HashSet<Tuple2<Integer, String>>(actual2));
		assertEquals(expected2.size(), actual2.size());
	}

	/**
	 * Test for active centralized trigger
	 */
	@Test
	public void testGroupedWindowingInvokableCentralActiveTrigger() {

		List<Tuple2<Integer, String>> inputs = new ArrayList<Tuple2<Integer, String>>();
		inputs.add(new Tuple2<Integer, String>(1, "a"));
		inputs.add(new Tuple2<Integer, String>(1, "b"));
		inputs.add(new Tuple2<Integer, String>(1, "c"));
		inputs.add(new Tuple2<Integer, String>(2, "a"));
		inputs.add(new Tuple2<Integer, String>(2, "b"));
		inputs.add(new Tuple2<Integer, String>(2, "c"));
		inputs.add(new Tuple2<Integer, String>(2, "b"));
		inputs.add(new Tuple2<Integer, String>(2, "a"));
		inputs.add(new Tuple2<Integer, String>(2, "c"));
		inputs.add(new Tuple2<Integer, String>(3, "c"));
		inputs.add(new Tuple2<Integer, String>(3, "a"));
		inputs.add(new Tuple2<Integer, String>(3, "b"));
		inputs.add(new Tuple2<Integer, String>(4, "a"));
		inputs.add(new Tuple2<Integer, String>(4, "b"));
		inputs.add(new Tuple2<Integer, String>(4, "c"));
		inputs.add(new Tuple2<Integer, String>(5, "c"));
		inputs.add(new Tuple2<Integer, String>(5, "a"));
		inputs.add(new Tuple2<Integer, String>(5, "b"));
		inputs.add(new Tuple2<Integer, String>(10, "b"));
		inputs.add(new Tuple2<Integer, String>(10, "a"));
		inputs.add(new Tuple2<Integer, String>(10, "c"));
		inputs.add(new Tuple2<Integer, String>(11, "a"));
		inputs.add(new Tuple2<Integer, String>(11, "a"));
		inputs.add(new Tuple2<Integer, String>(11, "c"));
		inputs.add(new Tuple2<Integer, String>(11, "c"));
		inputs.add(new Tuple2<Integer, String>(11, "b"));
		inputs.add(new Tuple2<Integer, String>(11, "b"));

		// Expected result:
		// For each group (a,b and c):
		// 1,2,3,4-3,4,5,6-5,6,7,8-7,8,9,10-9,10,11
		// 12-12-5-10-32

		List<Tuple2<Integer, String>> expected = new ArrayList<Tuple2<Integer, String>>();
		expected.add(new Tuple2<Integer, String>(12, "a"));
		expected.add(new Tuple2<Integer, String>(12, "b"));
		expected.add(new Tuple2<Integer, String>(12, "c"));
		expected.add(new Tuple2<Integer, String>(12, "a"));
		expected.add(new Tuple2<Integer, String>(12, "b"));
		expected.add(new Tuple2<Integer, String>(12, "c"));
		expected.add(new Tuple2<Integer, String>(5, "a"));
		expected.add(new Tuple2<Integer, String>(5, "b"));
		expected.add(new Tuple2<Integer, String>(5, "c"));
		expected.add(new Tuple2<Integer, String>(10, "a"));
		expected.add(new Tuple2<Integer, String>(10, "b"));
		expected.add(new Tuple2<Integer, String>(10, "c"));
		expected.add(new Tuple2<Integer, String>(32, "a"));
		expected.add(new Tuple2<Integer, String>(32, "b"));
		expected.add(new Tuple2<Integer, String>(32, "c"));

		TimeStamp<Tuple2<Integer, String>> myTimeStamp = new TimeStamp<Tuple2<Integer, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public long getTimestamp(Tuple2<Integer, String> value) {
				return value.f0;
			}

			@Override
			public long getStartTime() {
				return 1;
			}
		};

		ReduceFunction<Tuple2<Integer, String>> myReduceFunction = new ReduceFunction<Tuple2<Integer, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1,
					Tuple2<Integer, String> value2) throws Exception {
				return new Tuple2<Integer, String>(value1.f0 + value2.f0, value1.f1);
			}
		};

		LinkedList<TriggerPolicy<Tuple2<Integer, String>>> triggers = new LinkedList<TriggerPolicy<Tuple2<Integer, String>>>();
		// Trigger every 2 time units but delay the first trigger by 2 (First
		// trigger after 4, then every 2)
		triggers.add(new TimeTriggerPolicy<Tuple2<Integer, String>>(2L, myTimeStamp, 2L,
				new Extractor<Long, Tuple2<Integer, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> extract(Long in) {
						return new Tuple2<Integer, String>(in.intValue(), null);
					}
				}));

		LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>> evictions = new LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>>();
		// Always delete all elements older then 4
		evictions.add(new TimeEvictionPolicy<Tuple2<Integer, String>>(4L, myTimeStamp));

		LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>> distributedTriggers = new LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>>();

		GroupedWindowingInvokable<Tuple2<Integer, String>> invokable = new GroupedWindowingInvokable<Tuple2<Integer, String>>(
				myReduceFunction, new TupleKeySelector<Tuple2<Integer, String>>(1),
				distributedTriggers, evictions, triggers);

		ArrayList<Tuple2<Integer, String>> result = new ArrayList<Tuple2<Integer, String>>();
		for (Tuple2<Tuple2<Integer, String>, String[]> t : MockInvokable.createAndExecute(
				invokable, inputs)) {
			result.add(t.f0);
		}

		assertEquals(new HashSet<Tuple2<Integer, String>>(expected),
				new HashSet<Tuple2<Integer, String>>(result));
		assertEquals(expected.size(), result.size());
	}

	/**
	 * Test for multiple centralized trigger
	 */
	@Test
	public void testGroupedWindowingInvokableMultipleCentralTrigger() {
		LinkedList<TriggerPolicy<Integer>> triggers = new LinkedList<TriggerPolicy<Integer>>();
		triggers.add(new CountTriggerPolicy<Integer>(8));
		triggers.add(new CountTriggerPolicy<Integer>(5));

		LinkedList<CloneableEvictionPolicy<Integer>> evictions = new LinkedList<CloneableEvictionPolicy<Integer>>();
		// The active wrapper causes eviction even on (fake) elements which
		// triggered, but does not belong to the group.
		evictions.add(new ActiveCloneableEvictionPolicyWrapper<Integer>(
				new TumblingEvictionPolicy<Integer>()));

		LinkedList<CloneableTriggerPolicy<Integer>> distributedTriggers = new LinkedList<CloneableTriggerPolicy<Integer>>();

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		inputs.add(2);
		inputs.add(1);
		// 1st Trigger: 2;6
		inputs.add(2);
		inputs.add(1);
		inputs.add(2);
		// 2nd Trigger: 1;4
		inputs.add(2);
		inputs.add(1);
		// Final: 1,2

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(2);
		expected.add(6);
		expected.add(4);
		expected.add(1);
		expected.add(2);
		expected.add(1);

		ReduceFunction<Integer> myReduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		};

		GroupedWindowingInvokable<Integer> invokable = new GroupedWindowingInvokable<Integer>(
				myReduceFunction, new KeySelector<Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Integer value) {
						return value;
					}
				}, distributedTriggers, evictions, triggers);

		ArrayList<Integer> result = new ArrayList<Integer>();
		for (Tuple2<Integer, String[]> t : MockInvokable.createAndExecute(invokable, inputs)) {
			result.add(t.f0);
		}

		assertEquals(new HashSet<Integer>(expected), new HashSet<Integer>(result));
		assertEquals(expected.size(), result.size());
	}

	/**
	 * Test for combination of centralized trigger and
	 * distributed trigger at the same time
	 */
	@Test
	public void testGroupedWindowingInvokableCentralAndDistrTrigger() {
		LinkedList<TriggerPolicy<Integer>> triggers = new LinkedList<TriggerPolicy<Integer>>();
		triggers.add(new CountTriggerPolicy<Integer>(8));
		triggers.add(new CountTriggerPolicy<Integer>(5));

		LinkedList<CloneableEvictionPolicy<Integer>> evictions = new LinkedList<CloneableEvictionPolicy<Integer>>();
		// The active wrapper causes eviction even on (fake) elements which
		// triggered, but does not belong to the group.
		evictions.add(new ActiveCloneableEvictionPolicyWrapper<Integer>(
				new TumblingEvictionPolicy<Integer>()));

		LinkedList<CloneableTriggerPolicy<Integer>> distributedTriggers = new LinkedList<CloneableTriggerPolicy<Integer>>();
		distributedTriggers.add(new CountTriggerPolicy<Integer>(2));

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		// local on 2 => 4
		inputs.add(2);
		inputs.add(1);
		// local on 1 => 2
		// and 1st Central: 2;2
		// SUMS up to 2;2
		inputs.add(2);
		// local on 2 => 2
		inputs.add(1);
		inputs.add(2);
		// 2nd Central: 1;2
		inputs.add(2);
		inputs.add(1);
		// Final: 1,2

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(4);
		expected.add(2);
		expected.add(2);
		expected.add(2);
		expected.add(1);
		expected.add(2);
		expected.add(1);
		expected.add(2);

		ReduceFunction<Integer> myReduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		};

		GroupedWindowingInvokable<Integer> invokable = new GroupedWindowingInvokable<Integer>(
				myReduceFunction, new KeySelector<Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Integer value) {
						return value;
					}
				}, distributedTriggers, evictions, triggers);

		ArrayList<Integer> result = new ArrayList<Integer>();
		for (Tuple2<Integer, String[]> t : MockInvokable.createAndExecute(invokable, inputs)) {
			result.add(t.f0);
		}

		assertEquals(new HashSet<Integer>(expected), new HashSet<Integer>(result));
		assertEquals(expected.size(), result.size());
	}

}
