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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.ActiveCloneableEvictionPolicyWrapper;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class GroupedWindowInvokableTest {

	KeySelector<Tuple2<Integer, String>, ?> keySelector = new KeySelector<Tuple2<Integer, String>, String>() {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<Integer, String> value) throws Exception {
			return value.f1;
		}
	};

	/**
	 * Tests that illegal arguments result in failure. The following cases are
	 * tested: 1) having no trigger 2) having no eviction 3) having neither
	 * eviction nor trigger 4) having both, central and distributed eviction.
	 */
	@Test
	public void testGroupedWindowInvokableFailTest() {

		// create dummy reduce function
		ReduceFunction<Object> userFunction = new ReduceFunction<Object>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Object reduce(Object value1, Object value2) throws Exception {
				return null;
			}
		};

		// create dummy keySelector
		KeySelector<Object, Object> keySelector = new KeySelector<Object, Object>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Object getKey(Object value) throws Exception {
				return null;
			}
		};

		// create policy lists
		LinkedList<CloneableEvictionPolicy<Object>> distributedEvictionPolicies = new LinkedList<CloneableEvictionPolicy<Object>>();
		LinkedList<CloneableTriggerPolicy<Object>> distributedTriggerPolicies = new LinkedList<CloneableTriggerPolicy<Object>>();
		LinkedList<EvictionPolicy<Object>> centralEvictionPolicies = new LinkedList<EvictionPolicy<Object>>();
		LinkedList<TriggerPolicy<Object>> centralTriggerPolicies = new LinkedList<TriggerPolicy<Object>>();

		// empty trigger and policy lists should fail
		try {
			new GroupedWindowInvokable<Object, Object>(userFunction, keySelector,
					distributedTriggerPolicies, distributedEvictionPolicies,
					centralTriggerPolicies, centralEvictionPolicies);
			fail("Creating instance without any trigger or eviction policy should cause an UnsupportedOperationException but didn't. (1)");
		} catch (UnsupportedOperationException e) {
			// that's the expected case
		}

		// null for trigger and policy lists should fail
		try {
			new GroupedWindowInvokable<Object, Object>(userFunction, keySelector, null, null, null,
					null);
			fail("Creating instance without any trigger or eviction policy should cause an UnsupportedOperationException but didn't. (2)");
		} catch (UnsupportedOperationException e) {
			// that's the expected case
		}

		// empty eviction should still fail
		centralTriggerPolicies.add(new CountTriggerPolicy<Object>(5));
		distributedTriggerPolicies.add(new CountTriggerPolicy<Object>(5));
		try {
			new GroupedWindowInvokable<Object, Object>(userFunction, keySelector,
					distributedTriggerPolicies, distributedEvictionPolicies,
					centralTriggerPolicies, centralEvictionPolicies);
			fail("Creating instance without any eviction policy should cause an UnsupportedOperationException but didn't. (3)");
		} catch (UnsupportedOperationException e) {
			// that's the expected case
		}

		// empty trigger should still fail
		centralTriggerPolicies.clear();
		distributedTriggerPolicies.clear();
		centralEvictionPolicies.add(new CountEvictionPolicy<Object>(5));
		try {
			new GroupedWindowInvokable<Object, Object>(userFunction, keySelector,
					distributedTriggerPolicies, distributedEvictionPolicies,
					centralTriggerPolicies, centralEvictionPolicies);
			fail("Creating instance without any trigger policy should cause an UnsupportedOperationException but didn't. (4)");
		} catch (UnsupportedOperationException e) {
			// that's the expected case
		}

		// having both, central and distributed eviction, at the same time
		// should fail
		centralTriggerPolicies.add(new CountTriggerPolicy<Object>(5));
		distributedEvictionPolicies.add(new CountEvictionPolicy<Object>(5));
		try {
			new GroupedWindowInvokable<Object, Object>(userFunction, keySelector,
					distributedTriggerPolicies, distributedEvictionPolicies,
					centralTriggerPolicies, centralEvictionPolicies);
			fail("Creating instance with central and distributed eviction should cause an UnsupportedOperationException but didn't. (4)");
		} catch (UnsupportedOperationException e) {
			// that's the expected case
		}

	}

	/**
	 * Test for not active distributed triggers with single field
	 */
	@Test
	public void testGroupedWindowInvokableDistributedTriggerSimple() {
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

		List<Integer> expectedDistributedEviction = new ArrayList<Integer>();
		expectedDistributedEviction.add(15);
		expectedDistributedEviction.add(3);
		expectedDistributedEviction.add(3);
		expectedDistributedEviction.add(15);

		List<Integer> expectedCentralEviction = new ArrayList<Integer>();
		expectedCentralEviction.add(2);
		expectedCentralEviction.add(5);
		expectedCentralEviction.add(15);
		expectedCentralEviction.add(2);
		expectedCentralEviction.add(5);
		expectedCentralEviction.add(2);
		expectedCentralEviction.add(5);
		expectedCentralEviction.add(1);
		expectedCentralEviction.add(5);

		LinkedList<CloneableTriggerPolicy<Integer>> triggers = new LinkedList<CloneableTriggerPolicy<Integer>>();
		// Trigger on every 2nd element, but the first time after the 3rd
		triggers.add(new CountTriggerPolicy<Integer>(2, -1));

		LinkedList<CloneableEvictionPolicy<Integer>> evictions = new LinkedList<CloneableEvictionPolicy<Integer>>();
		// On every 2nd element, remove the oldest 2 elements, but the first
		// time after the 3rd element
		evictions.add(new CountEvictionPolicy<Integer>(2, 2, -1));

		LinkedList<TriggerPolicy<Integer>> centralTriggers = new LinkedList<TriggerPolicy<Integer>>();

		ReduceFunction<Integer> reduceFunction = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		};

		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) {
				return value;
			}
		};

		GroupedWindowInvokable<Integer, Integer> invokable = new GroupedWindowInvokable<Integer, Integer>(
				reduceFunction, keySelector, triggers, evictions, centralTriggers, null);

		List<Integer> result = MockContext.createAndExecute(invokable, inputs);

		List<Integer> actual = new LinkedList<Integer>();
		for (Integer current : result) {
			actual.add(current);
		}

		assertEquals(new HashSet<Integer>(expectedDistributedEviction),
				new HashSet<Integer>(actual));
		assertEquals(expectedDistributedEviction.size(), actual.size());

		// Run test with central eviction
		triggers.clear();
		centralTriggers.add(new CountTriggerPolicy<Integer>(2, -1));
		LinkedList<EvictionPolicy<Integer>> centralEvictions = new LinkedList<EvictionPolicy<Integer>>();
		centralEvictions.add(new CountEvictionPolicy<Integer>(2, 2, -1));

		invokable = new GroupedWindowInvokable<Integer, Integer>(reduceFunction, keySelector,
				triggers, null, centralTriggers, centralEvictions);

		result = MockContext.createAndExecute(invokable, inputs);
		actual = new LinkedList<Integer>();
		for (Integer current : result) {
			actual.add(current);
		}

		assertEquals(new HashSet<Integer>(expectedCentralEviction), new HashSet<Integer>(actual));
		assertEquals(expectedCentralEviction.size(), actual.size());
	}

	/**
	 * Test for non active distributed triggers with separated key field
	 */
	@Test
	public void testGroupedWindowInvokableDistributedTriggerComplex() {
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

		GroupedWindowInvokable<Tuple2<Integer, String>, Tuple2<Integer, String>> invokable2 = new GroupedWindowInvokable<Tuple2<Integer, String>, Tuple2<Integer, String>>(
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
				}, keySelector, triggers, evictions, centralTriggers, null);

		List<Tuple2<Integer, String>> result = MockContext.createAndExecute(invokable2, inputs2);

		List<Tuple2<Integer, String>> actual2 = new LinkedList<Tuple2<Integer, String>>();
		for (Tuple2<Integer, String> current : result) {
			actual2.add(current);
		}

		assertEquals(new HashSet<Tuple2<Integer, String>>(expected2),
				new HashSet<Tuple2<Integer, String>>(actual2));
		assertEquals(expected2.size(), actual2.size());
	}

	/**
	 * Test for active centralized trigger
	 */
	@Test
	public void testGroupedWindowInvokableCentralActiveTrigger() {

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

		Timestamp<Tuple2<Integer, String>> myTimeStamp = new Timestamp<Tuple2<Integer, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public long getTimestamp(Tuple2<Integer, String> value) {
				return value.f0;
			}
		};

		TimestampWrapper<Tuple2<Integer, String>> myTimeStampWrapper = new TimestampWrapper<Tuple2<Integer, String>>(
				myTimeStamp, 1);

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
		triggers.add(new TimeTriggerPolicy<Tuple2<Integer, String>>(2L, myTimeStampWrapper, 2L));

		LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>> evictions = new LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>>();
		// Always delete all elements older then 4
		evictions.add(new TimeEvictionPolicy<Tuple2<Integer, String>>(4L, myTimeStampWrapper));

		LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>> distributedTriggers = new LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>>();

		GroupedWindowInvokable<Tuple2<Integer, String>, Tuple2<Integer, String>> invokable = new GroupedWindowInvokable<Tuple2<Integer, String>, Tuple2<Integer, String>>(
				myReduceFunction, keySelector, distributedTriggers, evictions, triggers, null);

		ArrayList<Tuple2<Integer, String>> result = new ArrayList<Tuple2<Integer, String>>();
		for (Tuple2<Integer, String> t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}

		assertEquals(new HashSet<Tuple2<Integer, String>>(expected),
				new HashSet<Tuple2<Integer, String>>(result));
		assertEquals(expected.size(), result.size());

		// repeat the test with central eviction. The result should be the same.
		triggers.clear();
		triggers.add(new TimeTriggerPolicy<Tuple2<Integer, String>>(2L, myTimeStampWrapper, 2L));
		evictions.clear();
		LinkedList<EvictionPolicy<Tuple2<Integer, String>>> centralEvictions = new LinkedList<EvictionPolicy<Tuple2<Integer, String>>>();
		centralEvictions.add(new TimeEvictionPolicy<Tuple2<Integer, String>>(4L, myTimeStampWrapper));

		invokable = new GroupedWindowInvokable<Tuple2<Integer, String>, Tuple2<Integer, String>>(
				myReduceFunction, keySelector, distributedTriggers, evictions, triggers,
				centralEvictions);

		result = new ArrayList<Tuple2<Integer, String>>();
		for (Tuple2<Integer, String> t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}

		assertEquals(new HashSet<Tuple2<Integer, String>>(expected),
				new HashSet<Tuple2<Integer, String>>(result));
		assertEquals(expected.size(), result.size());
	}

	/**
	 * Test for multiple centralized trigger
	 */
	@Test
	public void testGroupedWindowInvokableMultipleCentralTrigger() {
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

		GroupedWindowInvokable<Integer, Integer> invokable = new GroupedWindowInvokable<Integer, Integer>(
				myReduceFunction, new KeySelector<Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Integer value) {
						return value;
					}
				}, distributedTriggers, evictions, triggers, null);

		ArrayList<Integer> result = new ArrayList<Integer>();
		for (Integer t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}

		assertEquals(new HashSet<Integer>(expected), new HashSet<Integer>(result));
		assertEquals(expected.size(), result.size());
	}

	/**
	 * Test for combination of centralized trigger and distributed trigger at
	 * the same time
	 */
	@Test
	public void testGroupedWindowInvokableCentralAndDistrTrigger() {
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

		GroupedWindowInvokable<Integer, Integer> invokable = new GroupedWindowInvokable<Integer, Integer>(
				myReduceFunction, new KeySelector<Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Integer value) {
						return value;
					}
				}, distributedTriggers, evictions, triggers, null);

		ArrayList<Integer> result = new ArrayList<Integer>();
		for (Integer t : MockContext.createAndExecute(invokable, inputs)) {
			result.add(t);
		}

		assertEquals(new HashSet<Integer>(expected), new HashSet<Integer>(result));
		assertEquals(expected.size(), result.size());
	}

}
