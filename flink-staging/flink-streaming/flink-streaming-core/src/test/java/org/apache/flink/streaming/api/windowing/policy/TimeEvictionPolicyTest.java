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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;

import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.junit.Test;

public class TimeEvictionPolicyTest {

	@Test
	public void timeEvictionTest() {
		// create some test data
		Integer[] times = { 1, 3, 4, 6, 7, 9, 14, 20, 21, 22, 30, 31, 33, 36, 40, 41, 42, 43, 44,
				45, 47, 55 };
		Integer[] numToDelete = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 0, 0, 0, 3 };

		// create a timestamp
		@SuppressWarnings("serial")
		Timestamp<Integer> timeStamp = new Timestamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

		};

		// test different granularity
		for (long granularity = 0; granularity < 40; granularity++) {
			// create policy
			TimeEvictionPolicy<Integer> policy = new TimeEvictionPolicy<Integer>(granularity,
					new TimestampWrapper<Integer>(timeStamp, 0));

			// The trigger status should not effect the policy. Therefore, it's
			// value is changed after each usage.
			boolean triggered = false;

			// The eviction should work similar with both, fake and real
			// elements. Which kind is used is changed on every 3rd element in
			// this test.
			int fakeAndRealCounter = 0;
			boolean fake = false;

			// test by adding values
			LinkedList<Integer> buffer = new LinkedList<Integer>();
			for (int i = 0; i < times.length; i++) {

				// check if the current element should be a fake
				fakeAndRealCounter++;
				if (fakeAndRealCounter > 2) {
					fake = !fake;
					fakeAndRealCounter = 0;
				}

				int result;

				if (fake) {
					// Notify eviction with fake element
					result = policy.notifyEvictionWithFakeElement(times[i], buffer.size());
				} else {
					// Notify eviction with real element
					result = policy.notifyEviction(times[i], (triggered = !triggered),
							buffer.size());
				}

				// handle correctness of eviction
				for (; result > 0 && !buffer.isEmpty(); result--) {
					if (buffer.getFirst() <= times[i] - granularity) {
						buffer.removeFirst();
					} else {
						fail("The policy wanted to evict time " + buffer.getFirst()
								+ " while the current time was " + times[i]
								+ "and the granularity was " + granularity);
					}
				}

				// test that all required evictions have been done
				if (!buffer.isEmpty()) {
					assertTrue("The policy did not evict " + buffer.getFirst()
							+ " while the current time was " + times[i]
							+ " and the granularity was " + granularity,
							(buffer.getFirst() >= times[i] - granularity));
				}

				// test influence of other evictions
				for (int j = numToDelete[i % numToDelete.length]; j > 0; j--) {
					if (!buffer.isEmpty()) {
						buffer.removeFirst();
					}
				}

				// add current element to buffer if it is no fake
				if (!fake) {
					buffer.add(times[i]);
				}

			}
		}
	}

	@Test
	public void equalsTest() {

		@SuppressWarnings("serial")
		Timestamp<Integer> timeStamp = new Timestamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

		};

		@SuppressWarnings("serial")
		Timestamp<Integer> timeStamp2 = new Timestamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

		};

		assertEquals(
				new TimeEvictionPolicy<Integer>(5, new TimestampWrapper<Integer>(timeStamp, 0)),
				new TimeEvictionPolicy<Integer>(5, new TimestampWrapper<Integer>(timeStamp, 0)));

		assertNotEquals(new TimeEvictionPolicy<Integer>(5, new TimestampWrapper<Integer>(timeStamp,
				0)), new TimeEvictionPolicy<Integer>(5,
				new TimestampWrapper<Integer>(timeStamp2, 0)));

		assertNotEquals(new TimeEvictionPolicy<Integer>(5, new TimestampWrapper<Integer>(timeStamp,
				0)),
				new TimeEvictionPolicy<Integer>(2, new TimestampWrapper<Integer>(timeStamp, 0)));

		assertNotEquals(new TimeEvictionPolicy<Integer>(5, new TimestampWrapper<Integer>(timeStamp,
				0)),
				new TimeEvictionPolicy<Integer>(5, new TimestampWrapper<Integer>(timeStamp, 3)));
	}

}
