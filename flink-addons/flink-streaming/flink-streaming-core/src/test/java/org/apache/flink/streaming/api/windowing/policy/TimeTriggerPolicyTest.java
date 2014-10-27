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

import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimeTriggerPolicyTest {

	@Test
	public void timeTriggerRegularNotifyTest() {
		// create some test data
		Integer[] times = { 1, 3, 4, 6, 7, 9, 14, 20, 21, 22, 30 };

		// create a timestamp
		@SuppressWarnings("serial")
		TimeStamp<Integer> timeStamp = new TimeStamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

			@Override
			public long getStartTime() {
				return 0;
			}

		};

		// test different granularity
		for (long granularity = 0; granularity < 31; granularity++) {
			// create policy
			TriggerPolicy<Integer> policy = new TimeTriggerPolicy<Integer>(granularity, timeStamp,
					new Extractor<Long, Integer>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Integer extract(Long in) {
							return in.intValue();
						}
					});

			// remember window border
			// Remark: This might NOT work in case the timeStamp uses
			// System.getCurrentTimeMillis to determine the start time.
			long currentTime = timeStamp.getStartTime();

			// test by adding values
			for (int i = 0; i < times.length; i++) {
				boolean result = policy.notifyTrigger(times[i]);
				// start time is included, but end time is excluded: >=
				if (times[i] >= currentTime + granularity) {
					if (granularity != 0) {
						currentTime = times[i] - ((times[i] - currentTime) % granularity);
					}
					assertTrue("The policy did not trigger at pos " + i + " (current time border: "
							+ currentTime + "; current granularity: " + granularity
							+ "; data point time: " + times[i] + ")", result);
				} else {
					assertFalse("The policy triggered wrong at pos " + i
							+ " (current time border: " + currentTime + "; current granularity: "
							+ granularity + "; data point time: " + times[i] + ")", result);
				}
			}
		}

	}

	@Test
	public void timeTriggerPreNotifyTest() {
		// create some test data
		Integer[] times = { 1, 3, 20, 26};

		// create a timestamp
		@SuppressWarnings("serial")
		TimeStamp<Integer> timeStamp = new TimeStamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

			@Override
			public long getStartTime() {
				return 0;
			}

		};

		// create policy
		TimeTriggerPolicy<Integer> policy = new TimeTriggerPolicy<Integer>(5, timeStamp,
				new Extractor<Long, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer extract(Long in) {
						return in.intValue();
					}
				});

		//expected result
		Integer[][] result={{},{},{5,10,15},{25}};
		
		//call policy
		for (int i=0;i<times.length;i++){
			arrayEqualityCheck(result[i],policy.preNotifyTrigger(times[i]));
			policy.notifyTrigger(times[i]);
		}
	}
	
	private void arrayEqualityCheck(Object[] array1, Object[] array2) {
		assertEquals("The result arrays must have the same length", array1.length, array2.length);
		for (int i = 0; i < array1.length; i++) {
			assertEquals("Unequal fields at position " + i, array1[i], array2[i]);
		}
	}

}
