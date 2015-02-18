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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.windowing.helper.Count;
import org.junit.Test;

public class CountTriggerPolicyTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCountTriggerPolicy() {

		List tuples = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		int counter;

		// Test count of different sizes (0..9)
		for (int i = 0; i < 10; i++) {
			TriggerPolicy triggerPolicy = Count.of(i).toTrigger();
			counter = 0;

			// Test first i steps (should not trigger)
			for (int j = 0; j < i; j++) {
				counter++;
				assertFalse("Triggerpolicy with count of " + i + " triggered at add nr. " + counter
						+ ". It should not trigger for the first " + i + " adds.",
						triggerPolicy.notifyTrigger(tuples.get(j)));
			}

			// Test the next three triggers
			for (int j = 0; j < 3; j++) {
				// The first add should trigger now
				counter++;
				assertTrue("Triggerpolicy with count of " + i
						+ " did not trigger at the expected pos " + counter + ".",
						triggerPolicy.notifyTrigger(tuples.get(j)));

				// the next i-1 adds should not trigger
				for (int k = 0; k < i - 1; k++) {
					counter++;
					assertFalse("Triggerpolicy with count of " + i + " triggered at add nr. "
							+ counter, triggerPolicy.notifyTrigger(tuples.get(k)));
				}
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCountTriggerPolicyStartValues() {

		List tuples = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

		// Test count of different sizes (0..9)
		for (int i = 0; i < 10; i++) {

			// Test different start values (-5..5)
			for (int j = -5; i < 6; i++) {
				TriggerPolicy triggerPolicy = new CountTriggerPolicy(i, j);
				// Add tuples without trigger
				for (int k = 0; k < ((i - j > 0) ? i - j : 0); k++) {
					assertFalse("Triggerpolicy with count of " + i + " and start value of " + j
							+ " triggered at add nr. " + (k + 1),
							triggerPolicy.notifyTrigger(tuples.get(k % 10)));
				}
				// Expect trigger
				assertTrue("Triggerpolicy with count of " + i + "and start value of " + j
						+ " did not trigger at the expected position.",
						triggerPolicy.notifyTrigger(tuples.get(0)));
			}
		}
	}

	@Test
	public void equalityTest() {
		assertEquals(new CountTriggerPolicy<Integer>(5, 5), new CountTriggerPolicy<Integer>(5, 5));

		assertEquals(new CountTriggerPolicy<Integer>(5, 5), new CountTriggerPolicy<Integer>(5, 5));
		assertEquals(new CountTriggerPolicy<Integer>(5), new CountTriggerPolicy<Integer>(5));

		assertNotEquals(new CountTriggerPolicy<Integer>(4, 5),
				new CountTriggerPolicy<Integer>(5, 5));
		assertNotEquals(new CountTriggerPolicy<Integer>(5, 5),
				new CountTriggerPolicy<Integer>(5, 4));
	}
}
