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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeoutTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.time.Duration;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ProcessingTimeoutTrigger}.
 */
public class ProcessingTimeoutTriggerTest {

	@Test
	public void testWindowFireWithoutResetTimer() throws Exception {
		TriggerTestHarness<Object, TimeWindow> testHarness =
				new TriggerTestHarness<>(
						ProcessingTimeoutTrigger.of(
								CountTrigger.of(3),
								Duration.ofMillis(50), false, true), new TimeWindow.Serializer());

		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));

		// Should be two states, one for ProcessingTimeoutTrigger and one for CountTrigger.
		assertEquals(2, testHarness.numStateEntries());
		assertEquals(1, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Should not fire before interval time.
		assertThrows(
				"Must have exactly one timer firing. Fired timers: []",
				IllegalStateException.class,
				() -> testHarness.advanceProcessingTime(Long.MIN_VALUE + 40, new TimeWindow(0, 2))
		);

		assertEquals(TriggerResult.FIRE,
				testHarness.advanceProcessingTime(Long.MIN_VALUE + 50, new TimeWindow(0, 2)));
		// After firing states should be clear.
		assertEquals(0, testHarness.numStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Check inner trigger is working as well
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.FIRE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
	}

	@Test
	public void testWindowFireWithResetTimer() throws Exception {
		TriggerTestHarness<Object, TimeWindow> testHarness =
				new TriggerTestHarness<>(
						ProcessingTimeoutTrigger.of(
								CountTrigger.of(3),
								Duration.ofMillis(50), true, true),
						new TimeWindow.Serializer());

		assertThrows(
				"Must have exactly one timer firing. Fired timers: []",
				IllegalStateException.class,
				() -> testHarness.advanceProcessingTime(0, new TimeWindow(0, 2))
		);
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
		assertThrows(
				"Must have exactly one timer firing. Fired timers: []",
				IllegalStateException.class,
				() -> testHarness.advanceProcessingTime(10, new TimeWindow(0, 2))
		);
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));

		// Should be two states, one for ProcessingTimeoutTrigger and one for CountTrigger.
		assertEquals(2, testHarness.numStateEntries());
		assertEquals(1, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Should not fire at timestampA+interval (at 50 millis), because resetTimer is on, it should fire at 60 millis.
		assertThrows(
				"Must have exactly one timer firing. Fired timers: []",
				IllegalStateException.class,
				() -> testHarness.advanceProcessingTime(50, new TimeWindow(0, 2))
		);

		assertEquals(TriggerResult.FIRE,
				testHarness.advanceProcessingTime(60, new TimeWindow(0, 2)));
		// After firing states should be clear.
		assertEquals(0, testHarness.numStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		// Check inner trigger is working as well
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1, 0), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1, 10), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.FIRE,
				testHarness.processElement(new StreamRecord<>(1, 20), new TimeWindow(0, 2)));
	}

	@Test
	public void testWindowFireWithoutClearOnTimeout() throws Exception {
		TriggerTestHarness<Object, TimeWindow> testHarness =
				new TriggerTestHarness<>(
						ProcessingTimeoutTrigger.of(
								CountTrigger.of(3), Duration.ofMillis(50), false, false),
						new TimeWindow.Serializer());

		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));

		// Should be two states, one for ProcessingTimeoutTrigger and one for CountTrigger.
		assertEquals(2, testHarness.numStateEntries());
		assertEquals(1, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		assertEquals(TriggerResult.FIRE,
				testHarness.advanceProcessingTime(Long.MIN_VALUE + 50, new TimeWindow(0, 2)));

		// After firing, the state of the inner trigger (e.g CountTrigger) should not be clear, same as the state of the timestamp.
		assertEquals(2, testHarness.numStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());
	}

	@Test
	public void testWindowPurgingWhenInnerTriggerIsPurging() throws Exception {
		TriggerTestHarness<Object, TimeWindow> testHarness =
				new TriggerTestHarness<>(ProcessingTimeoutTrigger.of(
						PurgingTrigger.of(ProcessingTimeTrigger.create()), Duration.ofMillis(50),
						false, false),
						new TimeWindow.Serializer());

		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
		assertEquals(TriggerResult.CONTINUE,
				testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));

		// Should be only one state for ProcessingTimeoutTrigger.
		assertEquals(1, testHarness.numStateEntries());
		// Should be two timers, one for ProcessingTimeoutTrigger timeout timer, and one for ProcessingTimeTrigger maxWindow timer.
		assertEquals(2, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		assertEquals(TriggerResult.FIRE_AND_PURGE,
				testHarness.advanceProcessingTime(Long.MIN_VALUE + 50, new TimeWindow(0, 2)));

		// Because shouldClearAtTimeout is false, the state of ProcessingTimeoutTrigger not cleared, same as ProcessingTimeTrigger timer for maxWindowTime.
		assertEquals(1, testHarness.numStateEntries());
		assertEquals(1, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

	}

}
