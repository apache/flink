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

package org.apache.flink.table.runtime.operators.window.triggers;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for triggers.
 */
public class TriggersTest {

	@Test
	public void testEventTime() {
		Trigger<?> trigger;
		String expected;

		trigger = EventTimeTriggers
				.afterEndOfWindow()
				.withEarlyFirings(ElementTriggers.every())
				.withLateFirings(ProcessingTimeTriggers.every(Duration.ofSeconds(1)));
		expected = "EventTime.afterEndOfWindow()" +
				".withEarlyFirings(Element.every())" +
				".withLateFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTimeTriggers.AfterEndOfWindowEarlyAndLate);

		trigger = EventTimeTriggers
				.afterEndOfWindow()
				.withEarlyFirings(ProcessingTimeTriggers.every(Duration.ofSeconds(1)))
				.withLateFirings(ElementTriggers.every());
		expected = "EventTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTimeTriggers.AfterEndOfWindowNoLate);

		// only periodic early trigger
		trigger = EventTimeTriggers
				.afterEndOfWindow()
				.withEarlyFirings(ProcessingTimeTriggers.every(Duration.ofSeconds(1)));
		expected = "EventTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof EventTimeTriggers.AfterEndOfWindowNoLate);

		// only Element.every() early trigger
		trigger = EventTimeTriggers
				.afterEndOfWindow()
				.withEarlyFirings(ElementTriggers.every());
		expected = "EventTime.afterEndOfWindow().withEarlyFirings(Element.every())";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof EventTimeTriggers.AfterEndOfWindowNoLate);

		// only periodic late trigger
		trigger = EventTimeTriggers
				.afterEndOfWindow()
				.withLateFirings(ProcessingTimeTriggers.every(Duration.ofMillis(1)));
		expected = "EventTime.afterEndOfWindow().withLateFirings(ProcessingTime.every(1))";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTimeTriggers.AfterEndOfWindowEarlyAndLate);

		// only Element.every() late trigger
		trigger = EventTimeTriggers
				.afterEndOfWindow()
				.withLateFirings(ElementTriggers.every());
		expected = "EventTime.afterEndOfWindow()";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTimeTriggers.AfterEndOfWindow);
	}

	@Test
	public void testProcessingTime() {
		Trigger<?> trigger;
		String expected;

		trigger = ProcessingTimeTriggers
				.afterEndOfWindow();
		expected = "ProcessingTime.afterEndOfWindow()";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof ProcessingTimeTriggers.AfterEndOfWindow);

		trigger = ProcessingTimeTriggers
				.afterEndOfWindow()
				.withEarlyFirings(ElementTriggers.every());
		expected = "ProcessingTime.afterEndOfWindow().withEarlyFirings(Element.every())";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof ProcessingTimeTriggers.AfterEndOfWindowNoLate);

		trigger = ProcessingTimeTriggers
				.afterEndOfWindow()
				.withEarlyFirings(ProcessingTimeTriggers.every(Duration.ofSeconds(1)));
		expected = "ProcessingTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof ProcessingTimeTriggers.AfterEndOfWindowNoLate);
	}
}
