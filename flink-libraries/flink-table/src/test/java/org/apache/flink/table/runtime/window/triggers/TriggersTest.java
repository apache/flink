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

package org.apache.flink.table.runtime.window.triggers;

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

		trigger = EventTime
			.afterEndOfWindow()
			.withEarlyFirings(Element.every())
			.withLateFirings(ProcessingTime.every(Duration.ofSeconds(1)));
		expected = "EventTime.afterEndOfWindow()" +
			".withEarlyFirings(Element.every())" +
			".withLateFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTime.AfterEndOfWindowEarlyAndLate);

		trigger = EventTime
			.afterEndOfWindow()
			.withEarlyFirings(ProcessingTime.every(Duration.ofSeconds(1)))
			.withLateFirings(Element.every());
		expected = "EventTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTime.AfterEndOfWindowNoLate);

		// only periodic early trigger
		trigger = EventTime
			.afterEndOfWindow()
			.withEarlyFirings(ProcessingTime.every(Duration.ofSeconds(1)));
		expected = "EventTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof EventTime.AfterEndOfWindowNoLate);

		// only Element.every() early trigger
		trigger = EventTime
			.afterEndOfWindow()
			.withEarlyFirings(Element.every());
		expected = "EventTime.afterEndOfWindow().withEarlyFirings(Element.every())";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof EventTime.AfterEndOfWindowNoLate);

		// only periodic late trigger
		trigger = EventTime
			.afterEndOfWindow()
			.withLateFirings(ProcessingTime.every(Duration.ofMillis(1)));
		expected = "EventTime.afterEndOfWindow().withLateFirings(ProcessingTime.every(1))";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTime.AfterEndOfWindowEarlyAndLate);

		// only Element.every() late trigger
		trigger = EventTime
			.afterEndOfWindow()
			.withLateFirings(Element.every());
		expected = "EventTime.afterEndOfWindow()";
		assertEquals(expected, trigger.toString());
		assertTrue(trigger instanceof EventTime.AfterEndOfWindow);
	}

	@Test
	public void testProcessingTime() {
		Trigger<?> trigger;
		String expected;

		trigger = ProcessingTime
			.afterEndOfWindow();
		expected = "ProcessingTime.afterEndOfWindow()";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof ProcessingTime.AfterEndOfWindow);

		trigger = ProcessingTime
			.afterEndOfWindow()
			.withEarlyFirings(Element.every());
		expected = "ProcessingTime.afterEndOfWindow().withEarlyFirings(Element.every())";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof ProcessingTime.AfterEndOfWindowNoLate);

		trigger = ProcessingTime
			.afterEndOfWindow()
			.withEarlyFirings(ProcessingTime.every(Duration.ofSeconds(1)));
		expected = "ProcessingTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))";
		assertEquals(expected, trigger.toString());
		//noinspection ConstantConditions
		assertTrue(trigger instanceof ProcessingTime.AfterEndOfWindowNoLate);
	}
}
