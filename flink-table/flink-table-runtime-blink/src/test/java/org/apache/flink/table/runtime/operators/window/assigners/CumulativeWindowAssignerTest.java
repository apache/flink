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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;

import static org.apache.flink.table.runtime.operators.window.WindowTestUtils.timeWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CumulativeWindowAssigner}.
 */
public class CumulativeWindowAssignerTest {

	private static final RowData ELEMENT = GenericRowData.of(StringData.fromString("String"));

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@SuppressWarnings("unchecked")
	@Test
	public void testWindowAssignment() {
		CumulativeWindowAssigner assigner = CumulativeWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000));

		assertThat(
			assigner.assignWindows(ELEMENT, 0L),
			containsInAnyOrder(
				timeWindow(0, 1000),
				timeWindow(0, 2000),
				timeWindow(0, 3000),
				timeWindow(0, 4000),
				timeWindow(0, 5000)));
		assertThat(
			assigner.assignWindows(ELEMENT, 4999L),
			contains(
				timeWindow(0, 5000)));
		assertThat(
			assigner.assignWindows(ELEMENT, 5000L),
			containsInAnyOrder(
				timeWindow(5000, 6000),
				timeWindow(5000, 7000),
				timeWindow(5000, 8000),
				timeWindow(5000, 9000),
				timeWindow(5000, 10000)));

		// test pane
		assertEquals(assigner.assignPane(ELEMENT, 0L), new TimeWindow(0, 1000));
		assertEquals(assigner.assignPane(ELEMENT, 4999L), new TimeWindow(4000, 5000));
		assertEquals(assigner.assignPane(ELEMENT, 2000), new TimeWindow(2000, 3000));
		assertEquals(assigner.assignPane(ELEMENT, 5000L), new TimeWindow(5000, 6000));

		assertThat(
			assigner.splitIntoPanes(new TimeWindow(0, 5000)),
			contains(
				timeWindow(0, 1000),
				timeWindow(1000, 2000),
				timeWindow(2000, 3000),
				timeWindow(3000, 4000),
				timeWindow(4000, 5000)));

		assertThat(
			assigner.splitIntoPanes(new TimeWindow(5000, 8000)),
			contains(
				timeWindow(5000, 6000),
				timeWindow(6000, 7000),
				timeWindow(7000, 8000)));

		assertEquals(assigner.getLastWindow(new TimeWindow(4000, 5000)), new TimeWindow(0, 5000));
		assertEquals(assigner.getLastWindow(new TimeWindow(2000, 3000)), new TimeWindow(0, 5000));
		assertEquals(assigner.getLastWindow(new TimeWindow(7000, 8000)), new TimeWindow(5000, 10000));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testWindowAssignmentWithOffset() {
		CumulativeWindowAssigner assigner = CumulativeWindowAssigner
			.of(Duration.ofMillis(5000), Duration.ofMillis(1000))
			.withOffset(Duration.ofMillis(100));

		assertThat(
			assigner.assignWindows(ELEMENT, 100L),
				containsInAnyOrder(
				timeWindow(100, 1100),
				timeWindow(100, 2100),
				timeWindow(100, 3100),
				timeWindow(100, 4100),
				timeWindow(100, 5100)));
		assertThat(
			assigner.assignWindows(ELEMENT, 5099L),
			contains(
				timeWindow(100, 5100)));
		assertThat(
			assigner.assignWindows(ELEMENT, 5100L),
			containsInAnyOrder(
				timeWindow(5100, 6100),
				timeWindow(5100, 7100),
				timeWindow(5100, 8100),
				timeWindow(5100, 9100),
				timeWindow(5100, 10100)));

		// test pane
		assertEquals(assigner.assignPane(ELEMENT, 100L), new TimeWindow(100, 1100));
		assertEquals(assigner.assignPane(ELEMENT, 5099L), new TimeWindow(4100, 5100));
		assertEquals(assigner.assignPane(ELEMENT, 2100), new TimeWindow(2100, 3100));
		assertEquals(assigner.assignPane(ELEMENT, 5100L), new TimeWindow(5100, 6100));

		assertThat(
			assigner.splitIntoPanes(new TimeWindow(100, 5100)),
			contains(
				timeWindow(100, 1100),
				timeWindow(1100, 2100),
				timeWindow(2100, 3100),
				timeWindow(3100, 4100),
				timeWindow(4100, 5100)));

		assertThat(
			assigner.splitIntoPanes(new TimeWindow(5100, 8100)),
			contains(
				timeWindow(5100, 6100),
				timeWindow(6100, 7100),
				timeWindow(7100, 8100)));

		assertEquals(assigner.getLastWindow(new TimeWindow(4100, 5100)), new TimeWindow(100, 5100));
		assertEquals(assigner.getLastWindow(new TimeWindow(2100, 3100)), new TimeWindow(100, 5100));
		assertEquals(assigner.getLastWindow(new TimeWindow(7100, 8100)), new TimeWindow(5100, 10100));
	}

	@Test
	public void testInvalidParameters1() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("step > 0 and size > 0");
		CumulativeWindowAssigner.of(Duration.ofSeconds(-2), Duration.ofSeconds(1));
	}

	@Test
	public void testInvalidParameters2() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("step > 0 and size > 0");
		CumulativeWindowAssigner.of(Duration.ofSeconds(2), Duration.ofSeconds(-1));
	}

	@Test
	public void testInvalidParameters3() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("size must be an integral multiple of step.");
		CumulativeWindowAssigner.of(Duration.ofSeconds(5000), Duration.ofSeconds(2000));
	}

	@Test
	public void testProperties() {
		CumulativeWindowAssigner assigner = CumulativeWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000));

		assertTrue(assigner.isEventTime());
		assertEquals(new TimeWindow.Serializer(), assigner.getWindowSerializer(new ExecutionConfig()));

		assertTrue(assigner.withEventTime().isEventTime());
		assertFalse(assigner.withProcessingTime().isEventTime());
	}
}
