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

import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

/**
 * Tests for verifying that {@link MergingWindowSet} correctly merges windows in various situations
 * and that the merge callback is called with the correct sets of windows.
 */
public class MergingWindowSetTest {

	@Test
	public void testIncrementalMerging() throws Exception {
		MergingWindowSet<TimeWindow> windowSet = new MergingWindowSet<>(EventTimeSessionWindows.withGap(Time.milliseconds(3)));

		TestingMergeFunction mergeFunction = new TestingMergeFunction();

		// add initial window
		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 4), windowSet.addWindow(new TimeWindow(0, 4), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		assertTrue(windowSet.getStateWindow(new TimeWindow(0, 4)).equals(new TimeWindow(0, 4)));

		// add some more windows
		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 4), windowSet.addWindow(new TimeWindow(0, 4), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 5), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
		assertTrue(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(0, 5), mergeFunction.mergeTarget());
		assertEquals(new TimeWindow(0, 4), mergeFunction.stateWindow());
		assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(0, 4)));
		assertTrue(mergeFunction.mergedStateWindows().isEmpty());

		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(4, 6), mergeFunction));
		assertTrue(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(0, 6), mergeFunction.mergeTarget());
		assertEquals(new TimeWindow(0, 4), mergeFunction.stateWindow());
		assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(0, 5)));
		assertTrue(mergeFunction.mergedStateWindows().isEmpty());

		assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

		// add some windows that falls into the already merged region
		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(1, 4), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(0, 4), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(4, 6), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

		// add some more windows that don't merge with the first bunch
		mergeFunction.reset();
		assertEquals(new TimeWindow(11, 14), windowSet.addWindow(new TimeWindow(11, 14), mergeFunction));
		assertFalse(mergeFunction.hasMerged());


		assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

		assertEquals(new TimeWindow(11, 14), windowSet.getStateWindow(new TimeWindow(11, 14)));

		// add some more windows that merge with the second bunch

		mergeFunction.reset();
		assertEquals(new TimeWindow(10, 14), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
		assertTrue(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(10, 14), mergeFunction.mergeTarget());
		assertEquals(new TimeWindow(11, 14), mergeFunction.stateWindow());
		assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(11, 14)));
		assertTrue(mergeFunction.mergedStateWindows().isEmpty());

		mergeFunction.reset();
		assertEquals(new TimeWindow(10, 15), windowSet.addWindow(new TimeWindow(12, 15), mergeFunction));
		assertTrue(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(10, 15), mergeFunction.mergeTarget());
		assertEquals(new TimeWindow(11, 14), mergeFunction.stateWindow());
		assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(10, 14)));
		assertTrue(mergeFunction.mergedStateWindows().isEmpty());

		mergeFunction.reset();
		assertEquals(new TimeWindow(10, 15), windowSet.addWindow(new TimeWindow(11, 14), mergeFunction));
		assertFalse(mergeFunction.hasMerged());


		assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

		assertEquals(new TimeWindow(11, 14), windowSet.getStateWindow(new TimeWindow(10, 15)));

		// retire the first batch of windows
		windowSet.retireWindow(new TimeWindow(0, 6));

		try {
			windowSet.getStateWindow(new TimeWindow(0, 6));
			fail("Expected exception");
		} catch (IllegalStateException e) {
			//ignore
		}

		assertTrue(windowSet.getStateWindow(new TimeWindow(10, 15)).equals(new TimeWindow(11, 14)));
	}

	@Test
	public void testLateMerging() throws Exception {
		MergingWindowSet<TimeWindow> windowSet = new MergingWindowSet<>(EventTimeSessionWindows.withGap(Time.milliseconds(3)));

		TestingMergeFunction mergeFunction = new TestingMergeFunction();

		// add several non-overlapping initial windoww

		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 3), windowSet.addWindow(new TimeWindow(0, 3), mergeFunction));
		assertFalse(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(0, 3), windowSet.getStateWindow(new TimeWindow(0, 3)));

		mergeFunction.reset();
		assertEquals(new TimeWindow(5, 8), windowSet.addWindow(new TimeWindow(5, 8), mergeFunction));
		assertFalse(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(5, 8), windowSet.getStateWindow(new TimeWindow(5, 8)));

		mergeFunction.reset();
		assertEquals(new TimeWindow(10, 13), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
		assertFalse(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(10, 13), windowSet.getStateWindow(new TimeWindow(10, 13)));

		// add a window that merges the later two windows
		mergeFunction.reset();
		assertEquals(new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(8, 10), mergeFunction));
		assertTrue(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(5, 13), mergeFunction.mergeTarget());
		assertThat(mergeFunction.stateWindow(), anyOf(is(new TimeWindow(5, 8)), is(new TimeWindow(10, 13))));
		assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(5, 8), new TimeWindow(10, 13)));
		assertThat(mergeFunction.mergedStateWindows(), anyOf(
				containsInAnyOrder(new TimeWindow(10, 13)),
				containsInAnyOrder(new TimeWindow(5, 8))));
		assertThat(mergeFunction.mergedStateWindows(), not(hasItem(mergeFunction.mergeTarget())));

		assertEquals(new TimeWindow(0, 3), windowSet.getStateWindow(new TimeWindow(0, 3)));

		mergeFunction.reset();
		assertEquals(new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(5, 8), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		mergeFunction.reset();
		assertEquals(new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(8, 10), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		mergeFunction.reset();
		assertEquals(new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
		assertFalse(mergeFunction.hasMerged());

		assertThat(windowSet.getStateWindow(new TimeWindow(5, 13)), anyOf(is(new TimeWindow(5, 8)), is(new TimeWindow(10, 13))));

		// add a window that merges all of them together
		mergeFunction.reset();
		assertEquals(new TimeWindow(0, 13), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
		assertTrue(mergeFunction.hasMerged());
		assertEquals(new TimeWindow(0, 13), mergeFunction.mergeTarget());
		assertThat(mergeFunction.stateWindow(), anyOf(is(new TimeWindow(0, 3)), is(new TimeWindow(5, 8)), is(new TimeWindow(10, 13))));
		assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(0, 3), new TimeWindow(5, 13)));
		assertThat(mergeFunction.mergedStateWindows(), anyOf(
				containsInAnyOrder(new TimeWindow(0, 3)),
				containsInAnyOrder(new TimeWindow(5, 8)),
				containsInAnyOrder(new TimeWindow(10, 13))));
		assertThat(mergeFunction.mergedStateWindows(), not(hasItem(mergeFunction.mergeTarget())));

		assertThat(windowSet.getStateWindow(new TimeWindow(0, 13)), anyOf(is(new TimeWindow(0, 3)), is(new TimeWindow(5, 8)), is(new TimeWindow(10, 13))));
	}

	private static class TestingMergeFunction implements MergingWindowSet.MergeFunction<TimeWindow> {
		private TimeWindow target = null;
		private Collection<TimeWindow> sources = null;

		private TimeWindow stateWindow = null;
		private Collection<TimeWindow> mergedStateWindows = null;

		public void reset() {
			target = null;
			sources = null;
			stateWindow = null;
			mergedStateWindows = null;
		}

		public boolean hasMerged() {
			return target != null;
		}

		public TimeWindow mergeTarget() {
			return target;
		}

		public Collection<TimeWindow> mergeSources() {
			return sources;
		}

		public TimeWindow stateWindow() {
			return stateWindow;
		}

		public Collection<TimeWindow> mergedStateWindows() {
			return mergedStateWindows;
		}

		@Override
		public void merge(TimeWindow mergeResult,
				Collection<TimeWindow> mergedWindows,
				TimeWindow stateWindowResult,
				Collection<TimeWindow> mergedStateWindows) throws Exception {
			if (target != null) {
				fail("More than one merge for adding a Window should not occur.");
			}
			this.stateWindow = stateWindowResult;
			this.target = mergeResult;
			this.mergedStateWindows = mergedStateWindows;
			this.sources = mergedWindows;
		}
	}
}
