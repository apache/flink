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

package org.apache.flink.table.runtime.operators.window.grouping;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.util.RowIterator;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link HeapWindowsGrouping}.
 */
public class HeapWindowsGroupingTest {

	@Test
	public void testJumpingWindowCase() throws IOException {
		Long[] ts = new Long[]{1L, 2L, 3L, 4L, 7L, 8L, 11L, 13L, 81L, 93L};
		// jumping window(10, 3)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(0, 3), TimeWindow.of(10, 13), TimeWindow.of(80, 83));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(asList(1L, 2L));
		expected.add(singletonList(11L));
		expected.add(singletonList(81L));
		verify(5000, ts, 3L, 10L, expected, expectedWindows);
	}

	@Test
	public void testNegativeCase() throws IOException {
		Long[] ts = new Long[]{-16L, -6L, -1L, 10L};
		// tumbling(10)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(-20, -10), TimeWindow.of(-10, 0), TimeWindow.of(10, 20));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(singletonList(-16L));
		expected.add(asList(-6L, -1L));
		expected.add(singletonList(10L));
		verify(5000, ts, 10L, 10L, expected, expectedWindows);

		// sliding(10, 11)
		expectedWindows = asList(TimeWindow.of(-20, -9),
				TimeWindow.of(-10, 1), TimeWindow.of(0, 11), TimeWindow.of(10, 21));
		expected.clear();
		expected.add(singletonList(-16L));
		expected.add(asList(-6L, -1L));
		expected.add(singletonList(10L));
		expected.add(singletonList(10L));
		verify(5000, ts, 11L, 10L, expected, expectedWindows);
	}

	@Test
	public void testNullValueCase() throws IOException {
		Long[] ts = new Long[]{null, 0L, 0L, 0L, 1L, 2L, 3L, 3L,
				4L, 5L, 6L, 7L, null, 7L, 8L, 9L, 11L, 12L, 12L, 12L, null};
		// sliding(4, 2)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(-2, 2), TimeWindow.of(0, 4), TimeWindow.of(2, 6), TimeWindow.of(4, 8),
				TimeWindow.of(6, 10), TimeWindow.of(8, 12), TimeWindow.of(10, 14),
				TimeWindow.of(12, 16));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(asList(0L, 0L, 0L, 1L));
		expected.add(asList(0L, 0L, 0L, 1L, 2L, 3L, 3L));
		expected.add(asList(2L, 3L, 3L, 4L, 5L));
		expected.add(asList(4L, 5L, 6L, 7L, 7L));
		expected.add(asList(6L, 7L, 7L, 8L, 9L));
		expected.add(asList(8L, 9L, 11L));
		expected.add(asList(11L, 12L, 12L, 12L));
		expected.add(asList(12L, 12L, 12L));
		verify(5000, ts, 4L, 2L, expected, expectedWindows);

		Long[] ts1 = new Long[]{null, null, 1L, 2L, 3L, 3L,
				4L, 5L, 6L, 7L, null, 7L, 8L, 9L, 11L, 12L, 12L, 12L, null};
		expectedWindows = asList(
				TimeWindow.of(-2, 2), TimeWindow.of(0, 4), TimeWindow.of(2, 6), TimeWindow.of(4, 8),
				TimeWindow.of(6, 10), TimeWindow.of(8, 12), TimeWindow.of(10, 14),
				TimeWindow.of(12, 16));
		expected.clear();
		expected.add(singletonList(1L));
		expected.add(asList(1L, 2L, 3L, 3L));
		expected.add(asList(2L, 3L, 3L, 4L, 5L));
		expected.add(asList(4L, 5L, 6L, 7L, 7L));
		expected.add(asList(6L, 7L, 7L, 8L, 9L));
		expected.add(asList(8L, 9L, 11L));
		expected.add(asList(11L, 12L, 12L, 12L));
		expected.add(asList(12L, 12L, 12L));
		verify(5000, ts1, 4L, 2L, expected, expectedWindows);
	}

	@Test
	public void testFirstOverlappingWindow() throws IOException {
		Long[] ts = new Long[]{1L};
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(-3, 2), TimeWindow.of(-2, 3), TimeWindow.of(-1, 4),
				TimeWindow.of(0, 5), TimeWindow.of(1, 6));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(singletonList(1L));
		expected.add(singletonList(1L));
		expected.add(singletonList(1L));
		expected.add(singletonList(1L));
		expected.add(singletonList(1L));
		verify(5000, ts, 5L, 1L, expected, expectedWindows);
	}

	@Test
	public void testCommonCase() throws IOException {
		Long[] ts = new Long[]{
				0L, 0L, 0L, 1L, 2L, 3L, 3L, 4L, 5L, 6L, 7L, 7L, 8L, 9L, 11L, 12L, 12L, 12L};
		// sliding(4, 2)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(-2, 2), TimeWindow.of(0, 4), TimeWindow.of(2, 6), TimeWindow.of(4, 8),
				TimeWindow.of(6, 10), TimeWindow.of(8, 12), TimeWindow.of(10, 14),
				TimeWindow.of(12, 16));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(asList(0L, 0L, 0L, 1L));
		expected.add(asList(0L, 0L, 0L, 1L, 2L, 3L, 3L));
		expected.add(asList(2L, 3L, 3L, 4L, 5L));
		expected.add(asList(4L, 5L, 6L, 7L, 7L));
		expected.add(asList(6L, 7L, 7L, 8L, 9L));
		expected.add(asList(8L, 9L, 11L));
		expected.add(asList(11L, 12L, 12L, 12L));
		expected.add(asList(12L, 12L, 12L));
		verify(5000, ts, 4L, 2L, expected, expectedWindows);

		// sliding(4, 3)
		expected.clear();
		expectedWindows = asList(
				TimeWindow.of(-3, 1),
				TimeWindow.of(0, 4), TimeWindow.of(3, 7), TimeWindow.of(6, 10),
				TimeWindow.of(9, 13), TimeWindow.of(12, 16));
		expected.add(asList(0L, 0L, 0L));
		expected.add(asList(0L, 0L, 0L, 1L, 2L, 3L, 3L));
		expected.add(asList(3L, 3L, 4L, 5L, 6L));
		expected.add(asList(6L, 7L, 7L, 8L, 9L));
		expected.add(asList(9L, 11L, 12L, 12L, 12L));
		expected.add(asList(12L, 12L, 12L));
		verify(5000, ts, 4L, 3L, expected, expectedWindows);

		// sliding(5, 5) tumbling(5)
		expected.clear();
		expectedWindows = asList(TimeWindow.of(0, 5), TimeWindow.of(5, 10),
				TimeWindow.of(10, 15));
		expected.add(asList(0L, 0L, 0L, 1L, 2L, 3L, 3L, 4L));
		expected.add(asList(5L, 6L, 7L, 7L, 8L, 9L));
		expected.add(asList(11L, 12L, 12L, 12L));
		verify(5000, ts, 5L, 5L, expected, expectedWindows);
	}

	@Test
	public void testSparseCase() throws IOException {
		Long[] ts = new Long[]{0L, 7L, 33L, 76L, 77L, 77L, 98L, 99L, 100L, 999L};
		// sliding(4, 2)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(-2, 2),
				TimeWindow.of(0, 4),
				TimeWindow.of(4, 8), TimeWindow.of(6, 10),
				TimeWindow.of(30, 34), TimeWindow.of(32, 36),
				TimeWindow.of(74, 78), TimeWindow.of(76, 80),
				TimeWindow.of(96, 100), TimeWindow.of(98, 102),
				TimeWindow.of(100, 104),
				TimeWindow.of(996, 1000), TimeWindow.of(998, 1002));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(singletonList(0L));
		expected.add(singletonList(0L));
		expected.add(singletonList(7L));
		expected.add(singletonList(7L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(asList(76L, 77L, 77L));
		expected.add(asList(76L, 77L, 77L));
		expected.add(asList(98L, 99L));
		expected.add(asList(98L, 99L, 100L));
		expected.add(singletonList(100L));
		expected.add(singletonList(999L));
		expected.add(singletonList(999L));
		verify(5000, ts, 4L, 2L, expected, expectedWindows);

		// sliding(9, 3)
		expectedWindows = asList(
				TimeWindow.of(-6, 3),
				TimeWindow.of(-3, 6),
				TimeWindow.of(0, 9),
				TimeWindow.of(3, 12), TimeWindow.of(6, 15),
				TimeWindow.of(27, 36), TimeWindow.of(30, 39), TimeWindow.of(33, 42),
				TimeWindow.of(69, 78), TimeWindow.of(72, 81), TimeWindow.of(75, 84),
				TimeWindow.of(90, 99), TimeWindow.of(93, 102), TimeWindow.of(96, 105),
				TimeWindow.of(99, 108),
				TimeWindow.of(993, 1002), TimeWindow.of(996, 1005), TimeWindow.of(999, 1008));
		expected = new ArrayList<>();
		expected.add(singletonList(0L));
		expected.add(singletonList(0L));
		expected.add(asList(0L, 7L));
		expected.add(singletonList(7L));
		expected.add(singletonList(7L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(asList(76L, 77L, 77L));
		expected.add(asList(76L, 77L, 77L));
		expected.add(asList(76L, 77L, 77L));
		expected.add(singletonList(98L));
		expected.add(asList(98L, 99L, 100L));
		expected.add(asList(98L, 99L, 100L));
		expected.add(asList(99L, 100L));
		expected.add(singletonList(999L));
		expected.add(singletonList(999L));
		expected.add(singletonList(999L));
		verify(5000, ts, 9L, 3L, expected, expectedWindows);
	}

	@Test
	public void testSingleInputCase() throws IOException {
		Long[] ts = new Long[]{33L};
		// sliding(4, 2)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(30, 34), TimeWindow.of(32, 36));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		verify(5000, ts, 4L, 2L, expected, expectedWindows);

		// sliding(19, 3)
		expectedWindows = asList(
				TimeWindow.of(15, 34), TimeWindow.of(18, 37),
				TimeWindow.of(21, 40), TimeWindow.of(24, 43),
				TimeWindow.of(27, 46), TimeWindow.of(30, 49),
				TimeWindow.of(33, 52));
		expected.clear();
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		expected.add(singletonList(33L));
		verify(5000, ts, 19L, 3L, expected, expectedWindows);
	}

	@Test(expected = IOException.class)
	public void testOOM() throws IOException {
		Long[] ts = new Long[]{33L, 33L, 33L, 33L, 33L, 33L};
		// sliding(4, 2)
		verify(5, ts, 4L, 2L, new ArrayList<>(), new ArrayList<>());
	}

	@Test
	public void testAdvanceWatermarkFirst() throws IOException {
		Long[] ts = new Long[]{16L};
		// sliding(8, 4)
		List<TimeWindow> expectedWindows = asList(
				TimeWindow.of(12, 20), TimeWindow.of(16, 24));
		List<List<Long>> expected = new ArrayList<>();
		expected.add(singletonList(16L));
		expected.add(singletonList(16L));

		List<List<Long>> actual = new ArrayList<>();
		List<TimeWindow> windows = new ArrayList<>();
		HeapWindowsGrouping grouping = new HeapWindowsGrouping(5000, 0L, 8L, 4L, 0, false);
		RowIterator<BinaryRowData> iterator = new HeapWindowsGroupingTest.TestInputIterator(ts);
		grouping.addInputToBuffer(iterator.getRow());
		// watermark to trigger all the windows first
		grouping.advanceWatermarkToTriggerAllWindows();
		processTriggerWindow(actual, windows, grouping);
		assertEquals(expectedWindows, windows);
		assertEquals(expected, actual);
	}

	@Test(expected = IllegalStateException.class)
	public void testInvalidWindowTrigger() throws IOException {
		Long[] ts = new Long[]{8L};
		RowIterator<BinaryRowData> iterator = new HeapWindowsGroupingTest.TestInputIterator(ts);
		HeapWindowsGrouping grouping = new HeapWindowsGrouping(5000, 0L, 8L, 4L, 0, false);
		grouping.addInputToBuffer(iterator.getRow());

		System.out.println("valid window trigger");
		RowIterator<BinaryRowData> iter = grouping.buildTriggerWindowElementsIterator();
		TimeWindow window = grouping.getTriggerWindow();
		List<Long> buffer = new ArrayList<>();
		while (iter.advanceNext()) {
			buffer.add(iter.getRow().getLong(0));
		}
		assertEquals(TimeWindow.of(0, 8L), window);
		assertEquals(Collections.emptyList(), buffer);

		System.out.println("try invalid window trigger");
		grouping.buildTriggerWindowElementsIterator();
	}

	private void verify(
			int limit, Long[] ts, long windowSize, long slideSize, List<List<Long>> expected,
			List<TimeWindow> expectedWindows) throws IOException {
		List<List<Long>> actual = new ArrayList<>();
		List<TimeWindow> windows = new ArrayList<>();
		HeapWindowsGrouping grouping = new HeapWindowsGrouping(limit, 0L, windowSize, slideSize, 0, false);

		RowIterator<BinaryRowData> iterator = new HeapWindowsGroupingTest.TestInputIterator(ts);
		while (iterator.advanceNext()) {
			BinaryRowData input = iterator.getRow();
			grouping.addInputToBuffer(input);
			processTriggerWindow(actual, windows, grouping);
		}
		grouping.advanceWatermarkToTriggerAllWindows();
		processTriggerWindow(actual, windows, grouping);

		assertEquals(expectedWindows, windows);
		assertEquals(expected, actual);
	}

	private void processTriggerWindow(
			List<List<Long>> actual, List<TimeWindow> windows, HeapWindowsGrouping grouping) {
		while (grouping.hasTriggerWindow()) {
			RowIterator<BinaryRowData> iter = grouping.buildTriggerWindowElementsIterator();
			TimeWindow window = grouping.getTriggerWindow();
			List<Long> buffer = new ArrayList<>();
			while (iter.advanceNext()) {
				buffer.add(iter.getRow().getLong(0));
			}
			if (!buffer.isEmpty()) {
				actual.add(buffer);
				windows.add(window);
			}
		}
	}

	private class TestInputIterator implements RowIterator<BinaryRowData> {
		private BinaryRowData row = new BinaryRowData(1);
		private BinaryRowWriter writer = new BinaryRowWriter(row);
		private List<Long> assignedWindowStart;
		private int count;

		TestInputIterator(Long[] assignedWindowStart) {
			this.assignedWindowStart = Arrays.asList(assignedWindowStart);
			this.assignedWindowStart.sort((o1, o2) -> {
				if (o1 == null && o2 == null) {
					return 0;
				} else {
					if (o1 == null) {
						return -1;
					}
					if (o2 == null) {
						return 1;
					}
					return (int) (o1 - o2);
				}
			});
			this.count = 0;
		}

		@Override
		public boolean advanceNext() {
			return count < assignedWindowStart.size();
		}

		@Override
		public BinaryRowData getRow() {
			writer.reset();
			if (assignedWindowStart.get(count) == null) {
				writer.setNullAt(0);
			} else {
				writer.writeLong(0, assignedWindowStart.get(count));
			}
			writer.complete();
			count++;
			return row;
		}
	}
}
