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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.avatica.util.DateTimeUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Assigning windows from the sorted input buffers.
 *
 * <p>Assign windows and trigger aggregate calculation based on {@link WindowsGrouping}.
 * It can avoid data expansion in time sliding window case.
 *
 * <p>Tumbling window case
 * Each keyed window corresponds to only one element in the {@link WindowsGrouping} Buffer
 * (grouping keys, assigned windows, agg buffers).
 *
 * <p>Sliding window case
 * 1. If the assign pane optimization strategy is used, each keyed window corresponding to
 * {@link WindowsGrouping} buffer contains windowsSize/paneSize number of elements
 * (assuming 1s/24h window, there are 86400 elements).
 * 2. Otherwise, the maximum number of elements in {@link WindowsGrouping} buffer is the maximum
 * number of inputs in a sliding window.
 *
 * <p>In most cases, assign pane optimization should be applied, so there should not be much data
 * in {@link WindowsGrouping}, and {@link HeapWindowsGrouping} is basically okay.
 */
public abstract class WindowsGrouping implements Closeable {

	private final long windowStartOffset;

	private final long windowSize;

	private final long slideSize;

	private final int timeIndex;

	private long watermark;

	private TimeWindow nextWindow;

	private TimeWindow currentWindow;

	private int triggerWindowStartIndex;

	private boolean emptyWindowTriggered;

	private boolean isDate;

	WindowsGrouping(long windowSize, long slideSize, int timestampIndex, boolean isDate) {
		this(0L, windowSize, slideSize, timestampIndex, isDate);
	}

	WindowsGrouping(
			long offset, long windowSize, long slideSize, int timeIndex, boolean isDate) {
		this.windowStartOffset = offset;
		this.windowSize = windowSize;
		this.slideSize = slideSize;
		this.timeIndex = timeIndex;
		this.isDate = isDate;
		nextWindow = null;
		watermark = Long.MIN_VALUE;
		// index to define
		triggerWindowStartIndex = 0;
		emptyWindowTriggered = true;
	}

	/**
	 * Reset for next group.
	 */
	public void reset() {
		nextWindow = null;
		watermark = Long.MIN_VALUE;
		triggerWindowStartIndex = 0;
		emptyWindowTriggered = true;
		resetBuffer();
	}

	public void addInputToBuffer(BinaryRowData input) throws IOException {
		if (!input.isNullAt(timeIndex)) {
			addIntoBuffer(input.copy());
			advanceWatermark(getTimeValue(input));
		}
	}

	/**
	 * Advance the watermark to trigger all the possible windows.
	 * It is designed to be idempotent.
	 */
	public void advanceWatermarkToTriggerAllWindows() {
		skipEmptyWindow();
		advanceWatermark(watermark + windowSize);
	}

	/**
	 * Check if there are windows could be triggered according to the current watermark.
	 *
	 * @return true when there are windows to be triggered.
	 * It is designed to be idempotent.
	 */
	public boolean hasTriggerWindow() {
		skipEmptyWindow();
		Preconditions.checkState(watermark == Long.MIN_VALUE || nextWindow != null,
				"next trigger window cannot be null.");
		return nextWindow != null && nextWindow.getEnd() <= watermark;
	}

	/**
	 * @return the iterator of the next triggerable window's elements.
	 */
	public RowIterator<BinaryRowData> buildTriggerWindowElementsIterator() {
		currentWindow = nextWindow;
		// It is illegal to call this method after [[hasTriggerWindow()]] has returned `false`.
		Preconditions.checkState(watermark == Long.MIN_VALUE || nextWindow != null,
				"next trigger window cannot be null.");
		if (nextWindow.getEnd() > watermark) {
			throw new IllegalStateException("invalid window triggered " + currentWindow);
		}

		// advance in the stride of slideSize for hasTriggerWindow
		nextWindow = TimeWindow.of(currentWindow.getStart() + slideSize,
				currentWindow.getStart() + slideSize + windowSize);
		// build trigger window elements' iterator
		emptyWindowTriggered = true;
		onBufferEvict(triggerWindowStartIndex);
		return new WindowsElementsIterator(newBufferIterator(triggerWindowStartIndex));
	}

	/**
	 * @return the last triggered window.
	 */
	public TimeWindow getTriggerWindow() {
		return currentWindow;
	}

	private boolean belongsToCurrentWindow(BinaryRowData element) {
		long currentTimestamp = getTimeValue(element);
		if (currentTimestamp >= currentWindow.getStart() &&
				currentTimestamp < currentWindow.getEnd()) {
			// evict elements for next window
			evictForWindow(element, nextWindow);
			return true;
		}
		return false;
	}

	private boolean evictForWindow(BinaryRowData element, TimeWindow window) {
		if (getTimeValue(element) < window.getStart()) {
			triggerWindowStartIndex++;
			return true;
		} else {
			return false;
		}
	}

	private void advanceWatermark(long timestamp) {
		watermark = timestamp;
	}

	private void skipEmptyWindow() {
		if (emptyWindowTriggered && watermark != Long.MIN_VALUE) {
			nextWindow = advanceNextWindowByWatermark(watermark);
			emptyWindowTriggered = false;
		}
	}

	private TimeWindow advanceNextWindowByWatermark(long watermark) {
		int maxOverlapping = (int) Math.ceil(windowSize * 1.0 / slideSize);
		long start = getWindowStartWithOffset(watermark, windowStartOffset, slideSize);
		for (int i = 1; i < maxOverlapping; i++) {
			long nextStart = start - slideSize;
			if (nextStart + windowSize > watermark) {
				start = nextStart;
			} else {
				break;
			}
		}
		if (nextWindow == null || start > nextWindow.getStart()) {
			// This check is used in the case of jumping window.
			return TimeWindow.of(start, start + windowSize);
		} else {
			return nextWindow;
		}
	}

	private long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		long remainder = (timestamp - offset) % windowSize;
		// handle both positive and negative cases
		if (remainder < 0) {
			return timestamp - (remainder + windowSize);
		} else {
			return timestamp - remainder;
		}
	}

	private long getTimeValue(RowData row) {
		if (isDate) {
			return row.getInt(timeIndex) * DateTimeUtils.MILLIS_PER_DAY;
		} else {
			return row.getLong(timeIndex);
		}
	}

	/**
	 * Iterator for a window.
	 */
	class WindowsElementsIterator implements RowIterator<BinaryRowData> {

		private final RowIterator<BinaryRowData> bufferIterator;
		private BinaryRowData next;

		WindowsElementsIterator(RowIterator<BinaryRowData> iterator) {
			this.bufferIterator = iterator;
		}

		@Override
		public boolean advanceNext() {
			// find the first element belongs to the current window,
			// evict elements from current window, used in the case of jumping window cases.
			do {
				if (bufferIterator.advanceNext()) {
					next = bufferIterator.getRow();
				} else {
					next = null;
					return false;
				}
			} while (evictForWindow(next, currentWindow));
			// Find the last element belongs to the current window.
			if (belongsToCurrentWindow(next)) {
				emptyWindowTriggered = false;
				return true;
			} else {
				next = null;
				return false;
			}
		}

		@Override
		public BinaryRowData getRow() {
			return next;
		}

	}

	protected abstract void resetBuffer();

	protected abstract void addIntoBuffer(BinaryRowData input) throws IOException;

	protected abstract void onBufferEvict(int limitIndex);

	protected abstract RowIterator<BinaryRowData> newBufferIterator(int startIndex);

}
