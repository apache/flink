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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.util.IterableIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link WindowAssigner} that windows elements into cumulative windows based on the timestamp of the
 * elements. Windows are overlap.
 */
public class CumulativeWindowAssigner extends PanedWindowAssigner<TimeWindow> implements InternalTimeWindowAssigner {

	private static final long serialVersionUID = 4895551155814656518L;

	private final long maxSize;

	private final long step;

	private final long offset;

	private final boolean isEventTime;

	protected CumulativeWindowAssigner(long maxSize, long step, long offset, boolean isEventTime) {
		if (maxSize <= 0 || step <= 0) {
			throw new IllegalArgumentException(
				"CumulativeWindowAssigner parameters must satisfy step > 0 and size > 0");
		}
		if (maxSize % step != 0) {
			throw new IllegalArgumentException(
				"CumulativeWindowAssigner requires size must be an integral multiple of step.");
		}

		this.maxSize = maxSize;
		this.step = step;
		this.offset = offset;
		this.isEventTime = isEventTime;
	}

	@Override
	public Collection<TimeWindow> assignWindows(RowData element, long timestamp) {
		List<TimeWindow> windows = new ArrayList<>();
		long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, maxSize);
		long lastEnd = start + maxSize;
		long firstEnd = TimeWindow.getWindowStartWithOffset(timestamp, offset, step) + step;
		for (long end = firstEnd; end <= lastEnd; end += step) {
			windows.add(new TimeWindow(start, end));
		}
		return windows;
	}

	@Override
	public TimeWindow assignPane(Object element, long timestamp) {
		long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, step);
		return new TimeWindow(start, start + step);
	}

	@Override
	public Iterable<TimeWindow> splitIntoPanes(TimeWindow window) {
		return new PanesIterable(window.getStart(), window.getEnd(), step);
	}

	@Override
	public TimeWindow getLastWindow(TimeWindow pane) {
		long windowStart = TimeWindow.getWindowStartWithOffset(pane.getStart(), offset, maxSize);
		// the last window is the max size window
		return new TimeWindow(windowStart, windowStart + maxSize);
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return isEventTime;
	}

	@Override
	public String toString() {
		return "CumulativeWindow(" + maxSize + ", " + step + ")";
	}

	private static class PanesIterable implements IterableIterator<TimeWindow> {

		private final long paneSize;
		private final long windowEnd;
		private long paneStart;
		private long paneEnd;

		PanesIterable(long windowStart, long windowEnd, long paneSize) {
			this.windowEnd = windowEnd;
			this.paneSize = paneSize;
			this.paneStart = windowStart;
			this.paneEnd = windowStart + paneSize;
		}

		@Override
		public boolean hasNext() {
			return paneEnd <= windowEnd;
		}

		@Override
		public TimeWindow next() {
			TimeWindow window = new TimeWindow(paneStart, paneEnd);
			paneStart += paneSize;
			paneEnd += paneSize;
			return window;
		}

		@Override
		public Iterator<TimeWindow> iterator() {
			return this;
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@link CumulativeWindowAssigner} that assigns
	 * elements to cumulative time windows based on the element timestamp.
	 *
	 * @param maxSize  The max size of the generated windows.
	 * @param step The step interval for window size to increase of the generated windows.
	 * @return The time policy.
	 */
	public static CumulativeWindowAssigner of(Duration maxSize, Duration step) {
		return new CumulativeWindowAssigner(maxSize.toMillis(), step.toMillis(), 0, true);
	}

	public CumulativeWindowAssigner withOffset(Duration offset) {
		return new CumulativeWindowAssigner(maxSize, step, offset.toMillis(), isEventTime);
	}

	public CumulativeWindowAssigner withEventTime() {
		return new CumulativeWindowAssigner(maxSize, step, offset, true);
	}

	public CumulativeWindowAssigner withProcessingTime() {
		return new CumulativeWindowAssigner(maxSize, step, offset, false);
	}
}
