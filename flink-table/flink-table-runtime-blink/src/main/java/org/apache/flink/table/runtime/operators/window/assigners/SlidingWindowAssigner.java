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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.util.IterableIterator;
import org.apache.flink.util.MathUtils;

import org.apache.commons.math3.util.ArithmeticUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link WindowAssigner} that windows elements into sliding windows based on the timestamp of the
 * elements. Windows can possibly overlap.
 */
public class SlidingWindowAssigner extends PanedWindowAssigner<TimeWindow> implements InternalTimeWindowAssigner {

	private static final long serialVersionUID = 4895551155814656518L;

	private final long size;

	private final long slide;

	private final long offset;

	private final long paneSize;

	private final int numPanesPerWindow;

	private final boolean isEventTime;

	protected SlidingWindowAssigner(long size, long slide, long offset, boolean isEventTime) {
		if (size <= 0 || slide <= 0) {
			throw new IllegalArgumentException(
				"SlidingWindowAssigner parameters must satisfy slide > 0 and size > 0");
		}

		this.size = size;
		this.slide = slide;
		this.offset = offset;
		this.isEventTime = isEventTime;
		this.paneSize = ArithmeticUtils.gcd(size, slide);
		this.numPanesPerWindow = MathUtils.checkedDownCast(size / paneSize);
	}

	@Override
	public Collection<TimeWindow> assignWindows(BaseRow element, long timestamp) {
		List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
		long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
		for (long start = lastStart; start > timestamp - size; start -= slide) {
			windows.add(new TimeWindow(start, start + size));
		}
		return windows;
	}

	@Override
	public TimeWindow assignPane(Object element, long timestamp) {
		long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, paneSize);
		return new TimeWindow(start, start + paneSize);
	}

	@Override
	public Iterable<TimeWindow> splitIntoPanes(TimeWindow window) {
		return new PanesIterable(window.getStart(), paneSize, numPanesPerWindow);
	}

	@Override
	public TimeWindow getLastWindow(TimeWindow pane) {
		long lastStart = TimeWindow.getWindowStartWithOffset(pane.getStart(), offset, slide);
		return new TimeWindow(lastStart, lastStart + size);
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
		return "SlidingWindow(" + size + ", " + slide + ")";
	}

	private static class PanesIterable implements IterableIterator<TimeWindow> {

		private final long paneSize;
		private long paneStart;
		private int numPanesRemaining;

		PanesIterable(long paneStart, long paneSize, int numPanesPerWindow) {
			this.paneStart = paneStart;
			this.paneSize = paneSize;
			this.numPanesRemaining = numPanesPerWindow;
		}

		@Override
		public boolean hasNext() {
			return numPanesRemaining > 0;
		}

		@Override
		public TimeWindow next() {
			TimeWindow window = new TimeWindow(paneStart, paneStart + paneSize);
			numPanesRemaining--;
			paneStart += paneSize;
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
	 * Creates a new {@code SlidingEventTimeWindows} {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner} that assigns
	 * elements to sliding time windows based on the element timestamp.
	 *
	 * @param size  The size of the generated windows.
	 * @param slide The slide interval of the generated windows.
	 * @return The time policy.
	 */
	public static SlidingWindowAssigner of(Duration size, Duration slide) {
		return new SlidingWindowAssigner(size.toMillis(), slide.toMillis(), 0, true);
	}

	public SlidingWindowAssigner withOffset(Duration offset) {
		return new SlidingWindowAssigner(size, slide, offset.toMillis(), isEventTime);
	}

	public SlidingWindowAssigner withEventTime() {
		return new SlidingWindowAssigner(size, slide, offset, true);
	}

	public SlidingWindowAssigner withProcessingTime() {
		return new SlidingWindowAssigner(size, slide, offset, false);
	}
}
