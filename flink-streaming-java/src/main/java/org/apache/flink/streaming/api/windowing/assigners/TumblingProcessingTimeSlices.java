/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link SliceAssigner} that sliceselements into window based on the current
 * system time of the machine the operation is running on. Windows cannot overlap.
 */
public class TumblingProcessingTimeSlices extends SliceAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private final long size;

	private final long offset;

	private TumblingProcessingTimeSlices(long size, long offset) {
		if (offset < 0 || offset >= size) {
			throw new IllegalArgumentException("TumblingProcessingTimeSlices parameters must satisfy  0 <= offset < size");
		}

		this.size = size;
		this.offset = offset;
	}

	@Override
	public TimeWindow assignSlice(Object element, long timestamp, WindowAssignerContext context) {
		final long now = context.getCurrentProcessingTime();
		long start = TimeWindow.getWindowStartWithOffset(now, offset, size);
		return new TimeWindow(start, start + size);
	}

	@Override
	public TypeInformation<TimeWindow> getWindowType() {
		return TypeInformation.of(TimeWindow.class);
	}

	public long getSize() {
		return size;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return ProcessingTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "TumblingProcessingTimeSlices(" + size + ")";
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeSlices} {@link SliceAssigner} that assigns
	 * elements to time window based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	public static TumblingProcessingTimeSlices of(Time size) {
		return new TumblingProcessingTimeSlices(size.toMilliseconds(), 0);
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeSlices} {@link SliceAssigner} that assigns
	 * elements to time window based on the element timestamp and offset.
	 *
	 * @param size The size of the generated windows.
	 * @param offset The offset which window start would be shifted by.
	 * @return The time policy.
	 */
	public static TumblingProcessingTimeSlices of(Time size, Time offset) {
		return new TumblingProcessingTimeSlices(size.toMilliseconds(), offset.toMilliseconds());
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return false;
	}
}
