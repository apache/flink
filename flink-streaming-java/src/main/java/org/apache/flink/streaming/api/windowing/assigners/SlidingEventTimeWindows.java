/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link WindowAssigner} that windows elements into sliding windows based on the timestamp of the
 * elements. Windows can possibly overlap.
 *
 * <p>For example, in order to window into windows of 1 minute, every 10 seconds:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<Tuple2<String, Integer>, String> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed =
 *   keyed.window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)));
 * } </pre>
 */
@PublicEvolving
public class SlidingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private final long size;

	private final long slide;

	private final long offset;

	protected SlidingEventTimeWindows(long size, long slide, long offset) {
		if (offset < 0 || offset >= slide || size <= 0) {
			throw new IllegalArgumentException("SlidingEventTimeWindows parameters must satisfy 0 <= offset < slide and size > 0");
		}

		this.size = size;
		this.slide = slide;
		this.offset = offset;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		if (timestamp > Long.MIN_VALUE) {
			List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
			long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
			for (long start = lastStart;
				start > timestamp - size;
				start -= slide) {
				windows.add(new TimeWindow(start, start + size));
			}
			return windows;
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
					"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
					"'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}

	public long getSize() {
		return size;
	}

	public long getSlide() {
		return slide;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return EventTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "SlidingEventTimeWindows(" + size + ", " + slide + ")";
	}

	/**
	 * Creates a new {@code SlidingEventTimeWindows} {@link WindowAssigner} that assigns
	 * elements to sliding time windows based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 * @param slide The slide interval of the generated windows.
	 * @return The time policy.
	 */
	public static SlidingEventTimeWindows of(Time size, Time slide) {
		return new SlidingEventTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), 0);
	}

	/**
	 * Creates a new {@code SlidingEventTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp and offset.
	 *
	 * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes
	 * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
	 * time windows start at 0:15:00,1:15:00,2:15:00,etc.
	 *
	 * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
	 * such as China which is using UTC+08:00,and you want a time window with size of one day,
	 * and window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.
	 * The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC time.
	 *
	 * @param size The size of the generated windows.
	 * @param slide  The slide interval of the generated windows.
	 * @param offset The offset which window start would be shifted by.
	 * @return The time policy.
	 */
	public static SlidingEventTimeWindows of(Time size, Time slide, Time offset) {
		return new SlidingEventTimeWindows(size.toMilliseconds(), slide.toMilliseconds(),
			offset.toMilliseconds() % slide.toMilliseconds());
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return true;
	}
}
