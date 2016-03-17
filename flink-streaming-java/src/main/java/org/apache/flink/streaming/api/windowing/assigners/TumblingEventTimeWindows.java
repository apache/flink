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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into windows based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * <p>
 * For example, in order to window into windows of 1 minute:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<Tuple2<String, Integer>, String> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed =
 *   keyed.window(TumblingEventTimeWindows.of(Time.minutes(1)));
 * } </pre>
 */
@PublicEvolving
public class TumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private long size;

	protected TumblingEventTimeWindows(long size) {
		this.size = size;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
		if (timestamp > Long.MIN_VALUE) {
			// Long.MIN_VALUE is currently assigned when no timestamp is present
			long start = timestamp - (timestamp % size);
			return Collections.singletonList(new TimeWindow(start, start + size));
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
					"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
					"'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}

	public long getSize() {
		return size;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return EventTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "TumblingEventTimeWindows(" + size + ")";
	}

	/**
	 * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	public static TumblingEventTimeWindows of(Time size) {
		return new TumblingEventTimeWindows(size.toMilliseconds());
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}
}
