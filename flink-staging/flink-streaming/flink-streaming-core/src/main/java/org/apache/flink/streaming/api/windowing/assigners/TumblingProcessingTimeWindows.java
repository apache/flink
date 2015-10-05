/**
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

import org.apache.flink.streaming.api.windowing.time.AbstractTime;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into time-based windows. The windowing is
 * based on system time. Windows cannot overlap.
 *
 * <p>
 * For example, in order to window into windows of 1 minute, every 10 seconds:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(TumblingProcessingTimeWindows.of(Time.of(1, MINUTES), Time.of(10, SECONDS));
 * } </pre>
 */
public class TumblingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private long size;

	private TumblingProcessingTimeWindows(long size) {
		this.size = size;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
		long time = System.currentTimeMillis();
		long start = time - (time % size);
		return Collections.singletonList(new TimeWindow(start, size));
	}

	public long getSize() {
		return size;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger() {
		return ProcessingTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "TumblingProcessingTimeWindows(" + size + ")";
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the current processing time.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	public static TumblingProcessingTimeWindows of(AbstractTime size) {
		return new TumblingProcessingTimeWindows(size.toMilliseconds());
	}
}
