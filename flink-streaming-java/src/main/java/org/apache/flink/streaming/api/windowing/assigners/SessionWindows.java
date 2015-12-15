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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.AbstractTime;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link WindowAssigner} that windows elements into windows based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * <p>
 * For example, in order to window into windows of 1 minute, every 10 seconds:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(TumblingTimeWindows.of(Time.of(1, MINUTES), Time.of(10, SECONDS));
 * } </pre>
 */
public class SessionWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	protected long sessionTimeout;

	protected SessionWindows(long sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
		return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		if (env.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
			return ProcessingTimeTrigger.create();
		} else {
			return EventTimeTrigger.create();
		}
	}

	@Override
	public boolean isMerging() {
		return true;
	}

	@Override
	public String toString() {
		return "SessionWindows(" + sessionTimeout + ")";
	}

	/**
	 * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns
	 * elements to sessions based on the element timestamp.
	 *
	 * @param size The session timeout, i.e. the time gap between sessions
	 * @return The policy.
	 */
	public static SessionWindows withGap(AbstractTime size) {
		return new SessionWindows(size.toMilliseconds());
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	/**
	 * Merge overlapping {@link TimeWindow}s.
	 */
	public void mergeWindows(Collection<TimeWindow> windows, WindowAssigner.MergeCallback<TimeWindow> c) {

		// sort the windows by the start time and then merge overlapping windows

		List<TimeWindow> sortedWindows = new ArrayList<>(windows);

		Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
			@Override
			public int compare(TimeWindow o1, TimeWindow o2) {
				return Long.compare(o1.getStart(), o2.getStart());
			}
		});

		List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
		Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

		for (TimeWindow candidate: sortedWindows) {
			if (currentMerge == null) {
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			} else if (currentMerge.f0.intersects(candidate)) {
				currentMerge.f0 = currentMerge.f0.cover(candidate);
				currentMerge.f1.add(candidate);
			} else {
				merged.add(currentMerge);
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			}
		}

		if (currentMerge != null) {
			merged.add(currentMerge);
		}

		for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
			if (m.f1.size() > 1) {
				c.merge(m.f1, m.f0);
			}
		}
	}

}
