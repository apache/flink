/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.ArrayList;
import java.util.List;

public class SessionWindowing {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

		input.add(new Tuple3<>("a", 1L, 1));
		input.add(new Tuple3<>("b", 1L, 1));
		input.add(new Tuple3<>("b", 3L, 1));
		input.add(new Tuple3<>("b", 5L, 1));
		input.add(new Tuple3<>("c", 6L, 1));
		// We expect to detect the session "a" earlier than this point (the old
		// functionality can only detect here when the next starts)
		input.add(new Tuple3<>("a", 10L, 1));
		// We expect to detect session "b" and "c" at this point as well
		input.add(new Tuple3<>("c", 11L, 1));

		DataStream<Tuple3<String, Long, Integer>> source = env
				.addSource(new EventTimeSourceFunction<Tuple3<String,Long,Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
						for (Tuple3<String, Long, Integer> value : input) {
							ctx.collectWithTimestamp(value, value.f1);
							ctx.emitWatermark(new Watermark(value.f1 - 1));
							if (!fileOutput) {
								System.out.println("Collected: " + value);
							}
						}
						ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
					}

					@Override
					public void cancel() {
					}
				});

		// We create sessions for each id with max timeout of 3 time units
		DataStream<Tuple3<String, Long, Integer>> aggregated = source
				.keyBy(0)
				.window(GlobalWindows.create())
				.trigger(new SessionTrigger(3L))
				.sum(2);

		if (fileOutput) {
			aggregated.writeAsText(outputPath);
		} else {
			aggregated.print();
		}

		env.execute();
	}

	private static class SessionTrigger extends Trigger<Tuple3<String, Long, Integer>, GlobalWindow> {

		private static final long serialVersionUID = 1L;

		private final Long sessionTimeout;

		private final ValueStateDescriptor<Long> stateDesc = 
				new ValueStateDescriptor<>("last-seen", Long.class, -1L);


		public SessionTrigger(Long sessionTimeout) {
			this.sessionTimeout = sessionTimeout;

		}

		@Override
		public TriggerResult onElement(Tuple3<String, Long, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

			ValueState<Long> lastSeenState = ctx.getPartitionedState(stateDesc);
			Long lastSeen = lastSeenState.value();

			Long timeSinceLastEvent = timestamp - lastSeen;

			ctx.deleteEventTimeTimer(lastSeen + sessionTimeout);

			// Update the last seen event time
			lastSeenState.update(timestamp);

			ctx.registerEventTimeTimer(timestamp + sessionTimeout);

			if (lastSeen != -1 && timeSinceLastEvent > sessionTimeout) {
				System.out.println("FIRING ON ELEMENT: " + element + " ts: " + timestamp + " last " + lastSeen);
				return TriggerResult.FIRE_AND_PURGE;
			} else {
				return TriggerResult.CONTINUE;
			}
		}

		@Override
		public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
			ValueState<Long> lastSeenState = ctx.getPartitionedState(stateDesc);
			Long lastSeen = lastSeenState.value();

			if (time - lastSeen >= sessionTimeout) {
				System.out.println("CTX: " + ctx + " Firing Time " + time + " last seen " + lastSeen);
				return TriggerResult.FIRE_AND_PURGE;
			}
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
			ValueState<Long> lastSeenState = ctx.getPartitionedState(stateDesc);
			if (lastSeenState.value() != -1) {
				ctx.deleteEventTimeTimer(lastSeenState.value() + sessionTimeout);
			}
			lastSeenState.clear();
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			if (args.length == 1) {
				fileOutput = true;
				outputPath = args[0];
			} else {
				System.err.println("Usage: SessionWindowing <result path>");
				return false;
			}
		}
		return true;
	}

}
