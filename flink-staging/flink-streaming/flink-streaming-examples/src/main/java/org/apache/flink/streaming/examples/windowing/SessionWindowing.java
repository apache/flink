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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.policy.CentralActiveTrigger;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;

import java.util.ArrayList;
import java.util.List;

public class SessionWindowing {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<Tuple3<String, Long, Integer>>();

		input.add(new Tuple3<String, Long, Integer>("a", 1L, 1));
		input.add(new Tuple3<String, Long, Integer>("b", 1L, 1));
		input.add(new Tuple3<String, Long, Integer>("b", 3L, 1));
		input.add(new Tuple3<String, Long, Integer>("b", 5L, 1));
		input.add(new Tuple3<String, Long, Integer>("c", 6L, 1));
		// We expect to detect the session "a" earlier than this point (the old
		// functionality can only detect here when the next starts)
		input.add(new Tuple3<String, Long, Integer>("a", 10L, 1));
		// We expect to detect session "b" and "c" at this point as well
		input.add(new Tuple3<String, Long, Integer>("c", 11L, 1));

		DataStream<Tuple3<String, Long, Integer>> source = env
				.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
					int index = 0;

					@Override
					public boolean reachedEnd() throws Exception {
						return index >= input.size();
					}

					@Override
					public Tuple3<String, Long, Integer> next() throws Exception {
						Tuple3<String, Long, Integer> result = input.get(index);
						index++;
						if (!fileOutput) {
							System.out.println("Collected: " + result);
							Thread.sleep(3000);
						}
						return result;
					}

				});

		// We create sessions for each id with max timeout of 3 time units
		DataStream<Tuple3<String, Long, Integer>> aggregated = source.groupBy(0)
				.window(new SessionTriggerPolicy(3L),
						new TumblingEvictionPolicy<Tuple3<String, Long, Integer>>()).sum(2)
				.flatten();

		if (fileOutput) {
			aggregated.writeAsText(outputPath);
		} else {
			aggregated.print();
		}

		env.execute();
	}

	private static class SessionTriggerPolicy implements
			CentralActiveTrigger<Tuple3<String, Long, Integer>> {

		private static final long serialVersionUID = 1L;

		private volatile Long lastSeenEvent = 1L;
		private Long sessionTimeout;

		public SessionTriggerPolicy(Long sessionTimeout) {
			this.sessionTimeout = sessionTimeout;

		}

		@Override
		public boolean notifyTrigger(Tuple3<String, Long, Integer> datapoint) {

			Long eventTimestamp = datapoint.f1;
			Long timeSinceLastEvent = eventTimestamp - lastSeenEvent;

			// Update the last seen event time
			lastSeenEvent = eventTimestamp;

			if (timeSinceLastEvent > sessionTimeout) {
				return true;
			} else {
				return false;
			}
		}

		@Override
		public Object[] notifyOnLastGlobalElement(Tuple3<String, Long, Integer> datapoint) {
			Long eventTimestamp = datapoint.f1;
			Long timeSinceLastEvent = eventTimestamp - lastSeenEvent;

			// Here we dont update the last seen event time because this data
			// belongs to a different group

			if (timeSinceLastEvent > sessionTimeout) {
				return new Object[]{datapoint};
			} else {
				return null;
			}
		}

		@Override
		public SessionTriggerPolicy clone() {
			return new SessionTriggerPolicy(sessionTimeout);
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
