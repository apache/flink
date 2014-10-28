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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.util.Collector;

/**
 * This example uses count based sliding windows to illustrate different
 * possibilities for the realization of sliding windows. Take a look on the code
 * which is commented out to see different setups.
 */
public class SlidingExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		/*
		 * SIMPLE-EXAMPLE: Use this to always keep the newest 10 elements in the
		 * buffer Resulting windows will have an overlap of 5 elements
		 */
		// TriggerPolicy<String> triggerPolicy=new
		// CountTriggerPolicy<String>(5);
		// EvictionPolicy<String> evictionPolicy=new
		// CountEvictionPolicy<String>(10);

		/*
		 * ADVANCED-EXAMPLE: Use this to have the last element of the last
		 * window as first element of the next window while the window size is
		 * always 5
		 */
		TriggerPolicy<String> triggerPolicy = new CountTriggerPolicy<String>(4, -1);
		EvictionPolicy<String> evictionPolicy = new CountEvictionPolicy<String>(5, 4);

		// This reduce function does a String concat.
		ReduceFunction<String> reduceFunction = new ReduceFunction<String>() {

			/**
			 * default version ID
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String reduce(String value1, String value2) throws Exception {
				return value1 + "|" + value2;
			}

		};

		DataStream<Tuple2<String, String[]>> stream = env.addSource(new CountingSource()).window(
				triggerPolicy, evictionPolicy, reduceFunction);

		stream.print();

		env.execute();
	}

	@SuppressWarnings("serial")
	private static class CountingSource implements SourceFunction<String> {

		private int counter = 0;

		@Override
		public void invoke(Collector<String> collector) throws Exception {
			// continuous emit
			while (true) {
				if (counter > 9999) {
					counter = 0;
				}
				collector.collect("V" + counter++);
			}
		}

	}
}
