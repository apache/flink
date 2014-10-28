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
import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.util.Collector;

/**
 * This example shows the functionality of time based windows. It utilizes the
 * {@link ActiveTriggerPolicy} implementation in the {@link TimeTriggerPolicy}.
 */
public class TimeWindowingExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		// Prevent output from being blocked
		env.setBufferTimeout(100);

		// Trigger every 1000ms
		TriggerPolicy<Integer> triggerPolicy = new TimeTriggerPolicy<Integer>(1000L,
				new DefaultTimeStamp<Integer>(), new Extractor<Long, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer extract(Long in) {
						return in.intValue();
					}

				});

		// Always keep the newest 100 elements in the buffer
		EvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(100);

		// This reduce function does a String concat.
		ReduceFunction<Integer> reduceFunction = new ReduceFunction<Integer>() {

			/**
			 * default version ID
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}

		};

		DataStream<Tuple2<Integer, String[]>> stream = env.addSource(new CountingSourceWithSleep()).window(triggerPolicy, evictionPolicy, reduceFunction);

		stream.print();

		env.execute();
	}

	/**
	 * This data source emit one element every 0.001 sec. The output is an
	 * Integer counting the output elements. As soon as the counter reaches
	 * 10000 it is reset to 0. On each reset the source waits 5 sec. before it
	 * restarts to produce elements.
	 */
	@SuppressWarnings("serial")
	private static class CountingSourceWithSleep implements SourceFunction<Integer> {

		private int counter = 0;

		@Override
		public void invoke(Collector<Integer> collector) throws Exception {
			// continuous emit
			while (true) {
				if (counter > 9999) {
					System.out.println("Source pauses now!");
					Thread.sleep(5000);
					System.out.println("Source continouse with emitting now!");
					counter = 0;
				}
				collector.collect(counter);

				// Wait 0.001 sec. before the next emit. Otherwise the source is
				// too fast for local tests and you might always see
				// SUM[k=1..9999](k) as result.
				Thread.sleep(1);

				counter++;
			}
		}

	}
}
