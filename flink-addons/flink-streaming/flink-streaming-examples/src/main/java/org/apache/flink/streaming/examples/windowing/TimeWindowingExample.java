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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.util.Collector;

/**
 * This example shows the functionality of time based windows. It utilizes the
 * {@link ActiveTriggerPolicy} implementation in the
 * {@link ActiveTimeTriggerPolicy}.
 */
public class TimeWindowingExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		// Prevent output from being blocked
		env.setBufferTimeout(100);

		KeySelector<Integer, Integer> myKey = new KeySelector<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				if (value < 2) {
					return 0;
				} else {
					return 1;
				}
			}

		};

		DataStream<Integer> stream = env.addSource(new CountingSourceWithSleep())
				.window(Count.of(100))
				.every(Time.of(1000, TimeUnit.MILLISECONDS))
				.groupBy(myKey)
				.sum();

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
			Random rnd = new Random();
			// continuous emit
			while (true) {
				if (counter > 9999) {
					System.out.println("Source pauses now!");
					Thread.sleep(5000);
					System.out.println("Source continouse with emitting now!");
					counter = 0;
				}
				collector.collect(rnd.nextInt(9) + 1);

				// Wait 0.001 sec. before the next emit. Otherwise the source is
				// too fast for local tests and you might always see
				// SUM[k=1..9999](k) as result.
				Thread.sleep(1);

				counter++;
			}
		}

	}
}
