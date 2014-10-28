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
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

/**
 * A minimal example as introduction to the policy based windowing
 */
public class BasicExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		// This reduce function does a String concat.
		ReduceFunction<String> reduceFunction = new ReduceFunction<String>() {

			/**
			 * Auto generates version ID
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String reduce(String value1, String value2) throws Exception {
				return value1 + "|" + value2;
			}

		};

		DataStream<Tuple2<String, String[]>> stream = env.addSource(new BasicSource())
				.window(Count.of(5)).every(Count.of(2)).reduce(reduceFunction);

		stream.print();

		env.execute();
	}

	public static class BasicSource implements SourceFunction<String> {

		private static final long serialVersionUID = 1L;
		String str = new String("streaming");

		@Override
		public void invoke(Collector<String> out) throws Exception {
			// continuous emit
			while (true) {
				out.collect(str);
			}
		}
	}

}
