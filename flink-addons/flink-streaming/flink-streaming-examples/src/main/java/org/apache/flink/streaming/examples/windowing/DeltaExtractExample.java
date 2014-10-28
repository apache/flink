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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.deltafunction.EuclideanDistance;
import org.apache.flink.streaming.api.windowing.extractor.FieldsFromTuple;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.util.Collector;

/**
 * This example gives an impression about how to use delta policies. It also
 * shows how extractors can be used.
 */
public class DeltaExtractExample {

	private static final int PARALLELISM = 1;

	@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		ReduceFunction<Tuple3<Double, Double, String>> concatStrings = new ReduceFunction<Tuple3<Double, Double, String>>() {
			@Override
			public Tuple3 reduce(Tuple3 value1, Tuple3 value2) throws Exception {
				return new Tuple3(value1.f0, value2.f1, value1.f2 + "|" + value2.f2);
			}
		};

		DataStream dstream = env
				.addSource(new CountingSource())
				.window(Delta.of(new EuclideanDistance(new FieldsFromTuple(0, 1)), new Tuple3(0d,
						0d, "foo"), 1.2)).every(Count.of(2)).reduce(concatStrings);

		dstream.print();
		env.execute();

	}

	@SuppressWarnings("serial")
	private static class CountingSource implements SourceFunction<Tuple3<Double, Double, String>> {

		private int counter = 0;

		@Override
		public void invoke(Collector<Tuple3<Double, Double, String>> collector) throws Exception {
			while (true) {
				if (counter > 9999) {
					counter = 0;
				}
				collector.collect(new Tuple3<Double, Double, String>((double) counter,
						(double) counter + 1, "V" + counter++));
			}
		}
	}

}
