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
package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * These tests verify that the api calls on
 * {@link WindowedStream} instantiate
 * the correct window operator.
 */
public class TimeWindowTranslationTest extends StreamingMultipleProgramsTestBase {

	/**
	 * These tests ensure that the fast aligned time windows operator is used if the
	 * conditions are right.
	 */
	@Test
	public void testFastTimeWindows() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(0)
				.timeWindow(Time.of(1000, TimeUnit.MILLISECONDS), Time.of(100, TimeUnit.MILLISECONDS))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof AggregatingProcessingTimeWindowOperator);

		DataStream<Tuple2<String, Integer>> window2 = source
				.keyBy(0)
				.timeWindow(Time.of(1000, TimeUnit.MILLISECONDS))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(Tuple tuple,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {

					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform2 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window2.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator2 = transform2.getOperator();
		Assert.assertTrue(operator2 instanceof AccumulatingProcessingTimeWindowOperator);
	}

	/**
	 * These tests ensure that the fast aligned time windows operator is used if the
	 * conditions are right.
	 *
	 * TODO: update once the fast aligned time windows operator is in
	 */
	@Ignore
	@Test
	public void testNonParallelFastTimeWindows() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.timeWindowAll(Time.of(1000, TimeUnit.MILLISECONDS),
						Time.of(100, TimeUnit.MILLISECONDS))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof AggregatingProcessingTimeWindowOperator);

		DataStream<Tuple2<String, Integer>> window2 = source
				.timeWindowAll(Time.of(1000, TimeUnit.MILLISECONDS))
				.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {

					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform2 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window2.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator2 = transform2.getOperator();
		Assert.assertTrue(operator2 instanceof AccumulatingProcessingTimeWindowOperator);
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	public static class DummyReducer extends RichReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			return value1;
		}
	}
}
