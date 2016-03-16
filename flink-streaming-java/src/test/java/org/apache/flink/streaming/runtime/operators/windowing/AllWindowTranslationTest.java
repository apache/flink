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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * These tests verify that the api calls on
 * {@link org.apache.flink.streaming.api.datastream.AllWindowedStream} instantiate
 * the correct window operator.
 */
public class AllWindowTranslationTest extends StreamingMultipleProgramsTestBase {

	/**
	 * These tests ensure that the correct trigger is set when using event-time windows.
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void testEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof WindowOperator);
		WindowOperator winOperator1 = (WindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator1.getStateDescriptor() instanceof ReducingStateDescriptor);

		DataStream<Tuple2<String, Integer>> window2 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
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
		Assert.assertTrue(operator2 instanceof WindowOperator);
		WindowOperator winOperator2 = (WindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator2.getStateDescriptor() instanceof ListStateDescriptor);
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testNonEvicting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.trigger(CountTrigger.of(100))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof WindowOperator);
		WindowOperator winOperator1 = (WindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator1.getStateDescriptor() instanceof ReducingStateDescriptor);

		DataStream<Tuple2<String, Integer>> window2 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(100))
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
		Assert.assertTrue(operator2 instanceof WindowOperator);
		WindowOperator winOperator2 = (WindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator2.getStateDescriptor() instanceof ListStateDescriptor);
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testEvicting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof EvictingWindowOperator);
		EvictingWindowOperator winOperator1 = (EvictingWindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator1.getEvictor() instanceof CountEvictor);
		Assert.assertTrue(winOperator1.getStateDescriptor() instanceof ListStateDescriptor);

		DataStream<Tuple2<String, Integer>> window2 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(100))
				.evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
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
		Assert.assertTrue(operator2 instanceof EvictingWindowOperator);
		EvictingWindowOperator winOperator2 = (EvictingWindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator2.getEvictor() instanceof TimeEvictor);
		Assert.assertTrue(winOperator2.getStateDescriptor() instanceof ListStateDescriptor);
	}

	/**
	 * These tests ensure that a Fold buffer is used if possible
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void testFoldBuffer() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DummyFolder folder = new DummyFolder();

		DataStream<Integer> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.fold(0, folder);

		OneInputTransformation<Tuple2<String, Integer>, Integer> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Integer>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Integer> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof WindowOperator);
		WindowOperator winOperator1 = (WindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator1.getStateDescriptor() instanceof FoldingStateDescriptor);

		DataStream<Integer> window2 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.evictor(CountEvictor.of(13))
				.fold(0, folder);

		OneInputTransformation<Tuple2<String, Integer>, Integer> transform2 = (OneInputTransformation<Tuple2<String, Integer>, Integer>) window2.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Integer> operator2 = transform2.getOperator();
		Assert.assertTrue(operator2 instanceof WindowOperator);
		WindowOperator winOperator2 = (WindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator2.getStateDescriptor() instanceof ListStateDescriptor);
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	public static class DummyReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			return value1;
		}
	}

	public static class DummyFolder implements FoldFunction<Tuple2<String, Integer>, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer fold(Integer accumulator, Tuple2<String, Integer> value) throws Exception {
			return accumulator;
		}
	}

}
