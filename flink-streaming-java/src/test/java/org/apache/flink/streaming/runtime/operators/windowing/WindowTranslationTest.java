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
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * These tests verify that the api calls on
 * {@link WindowedStream} instantiate
 * the correct window operator.
 */
public class WindowTranslationTest extends StreamingMultipleProgramsTestBase {

	/**
	 * .reduce() does not support RichReduceFunction, since the reduce function is used internally
	 * in a {@code ReducingState}.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testReduceFailWithRichReducer() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> window1 = source
			.keyBy(0)
			.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
			.reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
					Tuple2<String, Integer> value2) throws Exception {
					return null;
				}
			});
	}

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
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof WindowOperator);
		WindowOperator winOperator1 = (WindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator1.getStateDescriptor() instanceof ReducingStateDescriptor);

		DataStream<Tuple2<String, Integer>> window2 = source
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
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
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(100))
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
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(100))
				.evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
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
		Assert.assertTrue(operator2 instanceof EvictingWindowOperator);
		EvictingWindowOperator winOperator2 = (EvictingWindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator2.getEvictor() instanceof TimeEvictor);
		Assert.assertTrue(winOperator2.getStateDescriptor() instanceof ListStateDescriptor);
	}

	@Test
	public void testSessionWithFold() throws Exception {
		// verify that fold does not work with merging windows

		StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();

		WindowedStream<String, String, TimeWindow> windowedStream = env.fromElements("Hello", "Ciao")
				.keyBy(new KeySelector<String, String>() {
					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				})
				.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

		try {
			windowedStream.fold("", new FoldFunction<String, String>() {
				@Override
				public String fold(String accumulator, String value) throws Exception {
					return accumulator;
				}
			});
		} catch (UnsupportedOperationException e) {
			// expected
			// use a catch to ensure that the exception is thrown by the fold
			return;
		}

		fail("The fold call should fail.");

		env.execute();
	}

	@Test
	public void testMergingAssignerWithNonMergingTrigger() throws Exception {
		// verify that we check for trigger compatibility

		StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();

		WindowedStream<String, String, TimeWindow> windowedStream = env.fromElements("Hello", "Ciao")
				.keyBy(new KeySelector<String, String>() {
					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				})
				.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

		try {
			windowedStream.trigger(new Trigger<String, TimeWindow>() {
				@Override
				public TriggerResult onElement(String element,
						long timestamp,
						TimeWindow window,
						TriggerContext ctx) throws Exception {
					return null;
				}

				@Override
				public TriggerResult onProcessingTime(long time,
						TimeWindow window,
						TriggerContext ctx) throws Exception {
					return null;
				}

				@Override
				public TriggerResult onEventTime(long time,
						TimeWindow window,
						TriggerContext ctx) throws Exception {
					return null;
				}

				@Override
				public boolean canMerge() {
					return false;
				}
			});
		} catch (UnsupportedOperationException e) {
			// expected
			// use a catch to ensure that the exception is thrown by the fold
			return;
		}

		fail("The trigger call should fail.");

		env.execute();
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
}
