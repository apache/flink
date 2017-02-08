/*
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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * These tests verify that the api calls on {@link WindowedStream} instantiate the correct
 * window operator.
 *
 * <p>We also create a test harness and push one element into the operator to verify
 * that we get some output.
 */
@SuppressWarnings("serial")
public class WindowTranslationTest {

	// ------------------------------------------------------------------------
	//  Rich Pre-Aggregation Functions
	// ------------------------------------------------------------------------

	/**
	 * .reduce() does not support RichReduceFunction, since the reduce function is used internally
	 * in a {@code ReducingState}.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testReduceWithRichReducerFails() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		source
			.keyBy(0)
			.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
			.reduce(new RichReduceFunction<Tuple2<String, Integer>>() {

				@Override
				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
					Tuple2<String, Integer> value2) throws Exception {
					return null;
				}
			});

		fail("exception was not thrown");
	}

	/**
	 * .aggregate() does not support RichAggregateFunction, since the AggregationFunction is used internally
	 * in a {@code AggregatingState}.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testAgrgegateWithRichFunctionFails() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.aggregate(new DummyRichAggregationFunction<Tuple2<String,Integer>>());

		fail("exception was not thrown");
	}

	/**
	 * .fold() does not support RichFoldFunction, since the fold function is used internally
	 * in a {@code FoldingState}.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testFoldWithRichFolderFails() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.fold(new Tuple2<>("", 0), new RichFoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return null;
					}
				});

		fail("exception was not thrown");
	}

	// ------------------------------------------------------------------------
	//  Merging Windows Support
	// ------------------------------------------------------------------------

	@Test
	public void testSessionWithFoldFails() throws Exception {
		// verify that fold does not work with merging windows

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
				private static final long serialVersionUID = -4567902917104921706L;

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
	}

	@Test
	public void testMergingAssignerWithNonMergingTriggerFails() throws Exception {
		// verify that we check for trigger compatibility

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		WindowedStream<String, String, TimeWindow> windowedStream = env.fromElements("Hello", "Ciao")
				.keyBy(new KeySelector<String, String>() {
					private static final long serialVersionUID = 598309916882894293L;

					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				})
				.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

		try {
			windowedStream.trigger(new Trigger<String, TimeWindow>() {
				private static final long serialVersionUID = 6558046711583024443L;

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

				@Override
				public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}
			});
		} catch (UnsupportedOperationException e) {
			// expected
			// use a catch to ensure that the exception is thrown by the fold
			return;
		}

		fail("The trigger call should fail.");
	}


	// ------------------------------------------------------------------------
	//  Reduce Translation Tests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.reduce(new DummyReducer());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ReducingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.reduce(new DummyReducer());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ReducingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}


	/**
	 * Ignored because we currently don't have the fast processing-time window operator.
	 */
	@Test
	@SuppressWarnings("rawtypes")
	@Ignore
	public void testReduceFastProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.reduce(new DummyReducer());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof AggregatingProcessingTimeWindowOperator);

		processElementAndEnsureOutput(operator, null, BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithWindowFunctionEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.reduce(reducer, new WindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple3<String, String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ReducingStateDescriptor);

		processElementAndEnsureOutput(operator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithWindowFunctionProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.reduce(new DummyReducer(), new WindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String tuple,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple3<String, String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ReducingStateDescriptor);

		processElementAndEnsureOutput(operator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	/**
	 * Test for the deprecated .apply(Reducer, WindowFunction).
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyWithPreReducerEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(reducer, new WindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple3<String, String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ReducingStateDescriptor);

		processElementAndEnsureOutput(operator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	// ------------------------------------------------------------------------
	//  Aggregate Translation Tests
	// ------------------------------------------------------------------------

	@Test
	public void testAggregateEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.aggregate(new DummyAggregationFunction());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = 
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();

		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = 
				(WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;

		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof AggregatingStateDescriptor);

		processElementAndEnsureOutput(
				winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	public void testAggregateProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.aggregate(new DummyAggregationFunction());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = 
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();

		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = 
				(WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;

		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof AggregatingStateDescriptor);

		processElementAndEnsureOutput(
				winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	public void testAggregateWithWindowFunctionEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.aggregate(new DummyAggregationFunction(), new TestWindowFunction());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window.getTransformation();

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();

		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = 
				(WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;

		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof AggregatingStateDescriptor);

		processElementAndEnsureOutput(
				operator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	public void testAggregateWithWindowFunctionProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.aggregate(new DummyAggregationFunction(), new TestWindowFunction());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window.getTransformation();

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();

		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
				(WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;

		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof AggregatingStateDescriptor);

		processElementAndEnsureOutput(
				operator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	// ------------------------------------------------------------------------
	//  Fold Translation Tests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("rawtypes")
	public void testFoldEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window1 = source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.fold(new Tuple3<>("", "", 1), new DummyFolder());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof FoldingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testFoldProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.fold(new Tuple3<>("", "", 0), new DummyFolder());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String,  Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof FoldingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testFoldWithWindowFunctionEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.fold(new Tuple3<>("", "", 0), new DummyFolder(), new WindowFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple3<String, String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						for (Tuple3<String, String, Integer> in : values) {
							out.collect(new Tuple2<>(in.f0, in.f2));
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof FoldingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testFoldWithWindowFunctionProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.fold(new Tuple3<>("", "empty", 0), new DummyFolder(), new WindowFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple3<String, String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						for (Tuple3<String, String, Integer> in : values) {
							out.collect(new Tuple2<>(in.f0, in.f2));
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof FoldingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyWithPreFolderEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(new Tuple3<>("", "", 0), new DummyFolder(), new WindowFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple3<String, String, Integer>> values,
							Collector<Tuple3<String, String, Integer>> out) throws Exception {
						for (Tuple3<String, String, Integer> in : values) {
							out.collect(new Tuple3<>(in.f0, in. f1, in.f2));
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String,String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof FoldingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	// ------------------------------------------------------------------------
	//  Apply Translation Tests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(in);
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyProcessingTimeTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(in);
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingProcessingTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));

	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithCustomTrigger() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.trigger(CountTrigger.of(1))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ReducingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testFoldWithCustomTrigger() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window1 = source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.trigger(CountTrigger.of(1))
				.fold(new Tuple3<>("", "", 1), new DummyFolder());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof FoldingStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyWithCustomTrigger() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(1))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(in);
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof EvictingWindowOperator);
		EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator = (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getEvictor() instanceof CountEvictor);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void testFoldWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window1 = source
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.fold(new Tuple3<>("", "", 1), new DummyFolder());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof EvictingWindowOperator);
		EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator = (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getEvictor() instanceof CountEvictor);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		winOperator.setOutputType((TypeInformation) window1.getType(), new ExecutionConfig());
		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.keyBy(new TupleKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(1))
				.evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key,
							TimeWindow window,
							Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						for (Tuple2<String, Integer> in : values) {
							out.collect(in);
						}
					}
				});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof EvictingWindowOperator);
		EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator = (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof CountTrigger);
		Assert.assertTrue(winOperator.getEvictor() instanceof TimeEvictor);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	/**
	 * Ensure that we get some output from the given operator when pushing in an element and
	 * setting watermark and processing time to {@code Long.MAX_VALUE}.
	 */
	private static <K, IN, OUT> void processElementAndEnsureOutput(
			OneInputStreamOperator<IN, OUT> operator,
			KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType,
			IN element) throws Exception {

		KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						operator,
						keySelector,
						keyType);

		testHarness.open();

		testHarness.setProcessingTime(0);
		testHarness.processWatermark(Long.MIN_VALUE);

		testHarness.processElement(new StreamRecord<>(element, 0));

		// provoke any processing-time/event-time triggers
		testHarness.setProcessingTime(Long.MAX_VALUE);
		testHarness.processWatermark(Long.MAX_VALUE);

		// we at least get the two watermarks and should also see an output element
		assertTrue(testHarness.getOutput().size() >= 3);

		testHarness.close();
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	public static class DummyReducer implements ReduceFunction<Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			return value1;
		}
	}

	private static class DummyFolder implements FoldFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
		@Override
		public Tuple3<String, String, Integer> fold(
				Tuple3<String, String, Integer> accumulator,
				Tuple2<String, Integer> value) throws Exception {
			return accumulator;
		}
	}

	private static class DummyAggregationFunction 
			implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> createAccumulator() {
			return new Tuple2<>("", 0);
		}

		@Override
		public void add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
			accumulator.f0 = value.f0;
			accumulator.f1 = value.f1;
		}

		@Override
		public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
			return a;
		}
	}

	private static class DummyRichAggregationFunction<T> extends RichAggregateFunction<T, T, T> {

		@Override
		public T createAccumulator() {
			return null;
		}

		@Override
		public void add(T value, T accumulator) {}

		@Override
		public T getResult(T accumulator) {
			return accumulator;
		}

		@Override
		public T merge(T a, T b) {
			return a;
		}
	}

	private static class TestWindowFunction 
			implements WindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow> {

		@Override
		public void apply(String key,
				TimeWindow window,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, String, Integer>> out) throws Exception {

			for (Tuple2<String, Integer> in : values) {
				out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
			}
		}
	}

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}
