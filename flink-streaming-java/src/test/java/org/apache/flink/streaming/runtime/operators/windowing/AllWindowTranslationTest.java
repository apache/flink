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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * These tests verify that the api calls on {@link AllWindowedStream} instantiate the correct
 * window operator.
 *
 * <p>We also create a test harness and push one element into the operator to verify
 * that we get some output.
 */
@SuppressWarnings("serial")
public class AllWindowTranslationTest {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	// ------------------------------------------------------------------------
	//  rich function tests
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
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = -6448847205314995812L;

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return null;
					}
				});

		fail("exception was not thrown");
	}

	/**
	 * .aggregate() does not support RichAggregateFunction, since the AggregateFunction is used internally
	 * in an {@code AggregatingState}.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testAggregateWithRichFunctionFails() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.aggregate(new DummyRichAggregationFunction<Tuple2<String, Integer>>());

		fail("exception was not thrown");
	}

	// ------------------------------------------------------------------------
	//  Merging Windows Support
	// ------------------------------------------------------------------------

	@Test
	public void testMergingAssignerWithNonMergingTriggerFails() throws Exception {
		// verify that we check for trigger compatibility
		expectedException.expect(UnsupportedOperationException.class);
		expectedException.expectMessage("A merging window assigner cannot be used with a trigger" +
			" that does not support merging");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromElements("Hello", "Ciao")
				.windowAll(EventTimeSessionWindows.withGap(Time.seconds(5)))
				.trigger(new Trigger<String, TimeWindow>() {
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
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testMergingWindowsWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window1 = source
				.windowAll(EventTimeSessionWindows.withGap(Time.seconds(5)))
				.evictor(CountEvictor.of(5))
				.process(new TestProcessAllWindowFunction());

		OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator = (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof EventTimeSessionWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	// ------------------------------------------------------------------------
	//  reduce() translation tests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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
				.windowAll(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithWindowFunctionEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple3<String, String, Integer>> window = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.reduce(reducer, new AllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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
				.windowAll(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.reduce(new DummyReducer(), new AllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithProcessWindowFunctionEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple3<String, String, Integer>> window = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.reduce(reducer, new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Context ctx,
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
	public void testReduceWithProcessWindowFunctionProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple3<String, String, Integer>> window = source
				.windowAll(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.reduce(new DummyReducer(), new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Context ctx,
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

	@Test
	@SuppressWarnings("rawtypes")
	public void testReduceWithEvictorAndProcessFunction() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.reduce(
						reducer,
						new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
							@Override
							public void process(
									Context context,
									Iterable<Tuple2<String, Integer>> elements,
									Collector<Tuple2<String, Integer>> out) throws Exception {
								for (Tuple2<String, Integer> in : elements) {
									out.collect(in);
								}
							}
						});

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
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(reducer, new AllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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
	//  aggregate() translation tests
	// ------------------------------------------------------------------------

	@Test
	public void testAggregateEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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
				.windowAll(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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

		DataStream<Tuple3<String, String, Integer>> window = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.aggregate(new DummyAggregationFunction(), new TestAllWindowFunction());

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
				.windowAll(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.aggregate(new DummyAggregationFunction(), new TestAllWindowFunction());

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

	@Test
	public void testAggregateWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.aggregate(new DummyAggregationFunction());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();

		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
				(WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;

		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(
				winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	public void testAggregateWithEvictorAndProcessFunction() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.aggregate(
						new DummyAggregationFunction(),
						new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
							@Override
							public void process(
									Context context,
									Iterable<Tuple2<String, Integer>> elements,
									Collector<Tuple2<String, Integer>> out) throws Exception {
								for (Tuple2<String, Integer> in : elements) {
									out.collect(in);
								}
							}
						});

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();

		Assert.assertTrue(operator instanceof WindowOperator);
		WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
				(WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;

		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(
				winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	// ------------------------------------------------------------------------
	//  process() translation tests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("rawtypes")
	public void testProcessEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Context ctx,
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
	public void testProcessProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Context ctx,
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
	public void testProcessWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(1))
				.evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Context ctx,
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

	@Test
	@SuppressWarnings("rawtypes")
	public void testProcessWithCustomTrigger() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(1))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Context ctx,
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

	// ------------------------------------------------------------------------
	//  apply() translation tests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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
				.windowAll(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
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
	public void testApplyWithCustomTrigger() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(1))
				.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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
				.windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS)))
				.evictor(CountEvictor.of(100))
				.reduce(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof EvictingWindowOperator);
		EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator = (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
		Assert.assertTrue(winOperator.getTrigger() instanceof EventTimeTrigger);
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof SlidingEventTimeWindows);
		Assert.assertTrue(winOperator.getEvictor() instanceof CountEvictor);
		Assert.assertTrue(winOperator.getStateDescriptor() instanceof ListStateDescriptor);

		processElementAndEnsureOutput(winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, new Tuple2<>("hello", 1));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testApplyWithEvictor() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
				.trigger(CountTrigger.of(1))
				.evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
				.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
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
		Assert.assertTrue(winOperator.getWindowAssigner() instanceof TumblingEventTimeWindows);
		Assert.assertTrue(winOperator.getEvictor() instanceof TimeEvictor);
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

	private static class DummyReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			return value1;
		}
	}

	private static class DummyAggregationFunction
			implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> createAccumulator() {
			return new Tuple2<>("", 0);
		}

		@Override
		public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
			accumulator.f0 = value.f0;
			accumulator.f1 = value.f1;
			return accumulator;
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
		public T add(T value, T accumulator) {
			return accumulator;
		}

		@Override
		public T getResult(T accumulator) {
			return accumulator;
		}

		@Override
		public T merge(T a, T b) {
			return a;
		}
	}

	private static class TestAllWindowFunction
			implements AllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow> {

		@Override
		public void apply(
				TimeWindow window,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, String, Integer>> out) throws Exception {

			for (Tuple2<String, Integer> in : values) {
				out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
			}
		}
	}

	private static class TestProcessAllWindowFunction
			extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, TimeWindow> {

		@Override
		public void process(Context ctx,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, String, Integer>> out) throws Exception {

			for (Tuple2<String, Integer> in : values) {
				out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
			}
		}
	}

}
