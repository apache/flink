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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.HeapWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.PreAggregatingHeapWindowBuffer;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * These tests verify that the api calls on
 * {@link org.apache.flink.streaming.api.datastream.AllWindowedStream} instantiate
 * the correct window operator.
 */
public class AllWindowTranslationTest extends StreamingMultipleProgramsTestBase {

	/**
	 * These tests ensure that the fast aligned time windows operator is used if the
	 * conditions are right.
	 *
	 * TODO: update once fast aligned time windows operator is in
	 */
	@Ignore
	@Test
	public void testFastTimeWindows() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingProcessingTimeWindows.of(1000, 100))
				.reduceWindow(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof AggregatingProcessingTimeWindowOperator);

		DataStream<Tuple2<String, Integer>> window2 = source
				.windowAll(SlidingProcessingTimeWindows.of(1000, 100))
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

	@Test
	@SuppressWarnings("rawtypes")
	public void testNonEvicting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingProcessingTimeWindows.of(1000, 100))
				.trigger(CountTrigger.of(100))
				.reduceWindow(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof NonKeyedWindowOperator);
		NonKeyedWindowOperator winOperator1 = (NonKeyedWindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTriggerTemplate() instanceof CountTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingProcessingTimeWindows);
		Assert.assertTrue(winOperator1.getWindowBufferFactory() instanceof PreAggregatingHeapWindowBuffer.Factory);

		DataStream<Tuple2<String, Integer>> window2 = source
				.windowAll(TumblingProcessingTimeWindows.of(1000))
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
		Assert.assertTrue(operator2 instanceof NonKeyedWindowOperator);
		NonKeyedWindowOperator winOperator2 = (NonKeyedWindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTriggerTemplate() instanceof CountTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingProcessingTimeWindows);
		Assert.assertTrue(winOperator2.getWindowBufferFactory() instanceof HeapWindowBuffer.Factory);
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testEvicting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DummyReducer reducer = new DummyReducer();

		DataStream<Tuple2<String, Integer>> window1 = source
				.windowAll(SlidingProcessingTimeWindows.of(1000, 100))
				.evictor(CountEvictor.of(100))
				.reduceWindow(reducer);

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 = (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window1.getTransformation();
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 = transform1.getOperator();
		Assert.assertTrue(operator1 instanceof EvictingNonKeyedWindowOperator);
		EvictingNonKeyedWindowOperator winOperator1 = (EvictingNonKeyedWindowOperator) operator1;
		Assert.assertTrue(winOperator1.getTriggerTemplate() instanceof ProcessingTimeTrigger);
		Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingProcessingTimeWindows);
		Assert.assertTrue(winOperator1.getEvictor() instanceof CountEvictor);
		Assert.assertTrue(winOperator1.getWindowBufferFactory() instanceof HeapWindowBuffer.Factory);

		DataStream<Tuple2<String, Integer>> window2 = source
				.windowAll(TumblingProcessingTimeWindows.of(1000))
				.trigger(CountTrigger.of(100))
				.evictor(TimeEvictor.of(100))
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
		Assert.assertTrue(operator2 instanceof EvictingNonKeyedWindowOperator);
		EvictingNonKeyedWindowOperator winOperator2 = (EvictingNonKeyedWindowOperator) operator2;
		Assert.assertTrue(winOperator2.getTriggerTemplate() instanceof CountTrigger);
		Assert.assertTrue(winOperator2.getWindowAssigner() instanceof TumblingProcessingTimeWindows);
		Assert.assertTrue(winOperator2.getEvictor() instanceof TimeEvictor);
		Assert.assertTrue(winOperator2.getWindowBufferFactory() instanceof HeapWindowBuffer.Factory);
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
