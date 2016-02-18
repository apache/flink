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

package org.apache.flink.streaming.api.datastream;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.operators.StreamJoinOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static java.util.Objects.requireNonNull;

/**
 *
 * {@code JoinedStreams} supports join two {@link DataStream DataStreams} on two
 * different buffer time without one window time limit.
 *
 * <p>
 * To finalize the join operation you also need to specify a {@link KeySelector} and a {@link Time}
 * for both the first and second input.
 * If timeCharacteristic is TimeCharacteristic.EventTime, you also need to specify a {@link TimestampExtractor}
 * for both the first and second input.
 *
 * <p>
 * Example:
 *
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> one = ...;
 * DataStream<Tuple2<String, Integer>> twp = ...;
 *
 *** Join base on processing time ***
 * DataStream<T> result = one.join(two)
 *     .where(new MyFirstKeySelector())
 *     .window(SlidingTimeWindows.of(Time.of(6, TimeUnit.MILLISECONDS), Time.of(2, TimeUnit.MILLISECONDS))
 *     .equalTo(new MyFirstKeySelector())
 *     .window(TumblingTimeWindows.of(Time.of(2, TimeUnit.MILLISECONDS)))
 *     .apply(new MyJoinFunction());
 *
 *** Join base on event time ***
 * DataStream<T> result = one.join(two)
 *     .where(new MyFirstKeySelector())
 *     .assignTimestamps(timestampExtractor1)
 *     .window(SlidingTimeWindows.of(Time.of(6, TimeUnit.MILLISECONDS), Time.of(2, TimeUnit.MILLISECONDS))
 *     .equalTo(new MyFirstKeySelector())
 *     .assignTimestamps(timestampExtractor2)
 *     .window(TumblingTimeWindows.of(Time.of(2, TimeUnit.MILLISECONDS)))
 *     .apply(new MyJoinFunction());
 * } </pre>
 *
 */
public class TimeJoinedStreams<T1, T2> {

	/** The first input stream */
	private final DataStream<T1> input1;

	/** The second input stream */
	private final DataStream<T2> input2;


	/** The parallelism of joined stream */
	private final int parallelism;
	/**
	 * Creates new JoinedStreams data streams, which are the first step towards building a streaming co-group.
	 *
	 * @param input1 The first data stream.
	 * @param input2 The second data stream.
	 */
	public TimeJoinedStreams(DataStream<T1> input1, DataStream<T2> input2) {
		this.input1 = requireNonNull(input1);
		this.input2 = requireNonNull(input2);
		this.parallelism = Math.max(input1.getParallelism(), input2.getParallelism());
	}

	/**
	 * Specifies a {@link KeySelector} for elements from the first input.
	 */
	public <KEY> Where<KEY> where(KeySelector<T1, KEY> keySelector)  {
		TypeInformation<KEY> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
		return new Where<>(input1.clean(keySelector), keyType);
	}

	// ------------------------------------------------------------------------

	/**
	 * CoGrouped streams that have the key for one side defined.
	 *
	 * @param <KEY> The type of the key.
	 */
	public class Where<KEY> {

		private final KeySelector<T1, KEY> keySelector1;
		private final TypeInformation<KEY> keyType;

		Where(KeySelector<T1, KEY> keySelector1, TypeInformation<KEY> keyType) {
			this.keySelector1 = keySelector1;
			this.keyType = keyType;
		}


		public WithOneWindow window(WindowAssigner<Object, TimeWindow> assigner) throws Exception {
			return new WithOneWindow(assigner, keyType);
		}

		// --------------------------------------------------------------------

		/**
		 * A join operation that has {@link KeySelector KeySelectors}
		 * and {@link Time buffers} defined for both inputs.
		 */
		public class WithOneWindow {
			private final TypeInformation<KEY> keyType;
			private final WindowAssigner<Object, TimeWindow> windowAssigner;
			private final long slideSize;
			private final long windowSize;

			long getSlideSize(WindowAssigner assigner) throws Exception {
				if(assigner instanceof TumblingTimeWindows){
					return ((TumblingTimeWindows) assigner).getSize();
				} else if(assigner instanceof SlidingTimeWindows) {
					return ((SlidingTimeWindows) assigner).getSlide();
				} else {
					throw new Exception("TimeJoin only supports time window");
				}
			}

			long getWindowSize(WindowAssigner assigner) throws Exception {
				if(assigner instanceof TumblingTimeWindows){
					return ((TumblingTimeWindows) assigner).getSize();
				} else if(assigner instanceof SlidingTimeWindows) {
					return ((SlidingTimeWindows) assigner).getSize();
				} else {
					throw new Exception("TimeJoin only supports time window");
				}
			}

			WithOneWindow(WindowAssigner<Object, TimeWindow> assigner, TypeInformation<KEY> keyType) throws Exception {
				this.windowAssigner = assigner;
				this.keyType = keyType;
				this.slideSize = getSlideSize(assigner);
				this.windowSize = getWindowSize(assigner);
			}

			/**
			 * Specifies a {@link KeySelector} for elements from the second input.
			 */
			public EqualTo equalTo(KeySelector<T2, KEY> keySelector) {
				TypeInformation<KEY> otherKey = TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
				if (!otherKey.equals(this.keyType)) {
					throw new IllegalArgumentException("The keys for the two inputs are not equal: " +
							"first key = " + this.keyType + " , second key = " + otherKey);
				}

				return new EqualTo(input2.clean(keySelector));
			}

			// --------------------------------------------------------------------

			/**
			 * A co-group operation that has {@link KeySelector KeySelectors} defined for both inputs.
			 */
			public class EqualTo {

				private final KeySelector<T2, KEY> keySelector2;

				EqualTo(KeySelector<T2, KEY> keySelector2) {
					this.keySelector2 = requireNonNull(keySelector2);
				}

				/**
				 * Specifies the window1 on which the co-group operation works.
				 */
				public WithTwoWindows<KEY> window(WindowAssigner<Object, TimeWindow> assigner) throws Exception {
					// Check slide size
					long slideSize2 = getSlideSize(assigner);
					long windowSize2 = getWindowSize(assigner);
					if(slideSize != slideSize2){
						throw new ExceptionInInitializerError("Slide size of two windows should be equal");
					}
					if(windowSize < windowSize2) {
						throw new ExceptionInInitializerError("Window size of stream1 shouldn't be less than stream2");
					}
					return new WithTwoWindows<>(input1, input2,
							keySelector1, keySelector2, keyType,
							windowAssigner, windowSize, assigner, windowSize2);
				}
			}
		}

	}

	/**
	 * A join operation that has {@link KeySelector KeySelectors} defined for both inputs as
	 * well as a {@link WindowAssigner}.
	 * Doesn't support trigger and evictor
	 *
	 * @param <KEY> Type of the key. This must be the same for both inputs
	 */
	public class WithTwoWindows<KEY> {

		private final TypeInformation<KEY> keyType;

		private final DataStream<T1> input1;
		private final DataStream<T2> input2;

		private final KeySelector<T1, KEY> keySelector1;
		private final KeySelector<T2, KEY> keySelector2;
		private final long windowSize1;
		private final long windowSize2;

		private final WindowAssigner<Object, TimeWindow> windowAssigner1;
		private final WindowAssigner<Object, TimeWindow> windowAssigner2;
		protected final Trigger<Object, TimeWindow> trigger;

		protected WithTwoWindows(DataStream<T1> input1,
								DataStream<T2> input2,
								KeySelector<T1, KEY> keySelector1,
								KeySelector<T2, KEY> keySelector2,
								TypeInformation<KEY> keyType,
								WindowAssigner<Object, TimeWindow> windowAssigner1,
								long windowSize1,
								WindowAssigner<Object, TimeWindow> windowAssigner2,
								long windowSize2) {

			this.input1 = requireNonNull(input1);
			this.input2 = requireNonNull(input2);

			this.keySelector1 = requireNonNull(keySelector1);
			this.keySelector2 = requireNonNull(keySelector2);
			this.keyType = requireNonNull(keyType);

			this.windowAssigner1 = requireNonNull(windowAssigner1);
			this.windowAssigner2 = requireNonNull(windowAssigner2);

			this.windowSize1 = windowSize1;
			this.windowSize2 = windowSize2;

			trigger = windowAssigner1.getDefaultTrigger(input1.getExecutionEnvironment());
		}

		public StreamExecutionEnvironment getExecutionEnvironment() {
			return input1.getExecutionEnvironment();
		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window.
		 */
		public <OUT> DataStream<OUT> apply(JoinFunction<T1, T2, OUT> function) {
			TypeInformation<OUT> resultType = TypeExtractor.getBinaryOperatorReturnType(
					function,
					JoinFunction.class,
					true,
					true,
					input1.getType(),
					input2.getType(),
					"Join",
					false);

			return apply(function, resultType);
		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window1.
		 */
		public <OUT> DataStream<OUT> apply(JoinFunction<T1, T2, OUT> function, TypeInformation<OUT> resultType) {
			StreamExecutionEnvironment env = getExecutionEnvironment();
			function = env.clean(function);
			boolean enableSetProcessingTime = env.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

			CoGroupedStreams.UnionTypeInfo<T1, T2> unionType = new CoGroupedStreams.UnionTypeInfo<>(input1.getType(), input2.getType());

			ListStateDescriptor<CoGroupedStreams.TaggedUnion<T1, T2>> stateDesc = new ListStateDescriptor<>("window-contents",
					unionType.createSerializer(getExecutionEnvironment().getConfig()));

			StreamJoinOperator<KEY, T1, T2, OUT> joinOperator
					= new StreamJoinOperator<>(
					new JoinCoGroupFunction<>(function),
					keySelector1,
					keySelector2,
					keyType.createSerializer(getExecutionEnvironment().getConfig()),
					windowAssigner1,
					windowAssigner1.getWindowSerializer(getExecutionEnvironment().getConfig()),
					windowSize1,
					windowAssigner2,
					windowAssigner2.getWindowSerializer(getExecutionEnvironment().getConfig()),
					windowSize2,
					stateDesc,
					input1.getType().createSerializer(getExecutionEnvironment().getConfig()),
					input2.getType().createSerializer(getExecutionEnvironment().getConfig()),
					trigger
			).enableSetProcessingTime(enableSetProcessingTime);

			TwoInputTransformation<T1, T2, OUT> twoInputTransformation
					= new TwoInputTransformation<>(
					input1.keyBy(keySelector1).getTransformation(),
					input2.keyBy(keySelector2).getTransformation(),
					"Join",
					joinOperator,
					resultType,
					parallelism
			);
			twoInputTransformation.setStateKeySelectors(keySelector1, keySelector2);
			twoInputTransformation.setStateKeyType(keyType);

			return new DataStream<>(getExecutionEnvironment(), twoInputTransformation);
		}


		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window.
		 */
		public <OUT> DataStream<OUT> apply(FlatJoinFunction<T1, T2, OUT> function) {
			TypeInformation<OUT> resultType = TypeExtractor.getBinaryOperatorReturnType(
					function,
					JoinFunction.class,
					true,
					true,
					input1.getType(),
					input2.getType(),
					"Join",
					false);

			return apply(function, resultType);
		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window1.
		 */
		public <OUT> DataStream<OUT> apply(FlatJoinFunction<T1, T2, OUT> function, TypeInformation<OUT> resultType) {
			StreamExecutionEnvironment env = getExecutionEnvironment();
			function = env.clean(function);
			boolean enableSetProcessingTime = env.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

			CoGroupedStreams.UnionTypeInfo<T1, T2> unionType = new CoGroupedStreams.UnionTypeInfo<>(input1.getType(), input2.getType());

			ListStateDescriptor<CoGroupedStreams.TaggedUnion<T1, T2>> stateDesc = new ListStateDescriptor<>("window-contents",
					unionType.createSerializer(getExecutionEnvironment().getConfig()));

			StreamJoinOperator<KEY, T1, T2, OUT> joinOperator
					= new StreamJoinOperator<>(
					new FlatJoinCoGroupFunction<>(function),
					keySelector1,
					keySelector2,
					keyType.createSerializer(getExecutionEnvironment().getConfig()),
					windowAssigner1,
					windowAssigner1.getWindowSerializer(getExecutionEnvironment().getConfig()),
					windowSize1,
					windowAssigner2,
					windowAssigner2.getWindowSerializer(getExecutionEnvironment().getConfig()),
					windowSize2,
					stateDesc,
					input1.getType().createSerializer(getExecutionEnvironment().getConfig()),
					input2.getType().createSerializer(getExecutionEnvironment().getConfig()),
					trigger
			).enableSetProcessingTime(enableSetProcessingTime);

			TwoInputTransformation<T1, T2, OUT> twoInputTransformation
					= new TwoInputTransformation<>(
					input1.keyBy(keySelector1).getTransformation(),
					input2.keyBy(keySelector2).getTransformation(),
					"Join",
					joinOperator,
					resultType,
					parallelism
			);
			twoInputTransformation.setStateKeySelectors(keySelector1, keySelector2);
			twoInputTransformation.setStateKeyType(keyType);

			return new DataStream<>(getExecutionEnvironment(), twoInputTransformation);
		}
	}


	// ------------------------------------------------------------------------
	//  Implementation of the functions
	// ------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	/**
	 * CoGroup function that does a nested-loop join to get the join result.
	 */
	private static class JoinCoGroupFunction<T1, T2, T>
			extends WrappingFunction<JoinFunction<T1, T2, T>>
			implements CoGroupFunction<T1, T2, T> {
		private static final long serialVersionUID = 1L;

		public JoinCoGroupFunction(JoinFunction<T1, T2, T> wrappedFunction) {
			super(wrappedFunction);
		}

		@Override
		public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
			for (T1 val1: first) {
				for (T2 val2: second) {
					out.collect(wrappedFunction.join(val1, val2));
				}
			}
		}
	}

	/**
	 * CoGroup function that does a nested-loop join to get the join result. (FlatJoin version)
	 */
	private static class FlatJoinCoGroupFunction<T1, T2, T>
			extends WrappingFunction<FlatJoinFunction<T1, T2, T>>
			implements CoGroupFunction<T1, T2, T> {
		private static final long serialVersionUID = 1L;

		public FlatJoinCoGroupFunction(FlatJoinFunction<T1, T2, T> wrappedFunction) {
			super(wrappedFunction);
		}

		@Override
		public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
			for (T1 val1: first) {
				for (T2 val2: second) {
					wrappedFunction.join(val1, val2, out);
				}
			}
		}
	}
}
