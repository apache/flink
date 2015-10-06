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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 *{@code JoinedStreams} represents two {@link DataStream DataStreams} that have been joined.
 * A streaming join operation is evaluated over elements in a window.
 *
 * <p>
 * To finalize the join operation you also need to specify a {@link KeySelector} for
 * both the first and second input and a {@link WindowAssigner}.
 *
 * <p>
 * Note: Right now, the the join is being evaluated in memory so you need to ensure that the number
 * of elements per key does not get too high. Otherwise the JVM might crash.
 *
 * <p>
 * Example:
 *
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> one = ...;
 * DataStream<Tuple2<String, Integer>> twp = ...;
 *
 * DataStream<T> result = one.join(two)
 *     .where(new MyFirstKeySelector())
 *     .equalTo(new MyFirstKeySelector())
 *     .window(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
 *     .apply(new MyJoinFunction());
 * } </pre>
 */
public class JoinedStreams extends CoGroupedStreams{

	/**
	 * A join operation that does not yet have its {@link KeySelector KeySelectors} defined.
	 *
	 * @param <T1> Type of the elements from the first input
	 * @param <T2> Type of the elements from the second input
	 */
	public static class Unspecified<T1, T2> {
		DataStream<T1> input1;
		DataStream<T2> input2;

		protected Unspecified(DataStream<T1> input1,
				DataStream<T2> input2) {
			this.input1 = input1;
			this.input2 = input2;
		}

		/**
		 * Specifies a {@link KeySelector} for elements from the first input.
		 */
		public <KEY> WithKey<T1, T2, KEY> where(KeySelector<T1, KEY> keySelector)  {
			return new WithKey<>(input1, input2, keySelector, null);
		}

		/**
		 * Specifies a {@link KeySelector} for elements from the second input.
		 */
		public <KEY> WithKey<T1, T2, KEY> equalTo(KeySelector<T2, KEY> keySelector)  {
			return new WithKey<>(input1, input2, null, keySelector);
		}
	}

	/**
	 * A join operation that has {@link KeySelector KeySelectors} defined for either both or
	 * one input.
	 *
	 * <p>
	 * You need to specify a {@code KeySelector} for both inputs using {@link #where(KeySelector)}
	 * and {@link #equalTo(KeySelector)} before you can proceeed with specifying a
	 * {@link WindowAssigner} using {@link #window(WindowAssigner)}.
	 *
	 * @param <T1> Type of the elements from the first input
	 * @param <T2> Type of the elements from the second input
	 * @param <KEY> Type of the key. This must be the same for both inputs
	 */
	public static class WithKey<T1, T2, KEY> {
		DataStream<T1> input1;
		DataStream<T2> input2;

		KeySelector<T1, KEY> keySelector1;
		KeySelector<T2, KEY> keySelector2;

		protected WithKey(DataStream<T1> input1, DataStream<T2> input2, KeySelector<T1, KEY> keySelector1, KeySelector<T2, KEY> keySelector2) {
			this.input1 = input1;
			this.input2 = input2;

			this.keySelector1 = keySelector1;
			this.keySelector2 = keySelector2;
		}

		/**
		 * Specifies a {@link KeySelector} for elements from the first input.
		 */
		public WithKey<T1, T2, KEY> where(KeySelector<T1, KEY> keySelector)  {
			return new JoinedStreams.WithKey<>(input1, input2, keySelector, keySelector2);
		}

		/**
		 * Specifies a {@link KeySelector} for elements from the second input.
		 */
		public JoinedStreams.WithKey<T1, T2, KEY> equalTo(KeySelector<T2, KEY> keySelector)  {
			return new JoinedStreams.WithKey<>(input1, input2, keySelector1, keySelector);
		}

		/**
		 * Specifies the window on which the join operation works.
		 */
		public <W extends Window> JoinedStreams.WithWindow<T1, T2, KEY, W> window(WindowAssigner<? super TaggedUnion<T1, T2>, W> assigner) {
			if (keySelector1 == null || keySelector2 == null) {
				throw new UnsupportedOperationException("You first need to specify KeySelectors for both inputs using where() and equalTo().");

			}
			return new WithWindow<>(input1, input2, keySelector1, keySelector2, assigner, null, null);
		}
	}

	/**
	 * A join operation that has {@link KeySelector KeySelectors} defined for both inputs as
	 * well as a {@link WindowAssigner}.
	 *
	 * @param <T1> Type of the elements from the first input
	 * @param <T2> Type of the elements from the second input
	 * @param <KEY> Type of the key. This must be the same for both inputs
	 * @param <W> Type of {@link Window} on which the join operation works.
	 */
	public static class WithWindow<T1, T2, KEY, W extends Window> {
		private final DataStream<T1> input1;
		private final DataStream<T2> input2;

		private final KeySelector<T1, KEY> keySelector1;
		private final KeySelector<T2, KEY> keySelector2;

		private final WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner;

		private final Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger;

		private final Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor;

		protected WithWindow(DataStream<T1> input1,
				DataStream<T2> input2,
				KeySelector<T1, KEY> keySelector1,
				KeySelector<T2, KEY> keySelector2,
				WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner,
				Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger,
				Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor) {
			this.input1 = input1;
			this.input2 = input2;

			this.keySelector1 = keySelector1;
			this.keySelector2 = keySelector2;

			this.windowAssigner = windowAssigner;
			this.trigger = trigger;
			this.evictor = evictor;
		}

		/**
		 * Sets the {@code Trigger} that should be used to trigger window emission.
		 */
		public WithWindow<T1, T2, KEY, W> trigger(Trigger<? super TaggedUnion<T1, T2>, ? super W> newTrigger) {
			return new WithWindow<>(input1, input2, keySelector1, keySelector2, windowAssigner, newTrigger, evictor);
		}

		/**
		 * Sets the {@code Evictor} that should be used to evict elements from a window before emission.
		 *
		 * <p>
		 * Note: When using an evictor window performance will degrade significantly, since
		 * pre-aggregation of window results cannot be used.
		 */
		public WithWindow<T1, T2, KEY, W> evictor(Evictor<? super TaggedUnion<T1, T2>, ? super W> newEvictor) {
			return new WithWindow<>(input1, input2, keySelector1, keySelector2, windowAssigner, trigger, newEvictor);
		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window.
		 */
		public <T> DataStream<T> apply(JoinFunction<T1, T2, T> function) {
			TypeInformation<T> resultType = TypeExtractor.getBinaryOperatorReturnType(
					function,
					JoinFunction.class,
					true,
					true,
					input1.getType(),
					input2.getType(),
					"CoGroup",
					false);

			return apply(function, resultType);
		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window.
		 */
		public <T> DataStream<T> apply(FlatJoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
			//clean the closure
			function = input1.getExecutionEnvironment().clean(function);

			return input1.coGroup(input2)
					.where(keySelector1)
					.equalTo(keySelector2)
					.window(windowAssigner)
					.trigger(trigger)
					.evictor(evictor)
					.apply(new FlatJoinCoGroupFunction<>(function), resultType);

		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window.
		 */
		public <T> DataStream<T> apply(FlatJoinFunction<T1, T2, T> function) {
			TypeInformation<T> resultType = TypeExtractor.getBinaryOperatorReturnType(
					function,
					JoinFunction.class,
					true,
					true,
					input1.getType(),
					input2.getType(),
					"CoGroup",
					false);

			return apply(function, resultType);
		}

		/**
		 * Completes the join operation with the user function that is executed
		 * for each combination of elements with the same key in a window.
		 */
		public <T> DataStream<T> apply(JoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
			//clean the closure
			function = input1.getExecutionEnvironment().clean(function);

			return input1.coGroup(input2)
					.where(keySelector1)
					.equalTo(keySelector2)
					.window(windowAssigner)
					.trigger(trigger)
					.evictor(evictor)
					.apply(new JoinCoGroupFunction<>(function), resultType);

		}
	}

	/**
	 * Creates a new join operation from the two given inputs.
	 */
	public static <T1, T2> Unspecified<T1, T2> createJoin(DataStream<T1> input1, DataStream<T2> input2) {
		return new Unspecified<>(input1, input2);
	}

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
