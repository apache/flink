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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import static java.util.Objects.requireNonNull;

/**
 * {@code JoinedStreams} represents two {@link DataStream DataStreams} that have been joined. A
 * streaming join operation is evaluated over elements in a window.
 *
 * <p>To finalize the join operation you also need to specify a {@link KeySelector} for both the
 * first and second input and a {@link WindowAssigner}.
 *
 * <p>Note: Right now, the join is being evaluated in memory so you need to ensure that the number
 * of elements per key does not get too high. Otherwise the JVM might crash.
 *
 * <p>Example:
 *
 * <pre>{@code
 * DataStream<Tuple2<String, Integer>> one = ...;
 * DataStream<Tuple2<String, Integer>> two = ...;
 *
 * DataStream<T> result = one.join(two)
 *     .where(new MyFirstKeySelector())
 *     .equalTo(new MyFirstKeySelector())
 *     .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
 *     .apply(new MyJoinFunction());
 * }</pre>
 */
@Public
public class JoinedStreams<T1, T2> {

    /** The first input stream. */
    private final DataStream<T1> input1;

    /** The second input stream. */
    private final DataStream<T2> input2;

    /**
     * Creates new JoinedStreams data streams, which are the first step towards building a streaming
     * co-group.
     *
     * @param input1 The first data stream.
     * @param input2 The second data stream.
     */
    public JoinedStreams(DataStream<T1> input1, DataStream<T2> input2) {
        this.input1 = requireNonNull(input1);
        this.input2 = requireNonNull(input2);
    }

    /**
     * Specifies a {@link KeySelector} for elements from the first input.
     *
     * @param keySelector The KeySelector to be used for extracting the key for partitioning.
     */
    public <KEY> Where<KEY> where(KeySelector<T1, KEY> keySelector) {
        requireNonNull(keySelector);
        final TypeInformation<KEY> keyType =
                TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
        return where(keySelector, keyType);
    }

    /**
     * Specifies a {@link KeySelector} for elements from the first input with explicit type
     * information for the key type.
     *
     * @param keySelector The KeySelector to be used for extracting the first input's key for
     *     partitioning.
     * @param keyType The type information describing the key type.
     */
    public <KEY> Where<KEY> where(KeySelector<T1, KEY> keySelector, TypeInformation<KEY> keyType) {
        requireNonNull(keySelector);
        requireNonNull(keyType);
        return new Where<>(input1.clean(keySelector), keyType);
    }

    // ------------------------------------------------------------------------

    /**
     * Joined streams that have the key for one side defined.
     *
     * @param <KEY> The type of the key.
     */
    @Public
    public class Where<KEY> {

        private final KeySelector<T1, KEY> keySelector1;
        private final TypeInformation<KEY> keyType;

        Where(KeySelector<T1, KEY> keySelector1, TypeInformation<KEY> keyType) {
            this.keySelector1 = keySelector1;
            this.keyType = keyType;
        }

        /**
         * Specifies a {@link KeySelector} for elements from the second input.
         *
         * @param keySelector The KeySelector to be used for extracting the second input's key for
         *     partitioning.
         */
        public EqualTo equalTo(KeySelector<T2, KEY> keySelector) {
            requireNonNull(keySelector);
            final TypeInformation<KEY> otherKey =
                    TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
            return equalTo(keySelector, otherKey);
        }

        /**
         * Specifies a {@link KeySelector} for elements from the second input with explicit type
         * information for the key type.
         *
         * @param keySelector The KeySelector to be used for extracting the second input's key for
         *     partitioning.
         * @param keyType The type information describing the key type.
         */
        public EqualTo equalTo(KeySelector<T2, KEY> keySelector, TypeInformation<KEY> keyType) {
            requireNonNull(keySelector);
            requireNonNull(keyType);

            if (!keyType.equals(this.keyType)) {
                throw new IllegalArgumentException(
                        "The keys for the two inputs are not equal: "
                                + "first key = "
                                + this.keyType
                                + " , second key = "
                                + keyType);
            }

            return new EqualTo(input2.clean(keySelector));
        }

        // --------------------------------------------------------------------

        /** A join operation that has {@link KeySelector KeySelectors} defined for both inputs. */
        @Public
        public class EqualTo {

            private final KeySelector<T2, KEY> keySelector2;

            EqualTo(KeySelector<T2, KEY> keySelector2) {
                this.keySelector2 = requireNonNull(keySelector2);
            }

            /** Specifies the window on which the join operation works. */
            @PublicEvolving
            public <W extends Window> WithWindow<T1, T2, KEY, W> window(
                    WindowAssigner<? super TaggedUnion<T1, T2>, W> assigner) {
                return new WithWindow<>(
                        input1,
                        input2,
                        keySelector1,
                        keySelector2,
                        keyType,
                        assigner,
                        null,
                        null,
                        null);
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A join operation that has {@link KeySelector KeySelectors} defined for both inputs as well as
     * a {@link WindowAssigner}.
     *
     * @param <T1> Type of the elements from the first input
     * @param <T2> Type of the elements from the second input
     * @param <KEY> Type of the key. This must be the same for both inputs
     * @param <W> Type of {@link Window} on which the join operation works.
     */
    @Public
    public static class WithWindow<T1, T2, KEY, W extends Window> {

        private final DataStream<T1> input1;
        private final DataStream<T2> input2;

        private final KeySelector<T1, KEY> keySelector1;
        private final KeySelector<T2, KEY> keySelector2;
        private final TypeInformation<KEY> keyType;

        private final WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner;

        private final Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger;

        private final Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor;

        private final Time allowedLateness;

        private CoGroupedStreams.WithWindow<T1, T2, KEY, W> coGroupedWindowedStream;

        @PublicEvolving
        protected WithWindow(
                DataStream<T1> input1,
                DataStream<T2> input2,
                KeySelector<T1, KEY> keySelector1,
                KeySelector<T2, KEY> keySelector2,
                TypeInformation<KEY> keyType,
                WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner,
                Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger,
                Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor,
                Time allowedLateness) {

            this.input1 = requireNonNull(input1);
            this.input2 = requireNonNull(input2);

            this.keySelector1 = requireNonNull(keySelector1);
            this.keySelector2 = requireNonNull(keySelector2);
            this.keyType = requireNonNull(keyType);

            this.windowAssigner = requireNonNull(windowAssigner);

            this.trigger = trigger;
            this.evictor = evictor;

            this.allowedLateness = allowedLateness;
        }

        /** Sets the {@code Trigger} that should be used to trigger window emission. */
        @PublicEvolving
        public WithWindow<T1, T2, KEY, W> trigger(
                Trigger<? super TaggedUnion<T1, T2>, ? super W> newTrigger) {
            return new WithWindow<>(
                    input1,
                    input2,
                    keySelector1,
                    keySelector2,
                    keyType,
                    windowAssigner,
                    newTrigger,
                    evictor,
                    allowedLateness);
        }

        /**
         * Sets the {@code Evictor} that should be used to evict elements from a window before
         * emission.
         *
         * <p>Note: When using an evictor window performance will degrade significantly, since
         * pre-aggregation of window results cannot be used.
         */
        @PublicEvolving
        public WithWindow<T1, T2, KEY, W> evictor(
                Evictor<? super TaggedUnion<T1, T2>, ? super W> newEvictor) {
            return new WithWindow<>(
                    input1,
                    input2,
                    keySelector1,
                    keySelector2,
                    keyType,
                    windowAssigner,
                    trigger,
                    newEvictor,
                    allowedLateness);
        }

        /**
         * Sets the time by which elements are allowed to be late.
         *
         * @see WindowedStream#allowedLateness(Time)
         */
        @PublicEvolving
        public WithWindow<T1, T2, KEY, W> allowedLateness(Time newLateness) {
            return new WithWindow<>(
                    input1,
                    input2,
                    keySelector1,
                    keySelector2,
                    keyType,
                    windowAssigner,
                    trigger,
                    evictor,
                    newLateness);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p>Note: This method's return type does not support setting an operator-specific
         * parallelism. Due to binary backwards compatibility, this cannot be altered. Use the
         * {@link #with(JoinFunction)} method to set an operator-specific parallelism.
         */
        public <T> DataStream<T> apply(JoinFunction<T1, T2, T> function) {
            TypeInformation<T> resultType =
                    TypeExtractor.getBinaryOperatorReturnType(
                            function,
                            JoinFunction.class,
                            0,
                            1,
                            2,
                            TypeExtractor.NO_INDEX,
                            input1.getType(),
                            input2.getType(),
                            "Join",
                            false);

            return apply(function, resultType);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p><b>Note:</b> This is a temporary workaround while the {@link #apply(JoinFunction)}
         * method has the wrong return type and hence does not allow one to set an operator-specific
         * parallelism
         *
         * @deprecated This method will be removed once the {@link #apply(JoinFunction)} method is
         *     fixed in the next major version of Flink (2.0).
         */
        @PublicEvolving
        @Deprecated
        public <T> SingleOutputStreamOperator<T> with(JoinFunction<T1, T2, T> function) {
            return (SingleOutputStreamOperator<T>) apply(function);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p>Note: This method's return type does not support setting an operator-specific
         * parallelism. Due to binary backwards compatibility, this cannot be altered. Use the
         * {@link #with(JoinFunction, TypeInformation)}, method to set an operator-specific
         * parallelism.
         */
        public <T> DataStream<T> apply(
                FlatJoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
            // clean the closure
            function = input1.getExecutionEnvironment().clean(function);

            coGroupedWindowedStream =
                    input1.coGroup(input2)
                            .where(keySelector1)
                            .equalTo(keySelector2)
                            .window(windowAssigner)
                            .trigger(trigger)
                            .evictor(evictor)
                            .allowedLateness(allowedLateness);

            return coGroupedWindowedStream.apply(
                    new FlatJoinCoGroupFunction<>(function), resultType);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p><b>Note:</b> This is a temporary workaround while the {@link #apply(JoinFunction,
         * TypeInformation)} method has the wrong return type and hence does not allow one to set an
         * operator-specific parallelism
         *
         * @deprecated This method will be replaced by {@link #apply(FlatJoinFunction,
         *     TypeInformation)} in Flink 2.0. So use the {@link #apply(FlatJoinFunction,
         *     TypeInformation)} in the future.
         */
        @PublicEvolving
        @Deprecated
        public <T> SingleOutputStreamOperator<T> with(
                FlatJoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
            return (SingleOutputStreamOperator<T>) apply(function, resultType);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p>Note: This method's return type does not support setting an operator-specific
         * parallelism. Due to binary backwards compatibility, this cannot be altered. Use the
         * {@link #with(FlatJoinFunction)}, method to set an operator-specific parallelism.
         */
        public <T> DataStream<T> apply(FlatJoinFunction<T1, T2, T> function) {
            TypeInformation<T> resultType =
                    TypeExtractor.getBinaryOperatorReturnType(
                            function,
                            FlatJoinFunction.class,
                            0,
                            1,
                            2,
                            new int[] {2, 0},
                            input1.getType(),
                            input2.getType(),
                            "Join",
                            false);

            return apply(function, resultType);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p><b>Note:</b> This is a temporary workaround while the {@link #apply(FlatJoinFunction)}
         * method has the wrong return type and hence does not allow one to set an operator-specific
         * parallelism.
         *
         * @deprecated This method will be removed once the {@link #apply(FlatJoinFunction)} method
         *     is fixed in the next major version of Flink (2.0).
         */
        @PublicEvolving
        @Deprecated
        public <T> SingleOutputStreamOperator<T> with(FlatJoinFunction<T1, T2, T> function) {
            return (SingleOutputStreamOperator<T>) apply(function);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p>Note: This method's return type does not support setting an operator-specific
         * parallelism. Due to binary backwards compatibility, this cannot be altered. Use the
         * {@link #with(JoinFunction, TypeInformation)}, method to set an operator-specific
         * parallelism.
         */
        public <T> DataStream<T> apply(
                JoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
            // clean the closure
            function = input1.getExecutionEnvironment().clean(function);

            coGroupedWindowedStream =
                    input1.coGroup(input2)
                            .where(keySelector1)
                            .equalTo(keySelector2)
                            .window(windowAssigner)
                            .trigger(trigger)
                            .evictor(evictor)
                            .allowedLateness(allowedLateness);

            return coGroupedWindowedStream.apply(new JoinCoGroupFunction<>(function), resultType);
        }

        /**
         * Completes the join operation with the user function that is executed for each combination
         * of elements with the same key in a window.
         *
         * <p><b>Note:</b> This is a temporary workaround while the {@link #apply(FlatJoinFunction,
         * TypeInformation)} method has the wrong return type and hence does not allow one to set an
         * operator-specific parallelism
         *
         * @deprecated This method will be removed once the {@link #apply(JoinFunction,
         *     TypeInformation)} method is fixed in the next major version of Flink (2.0).
         */
        @PublicEvolving
        @Deprecated
        public <T> SingleOutputStreamOperator<T> with(
                JoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
            return (SingleOutputStreamOperator<T>) apply(function, resultType);
        }

        @VisibleForTesting
        Time getAllowedLateness() {
            return allowedLateness;
        }

        @VisibleForTesting
        CoGroupedStreams.WithWindow<T1, T2, KEY, W> getCoGroupedWindowedStream() {
            return coGroupedWindowedStream;
        }
    }

    // ------------------------------------------------------------------------
    //  Implementation of the functions
    // ------------------------------------------------------------------------

    /** CoGroup function that does a nested-loop join to get the join result. */
    private static class JoinCoGroupFunction<T1, T2, T>
            extends WrappingFunction<JoinFunction<T1, T2, T>>
            implements CoGroupFunction<T1, T2, T> {
        private static final long serialVersionUID = 1L;

        public JoinCoGroupFunction(JoinFunction<T1, T2, T> wrappedFunction) {
            super(wrappedFunction);
        }

        @Override
        public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out)
                throws Exception {
            for (T1 val1 : first) {
                for (T2 val2 : second) {
                    out.collect(wrappedFunction.join(val1, val2));
                }
            }
        }
    }

    /** CoGroup function that does a nested-loop join to get the join result. (FlatJoin version) */
    private static class FlatJoinCoGroupFunction<T1, T2, T>
            extends WrappingFunction<FlatJoinFunction<T1, T2, T>>
            implements CoGroupFunction<T1, T2, T> {
        private static final long serialVersionUID = 1L;

        public FlatJoinCoGroupFunction(FlatJoinFunction<T1, T2, T> wrappedFunction) {
            super(wrappedFunction);
        }

        @Override
        public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out)
                throws Exception {
            for (T1 val1 : first) {
                for (T2 val2 : second) {
                    wrappedFunction.join(val1, val2, out);
                }
            }
        }
    }
}
