/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.state.api.output.operators.StateBootstrapWrapperOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorBuilder;

import java.util.OptionalInt;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link WindowedStateTransformation} represents a {@link OneInputStateTransformation} for
 * bootstrapping window state.
 *
 * @param <K> The type of the key in the window.
 * @param <T> The type of the elements in the window.
 * @param <W> The type of the window.
 */
@PublicEvolving
public class WindowedStateTransformation<T, K, W extends Window> {

    private final DataStream<T> input;

    private final WindowOperatorBuilder<T, K, W> builder;

    private final OptionalInt operatorMaxParallelism;

    private final KeySelector<T, K> keySelector;

    private final TypeInformation<K> keyType;

    WindowedStateTransformation(
            DataStream<T> input,
            OptionalInt operatorMaxParallelism,
            KeySelector<T, K> keySelector,
            TypeInformation<K> keyType,
            WindowAssigner<? super T, W> windowAssigner) {
        this.input = input;
        this.operatorMaxParallelism = operatorMaxParallelism;
        this.keySelector = keySelector;
        this.keyType = keyType;

        this.builder =
                new WindowOperatorBuilder<>(
                        windowAssigner,
                        windowAssigner.getDefaultTrigger(),
                        input.getExecutionEnvironment().getConfig(),
                        input.getType(),
                        keySelector,
                        keyType);
    }

    /** Sets the {@code Trigger} that should be used to trigger window emission. */
    @PublicEvolving
    public WindowedStateTransformation<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
        builder.trigger(trigger);
        return this;
    }

    /**
     * Sets the {@code Evictor} that should be used to evict elements from a window before emission.
     *
     * <p>Note: When using an evictor window performance will degrade significantly, since
     * incremental aggregation of window results cannot be used.
     */
    @PublicEvolving
    public WindowedStateTransformation<T, K, W> evictor(Evictor<? super T, ? super W> evictor) {
        builder.evictor(evictor);
        return this;
    }

    // ------------------------------------------------------------------------
    //  Operations on the keyed windows
    // ------------------------------------------------------------------------

    /**
     * Applies a reduce function to the window. The window function is called for each evaluation of
     * the window for each key individually. The output of the reduce function is interpreted as a
     * regular non-windowed stream.
     *
     * <p>This window will try and incrementally aggregate data as much as the window policies
     * permit. For example, tumbling time windows can aggregate the data, meaning that only one
     * element per key is stored. Sliding time windows will aggregate on the granularity of the
     * slide interval, so a few elements are stored per key (one per slide interval). Custom windows
     * may not be able to incrementally aggregate, or may need to store extra values in an
     * aggregation tree.
     *
     * @param function The reduce function.
     * @return The data stream that is the result of applying the reduce function to the window.
     */
    @SuppressWarnings("unchecked")
    public StateBootstrapTransformation<T> reduce(ReduceFunction<T> function) {
        if (function instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "ReduceFunction of reduce can not be a RichFunction. "
                            + "Please use reduce(ReduceFunction, WindowFunction) instead.");
        }

        // clean the closure
        function = input.getExecutionEnvironment().clean(function);
        return reduce(function, new PassThroughWindowFunction<>());
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Arriving data is incrementally aggregated using the given reducer.
     *
     * @param reduceFunction The reduce function that is used for incremental aggregation.
     * @param function The window function.
     * @return The data stream that is the result of applying the window function to the window.
     */
    public <R> StateBootstrapTransformation<T> reduce(
            ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) {

        // clean the closures
        function = input.getExecutionEnvironment().clean(function);
        reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

        WindowOperator<K, T, ?, R, W> operator = builder.reduce(reduceFunction, function);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Arriving data is incrementally aggregated using the given reducer.
     *
     * @param reduceFunction The reduce function that is used for incremental aggregation.
     * @param function The window function.
     * @return The data stream that is the result of applying the window function to the window.
     */
    @Internal
    public <R> StateBootstrapTransformation<T> reduce(
            ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function) {
        // clean the closures
        function = input.getExecutionEnvironment().clean(function);
        reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

        WindowOperator<K, T, ?, R, W> operator = builder.reduce(reduceFunction, function);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }

    // ------------------------------------------------------------------------
    //  Aggregation Function
    // ------------------------------------------------------------------------

    /**
     * Applies the given aggregation function to each window. The aggregation function is called for
     * each element, aggregating values incrementally and keeping the state to one accumulator per
     * key and window.
     *
     * @param function The aggregation function.
     * @return The data stream that is the result of applying the fold function to the window.
     * @param <ACC> The type of the AggregateFunction's accumulator
     * @param <R> The type of the elements in the resulting stream, equal to the AggregateFunction's
     *     result type
     */
    @PublicEvolving
    public <ACC, R> StateBootstrapTransformation<T> aggregate(
            AggregateFunction<T, ACC, R> function) {
        checkNotNull(function, "function");

        if (function instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregation function cannot be a RichFunction.");
        }

        TypeInformation<ACC> accumulatorType =
                TypeExtractor.getAggregateFunctionAccumulatorType(
                        function, input.getType(), null, false);

        return aggregate(function, accumulatorType);
    }

    /**
     * Applies the given aggregation function to each window. The aggregation function is called for
     * each element, aggregating values incrementally and keeping the state to one accumulator per
     * key and window.
     *
     * @param function The aggregation function.
     * @return The data stream that is the result of applying the aggregation function to the
     *     window.
     * @param <ACC> The type of the AggregateFunction's accumulator
     * @param <R> The type of the elements in the resulting stream, equal to the AggregateFunction's
     *     result type
     */
    @PublicEvolving
    public <ACC, R> StateBootstrapTransformation<T> aggregate(
            AggregateFunction<T, ACC, R> function, TypeInformation<ACC> accumulatorType) {

        checkNotNull(function, "function");
        checkNotNull(accumulatorType, "accumulatorType");

        if (function instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregation function cannot be a RichFunction.");
        }

        return aggregate(function, new PassThroughWindowFunction<>(), accumulatorType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
     * that the window function typically has only a single value to process when called.
     *
     * @param aggFunction The aggregate function that is used for incremental aggregation.
     * @param windowFunction The window function.
     * @return The data stream that is the result of applying the window function to the window.
     * @param <ACC> The type of the AggregateFunction's accumulator
     * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
     * @param <R> The type of the elements in the resulting stream, equal to the WindowFunction's
     *     result type
     */
    @PublicEvolving
    public <ACC, V, R> StateBootstrapTransformation<T> aggregate(
            AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> windowFunction) {

        checkNotNull(aggFunction, "aggFunction");
        checkNotNull(windowFunction, "windowFunction");

        TypeInformation<ACC> accumulatorType =
                TypeExtractor.getAggregateFunctionAccumulatorType(
                        aggFunction, input.getType(), null, false);

        return aggregate(aggFunction, windowFunction, accumulatorType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
     * that the window function typically has only a single value to process when called.
     *
     * @param aggregateFunction The aggregation function that is used for incremental aggregation.
     * @param windowFunction The window function.
     * @param accumulatorType Type information for the internal accumulator type of the aggregation
     *     function
     * @return The data stream that is the result of applying the window function to the window.
     * @param <ACC> The type of the AggregateFunction's accumulator
     * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
     * @param <R> The type of the elements in the resulting stream, equal to the WindowFunction's
     *     result type
     */
    @PublicEvolving
    public <ACC, V, R> StateBootstrapTransformation<T> aggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            WindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType) {

        checkNotNull(aggregateFunction, "aggregateFunction");
        checkNotNull(windowFunction, "windowFunction");
        checkNotNull(accumulatorType, "accumulatorType");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregate function cannot be a RichFunction.");
        }

        // clean the closures
        windowFunction = input.getExecutionEnvironment().clean(windowFunction);
        aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

        WindowOperator<K, T, ?, R, W> operator =
                builder.aggregate(aggregateFunction, windowFunction, accumulatorType);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
     * that the window function typically has only a single value to process when called.
     *
     * @param aggFunction The aggregate function that is used for incremental aggregation.
     * @param windowFunction The window function.
     * @return The data stream that is the result of applying the window function to the window.
     * @param <ACC> The type of the AggregateFunction's accumulator
     * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
     * @param <R> The type of the elements in the resulting stream, equal to the WindowFunction's
     *     result type
     */
    @PublicEvolving
    public <ACC, V, R> StateBootstrapTransformation<T> aggregate(
            AggregateFunction<T, ACC, V> aggFunction,
            ProcessWindowFunction<V, R, K, W> windowFunction) {

        checkNotNull(aggFunction, "aggFunction");
        checkNotNull(windowFunction, "windowFunction");

        TypeInformation<ACC> accumulatorType =
                TypeExtractor.getAggregateFunctionAccumulatorType(
                        aggFunction, input.getType(), null, false);

        return aggregate(aggFunction, windowFunction, accumulatorType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
     * that the window function typically has only a single value to process when called.
     *
     * @param aggregateFunction The aggregation function that is used for incremental aggregation.
     * @param windowFunction The window function.
     * @param accumulatorType Type information for the internal accumulator type of the aggregation
     *     function
     * @return The data stream that is the result of applying the window function to the window.
     * @param <ACC> The type of the AggregateFunction's accumulator
     * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
     * @param <R> The type of the elements in the resulting stream, equal to the WindowFunction's
     *     result type
     */
    @PublicEvolving
    public <ACC, V, R> StateBootstrapTransformation<T> aggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            ProcessWindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType) {

        checkNotNull(aggregateFunction, "aggregateFunction");
        checkNotNull(windowFunction, "windowFunction");
        checkNotNull(accumulatorType, "accumulatorType");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregate function cannot be a RichFunction.");
        }

        // clean the closures
        windowFunction = input.getExecutionEnvironment().clean(windowFunction);
        aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

        WindowOperator<K, T, ?, R, W> operator =
                builder.aggregate(aggregateFunction, windowFunction, accumulatorType);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }

    // ------------------------------------------------------------------------
    //  Window Function (apply)
    // ------------------------------------------------------------------------

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Note that this function requires that all data in the windows is buffered until the window
     * is evaluated, as the function provides no means of incremental aggregation.
     *
     * @param function The window function.
     * @return The data stream that is the result of applying the window function to the window.
     */
    public <R> StateBootstrapTransformation<T> apply(WindowFunction<T, R, K, W> function) {
        WindowOperator<K, T, ?, R, W> operator = builder.apply(function);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Note that this function requires that all data in the windows is buffered until the window
     * is evaluated, as the function provides no means of incremental aggregation.
     *
     * @param function The window function.
     * @param resultType Type information for the result type of the window function
     * @return The data stream that is the result of applying the window function to the window.
     */
    public <R> StateBootstrapTransformation<T> apply(
            WindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
        function = input.getExecutionEnvironment().clean(function);

        WindowOperator<K, T, ?, R, W> operator = builder.apply(function);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }

    /**
     * Applies the given window function to each window. The window function is called for each
     * evaluation of the window for each key individually. The output of the window function is
     * interpreted as a regular non-windowed stream.
     *
     * <p>Note that this function requires that all data in the windows is buffered until the window
     * is evaluated, as the function provides no means of incremental aggregation.
     *
     * @param function The window function.
     * @return The data stream that is the result of applying the window function to the window.
     */
    @PublicEvolving
    public <R> StateBootstrapTransformation<T> process(ProcessWindowFunction<T, R, K, W> function) {
        WindowOperator<K, T, ?, R, W> operator = builder.process(function);

        SavepointWriterOperatorFactory factory =
                (timestamp, path) -> new StateBootstrapWrapperOperator<>(timestamp, path, operator);
        return new StateBootstrapTransformation<>(
                input, operatorMaxParallelism, factory, keySelector, keyType);
    }
}
