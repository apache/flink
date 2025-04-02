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

package org.apache.flink.runtime.asyncprocessing.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * A function that processes elements of two keyed streams and produces a single output stream.
 *
 * <p>The function will be called for every element in the input streams and can produce zero or
 * more output elements. Contrary to the {@link CoFlatMapFunction}, this function can also query the
 * time (both event and processing) and set timers, through the provided {@link Context}. When
 * reacting to the firing of timers the function can emit yet more elements.
 *
 * <p>An example use case for connected streams is the application of a set of rules that change
 * over time ({@code stream A}) to the elements contained in another stream (stream {@code B}). The
 * rules contained in {@code stream A} can be stored in the state and wait for new elements to
 * arrive on {@code stream B}. Upon reception of a new element on {@code stream B}, the function can
 * apply the previously stored rules to the element and emit a result, and/or register a timer that
 * will trigger an action in the future.
 *
 * @param <K> Type of the key.
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
@Internal
public abstract class DeclaringAsyncKeyedCoProcessFunction<K, IN1, IN2, OUT>
        extends KeyedCoProcessFunction<K, IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;

    /** Override and finalize this method. Please use {@link #declareProcess1} instead. */
    @Override
    public final void processElement1(IN1 value, Context ctx, Collector<OUT> out) throws Exception {
        throw new IllegalAccessException("This method is replaced by declareProcess1.");
    }

    /** Override and finalize this method. Please use {@link #declareProcess2} instead. */
    @Override
    public final void processElement2(IN2 value, Context ctx, Collector<OUT> out) throws Exception {
        throw new IllegalAccessException("This method is replaced by declareProcess2.");
    }

    /** Override and finalize this method. Please use {@link #declareOnTimer} instead. */
    public final void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out)
            throws Exception {
        throw new IllegalAccessException("This method is replaced by declareOnTimer.");
    }

    /**
     * Declaring variables before {@link #declareProcess1} and {@link #declareProcess2} and {@link
     * #declareOnTimer}.
     */
    public void declareVariables(DeclarationContext context) {}

    /**
     * Declare a process for one element from the first of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
     *     {@link TimerService} for registering timers and querying the time. The context is only
     *     valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @return the whole processing logic just like {@code processElement}.
     */
    public abstract ThrowingConsumer<IN1, Exception> declareProcess1(
            DeclarationContext context, Context ctx, Collector<OUT> out)
            throws DeclarationException;

    /**
     * Declare a process for one element from the second of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
     *     {@link TimerService} for registering timers and querying the time. The context is only
     *     valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @return the whole processing logic just like {@code processElement}.
     */
    public abstract ThrowingConsumer<IN2, Exception> declareProcess2(
            DeclarationContext context, Context ctx, Collector<OUT> out)
            throws DeclarationException;

    /**
     * Declare a procedure which is called when a timer set using {@link TimerService} fires.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
     *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
     *     registering timers and querying the time. The context is only valid during the invocation
     *     of this method, do not store it.
     * @param out The processor for processing timestamps.
     */
    public ThrowingConsumer<Long, Exception> declareOnTimer(
            DeclarationContext context, OnTimerContext ctx, Collector<OUT> out)
            throws DeclarationException {
        return (t) -> {};
    }
}
