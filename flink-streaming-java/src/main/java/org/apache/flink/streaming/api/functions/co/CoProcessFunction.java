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

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A function that processes elements of two streams and produces a single output one.
 *
 * <p>The function will be called for every element in the input streams and can produce zero or
 * more output elements. Contrary to the {@link CoFlatMapFunction}, this function can also query the
 * time (both event and processing) and set timers, through the provided {@link Context}. When
 * reacting to the firing of set timers the function can emit yet more elements.
 *
 * <p>An example use-case for connected streams would be the application of a set of rules that
 * change over time ({@code stream A}) to the elements contained in another stream (stream {@code
 * B}). The rules contained in {@code stream A} can be stored in the state and wait for new elements
 * to arrive on {@code stream B}. Upon reception of a new element on {@code stream B}, the function
 * can now apply the previously stored rules to the element and directly emit a result, and/or
 * register a timer that will trigger an action in the future.
 *
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
@PublicEvolving
public abstract class CoProcessFunction<IN1, IN2, OUT> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * This method is called for each element in the first of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *     timers and querying the time. The context is only valid during the invocation of this
     *     method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *     and go into recovery.
     */
    public abstract void processElement1(IN1 value, Context ctx, Collector<OUT> out)
            throws Exception;

    /**
     * This method is called for each element in the second of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *     timers and querying the time. The context is only valid during the invocation of this
     *     method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *     and go into recovery.
     */
    public abstract void processElement2(IN2 value, Context ctx, Collector<OUT> out)
            throws Exception;

    /**
     * Called when a timer set using {@link TimerService} fires.
     *
     * @param timestamp The timestamp of the firing timer.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
     *     querying the {@link TimeDomain} of the firing timer and getting a {@link TimerService}
     *     for registering timers and querying the time. The context is only valid during the
     *     invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {}

    /**
     * Information available in an invocation of {@link #processElement1(Object, Context,
     * Collector)}/ {@link #processElement2(Object, Context, Collector)} or {@link #onTimer(long,
     * OnTimerContext, Collector)}.
     */
    public abstract class Context {

        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        public abstract Long timestamp();

        /** A {@link TimerService} for querying time and registering timers. */
        public abstract TimerService timerService();

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        public abstract <X> void output(OutputTag<X> outputTag, X value);
    }

    /**
     * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    public abstract class OnTimerContext extends Context {
        /** The {@link TimeDomain} of the firing timer. */
        public abstract TimeDomain timeDomain();
    }
}
