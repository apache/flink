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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A function that processes elements of a stream.
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)} is
 * invoked. This can produce zero or more elements as output. Implementations can also query the
 * time and set timers through the provided {@link Context}. For firing timers {@link #onTimer(long,
 * OnTimerContext, Collector)} will be invoked. This can again produce zero or more elements as
 * output and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code ProcessFunction} is applied on a {@code KeyedStream}.
 *
 * <p><b>NOTE:</b> A {@code ProcessFunction} is always a {@link
 * org.apache.flink.api.common.functions.RichFunction}. Therefore, access to the {@link
 * org.apache.flink.api.common.functions.RuntimeContext} is always available and setup and teardown
 * methods can be implemented. See {@link
 * org.apache.flink.api.common.functions.RichFunction#open(OpenContext)} and {@link
 * org.apache.flink.api.common.functions.RichFunction#close()}.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@PublicEvolving
public abstract class ProcessFunction<I, O> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
     *     {@link TimerService} for registering timers and querying the time. The context is only
     *     valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public abstract void processElement(I value, Context ctx, Collector<O> out) throws Exception;

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
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {}

    /**
     * Information available in an invocation of {@link #processElement(Object, Context, Collector)}
     * or {@link #onTimer(long, OnTimerContext, Collector)}.
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
