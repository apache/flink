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

package org.apache.flink.runtime.asyncprocessing.streaming.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.runtime.asyncprocessing.functions.AbstractAsyncStatefulRichFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * A keyed function that processes elements of a stream. This is the async state version.
 *
 * <p>For every element in the input stream the process {@link #declareProcess} declared is invoked.
 * This can produce zero or more elements as output. Implementations can also query the time and set
 * timers through the provided {@link Context}. For firing timers the process {@link
 * #declareOnTimer} declared will be invoked. This can again produce zero or more elements as output
 * and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code AsyncKeyedProcessFunction} is applied on a {@code KeyedStream}.
 *
 * @param <K> Type of the key.
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@Internal
public abstract class AsyncKeyedProcessFunction<K, I, O> extends AbstractAsyncStatefulRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * Process one element from the input stream.
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
    public abstract ThrowingConsumer<I, Exception> declareProcess(
            DeclarationContext context, Context ctx, Collector<O> out) throws DeclarationException;

    /**
     * Called when a timer set using {@link TimerService} fires.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
     *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
     *     registering timers and querying the time. The context is only valid during the invocation
     *     of this method, do not store it.
     * @param out The processor for processing timestamps.
     */
    public ThrowingConsumer<Long, Exception> declareOnTimer(
            DeclarationContext context, OnTimerContext ctx, Collector<O> out)
            throws DeclarationException {
        return (t) -> {};
    }

    /**
     * Information available in an invocation of {@link #declareProcess} or {@link #declareOnTimer}.
     */
    @Internal
    public abstract class Context {

        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, depending on the stream's watermark strategy.
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

        /** Get key of the element being processed. */
        public abstract K getCurrentKey();
    }

    /** Information available in an invocation of processor defined by {@link #declareOnTimer}. */
    @Internal
    public abstract class OnTimerContext extends Context {
        /** The {@link TimeDomain} of the firing timer. */
        public abstract TimeDomain timeDomain();

        /** Get key of the firing timer. */
        @Override
        public abstract K getCurrentKey();
    }
}
