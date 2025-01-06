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
import org.apache.flink.runtime.asyncprocessing.operators.AsyncKeyedProcessOperator;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * A keyed function that processes elements of a stream. This works with {@link
 * AsyncKeyedProcessOperator} using the async state access and declares the processing logic as
 * FLIP-455 describes. This function extends from the non-declaring {@link KeyedProcessFunction} for
 * the convenience of unifying logic in the operator {@link AsyncKeyedProcessOperator}. Maybe later
 * we could extract the common part {@link KeyedProcessFunction} and this class to a base class, and
 * deviate from that.
 *
 * <p>For every element in the input stream the process {@link #declareProcess} declared is invoked.
 * This can produce zero or more elements as output. Implementations can also query the time and set
 * timers through the provided {@link Context}. For firing timers the process {@link
 * #declareOnTimer} declared will be invoked. This can again produce zero or more elements as output
 * and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code DeclaringAsyncKeyedProcessFunction} is applied on a {@code KeyedStream}.
 *
 * @param <K> Type of the key.
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@Internal
public abstract class DeclaringAsyncKeyedProcessFunction<K, I, O>
        extends KeyedProcessFunction<K, I, O> {

    private static final long serialVersionUID = 1L;

    /** Override and finalize this method. Please use {@link #declareProcess} instead. */
    @Override
    public final void processElement(I value, Context ctx, Collector<O> out) throws Exception {
        throw new IllegalAccessException("This method is replaced by declareProcess.");
    }

    /** Override and finalize this method. Please use {@link #declareOnTimer} instead. */
    @Override
    public final void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out)
            throws Exception {
        throw new IllegalAccessException("This method is replaced by declareOnTimer.");
    }

    /** Declaring variables before {@link #declareProcess} and {@link #declareOnTimer}. */
    public void declareVariables(DeclarationContext context) {}

    /**
     * Declare a process for one element from the input stream.
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
            DeclarationContext context, OnTimerContext ctx, Collector<O> out)
            throws DeclarationException {
        return (t) -> {};
    }
}
