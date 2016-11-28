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
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

/**
 * A function that processes elements of a stream.
 *
 * <p>The function will be called for every element in the input stream and can produce
 * zero or more output. The function can also query the time and set timers. When
 * reacting to the firing of set timers the function can emit yet more elements.
 *
 * <p>The function will be called for every element in the input stream and can produce
 * zero or more output elements. Contrary to the
 * {@link org.apache.flink.api.common.functions.FlatMapFunction}, this function can also query
 * the time (both event and processing) and set timers, through the provided {@link Context}.
 * When reacting to the firing of set timers the function can directly emit a result, and/or
 * register a timer that will trigger an action in the future.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
@PublicEvolving
public interface ProcessFunction<I, O> extends Function {

	/**
	 * Process one element from the input stream.
	 *
	 * <p>This function can output zero or more elements using the {@link Collector} parameter
	 * and also update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param value The input value.
	 * @param ctx A {@link Context} that allows querying the timestamp of the element and getting
	 *            a {@link TimerService} for registering timers and querying the time. The
	 *            context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void processElement(I value, Context ctx, Collector<O> out) throws Exception;

	/**
	 * Called when a timer set using {@link TimerService} fires.
	 *
	 * @param timestamp The timestamp of the firing timer.
	 * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
	 *            querying the {@link TimeDomain} of the firing timer and getting a
	 *            {@link TimerService} for registering timers and querying the time.
	 *            The context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception ;

	/**
	 * Information available in an invocation of {@link #processElement(Object, Context, Collector)}
	 * or {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	interface Context {

		/**
		 * Timestamp of the element currently being processed or timestamp of a firing timer.
		 *
		 * <p>This might be {@code null}, for example if the time characteristic of your program
		 * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
		 */
		Long timestamp();

		/**
		 * A {@link TimerService} for querying time and registering timers.
		 */
		TimerService timerService();
	}

	/**
	 * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	interface OnTimerContext extends Context {
		/**
		 * The {@link TimeDomain} of the firing timer.
		 */
		TimeDomain timeDomain();
	}

}
