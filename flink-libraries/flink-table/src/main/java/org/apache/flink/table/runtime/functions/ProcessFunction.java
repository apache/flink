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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

/**
 * A function that processes elements of a stream.
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)}
 * is invoked. This can produce zero or more elements as output. Implementations can also
 * query the time and set timers through the provided {@link Context}. For firing timers
 * {@link #onTimer(long, OnTimerContext, Collector)} will be invoked. This can again produce
 * zero or more elements as output and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code ProcessFunction} is applied on a {@code KeyedStream}.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
public abstract class ProcessFunction<I, O> implements Function {

	private static final long serialVersionUID = 1L;

	protected transient ExecutionContext executionContext;

	public void open(ExecutionContext ctx) throws Exception {
		this.executionContext = ctx;
	}

	public void close() throws Exception {}

	public void endInput(Collector<O> out) throws Exception {}

	protected RuntimeContext getRuntimeContext() {
		if (this.executionContext != null) {
			return this.executionContext.getRuntimeContext();
		} else {
			throw new IllegalStateException("The stream exec runtime context has not been initialized.");
		}
	}

	/**
	 * Process one element from the input stream.
	 *
	 * <p>This function can output zero or more elements using the {@link Collector} parameter
	 * and also update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param input The input value.
	 * @param ctx A {@link Context} that allows querying the timestamp of the element and getting
	 *            a {@link TimerService} for registering timers and querying the time. The
	 *            context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	public abstract void processElement(I input, Context ctx, Collector<O> out) throws Exception;

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
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {}

	/**
	 * Information available in an invocation of {@link #processElement(Object, Context, Collector)}
	 * or {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	public abstract static class Context {

		/**
		 * A {@link TimerService} for querying time and registering timers.
		 */
		public abstract TimerService timerService();
	}

	/**
	 * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	public abstract static class OnTimerContext extends Context {
		/**
		 * The {@link TimeDomain} of the firing timer.
		 */
		public abstract TimeDomain timeDomain();
	}

}
