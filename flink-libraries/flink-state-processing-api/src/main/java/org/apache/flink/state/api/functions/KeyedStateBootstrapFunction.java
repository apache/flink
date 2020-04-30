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

package org.apache.flink.state.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;

/**
 * A function that writes keyed state to a new operator.
 *
 * <p>For every element {@link #processElement(Object, Context)} is invoked. This can write data to
 * state and set timers.
 *
 * <p><b>NOTE:</b> A {@code KeyedStateBootstrapFunction} is always a {@link
 * org.apache.flink.api.common.functions.RichFunction}. Therefore, access to the {@link
 * org.apache.flink.api.common.functions.RuntimeContext} is always available and setup and teardown
 * methods can be implemented. See {@link
 * org.apache.flink.api.common.functions.RichFunction#open(Configuration)})} and {@link
 * org.apache.flink.api.common.functions.RichFunction#close()}.
 *
 * @param <K> Type of the keys.
 * @param <IN> Type of the input.
 */
@PublicEvolving
public abstract class KeyedStateBootstrapFunction<K, IN> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * Process one element from the input stream.
	 *
	 * <p>This function can update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param value The input value.
	 * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
	 *     {@link TimerService} for registering timers and querying the time. The context is only
	 *     valid during the invocation of this method, do not store it.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the
	 *     operation to fail and may trigger recovery.
	 */
	public abstract void processElement(IN value, Context ctx) throws Exception;

	/** Information available in an invocation of {@link #processElement(Object, Context)}. */
	public abstract class Context {

		/** A {@link TimerService} for querying time and registering timers. */
		public abstract TimerService timerService();

		/** Get key of the element being processed. */
		public abstract K getCurrentKey();
	}
}

