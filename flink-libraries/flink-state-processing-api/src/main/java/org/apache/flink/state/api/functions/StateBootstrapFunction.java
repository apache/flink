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
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * Interface for writing elements to operator state.
 *
 * @param <IN> The type of the input.
 */
@PublicEvolving
public abstract class StateBootstrapFunction<IN> extends AbstractRichFunction implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * Writes the given value to operator state. This function is called for every record.
	 *
	 * @param value The input record.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the
	 *     operation to fail and may trigger recovery.
	 */
	public abstract void processElement(IN value, Context ctx) throws Exception;

	/**
	 * Context that {@link StateBootstrapFunction}'s can use for getting additional data about an input
	 * record.
	 *
	 * <p>The context is only valid for the duration of a {@link
	 * StateBootstrapFunction#processElement(Object, Context)} call. Do not store the context and use
	 * afterwards!
	 */
	public interface Context {

		/** Returns the current processing time. */
		long currentProcessingTime();
	}
}
