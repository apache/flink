/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Interface for implementing user defined sink functionality.
 *
 * @param <IN> Input type parameter.
 */
@Public
public interface SinkFunction<IN> extends Function, Serializable {

	/**
	 * Function for standard sink behaviour. This function is called for every record.
	 *
	 * @param value The input record.
	 * @throws Exception
	 * @deprecated Use {@link #invoke(Object, Context)}.
	 */
	@Deprecated
	default void invoke(IN value) throws Exception {
	}

	/**
	 * Writes the given value to the sink. This function is called for every record.
	 *
	 * @param value The input record.
	 * @param context Additional context about the input record.
	 * @throws Exception
	 */
	default void invoke(IN value, Context context) throws Exception {
		invoke(value);
	}

	/**
	 * Context that {@link SinkFunction SinkFunctions } can use for getting additional data about
	 * an input record.
	 *
	 * <p>The context is only valid for the duration of a
	 * {@link SinkFunction#invoke(Object, Context)} call. Do not store the context and use
	 * afterwards!
	 *
	 * @param <T> The type of elements accepted by the sink.
	 */
	@Public // Interface might be extended in the future with additional methods.
	interface Context<T> {

		/** Returns the current processing time. */
		long currentProcessingTime();

		/** Returns the current event-time watermark. */
		long currentWatermark();

		/**
		 * Returns the timestamp of the current input record.
		 */
		long timestamp();

		/**
		 * Checks whether this record has a timestamp.
		 *
		 * @return True if the record has a timestamp, false if not.
		 */
		boolean hasTimestamp();
	}
}
