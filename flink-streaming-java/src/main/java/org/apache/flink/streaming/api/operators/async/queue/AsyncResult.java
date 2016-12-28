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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Asynchronous result returned by the {@link StreamElementQueue}. The asynchronous result can
 * either be a {@link Watermark} or a collection of new output elements produced by the
 * {@link AsyncFunction}.
 */
@Internal
public interface AsyncResult {

	/**
	 * True if the async result is a {@link Watermark}; otherwise false.
	 *
	 * @return True if the async result is a {@link Watermark}; otherwise false.
	 */
	boolean isWatermark();

	/**
	 * True fi the async result is a collection of output elements; otherwise false.
	 *
	 * @return True if the async reuslt is a collection of output elements; otherwise false
	 */
	boolean isResultCollection();

	/**
	 * Return this async result as a async watermark result.
	 *
	 * @return this result as a {@link AsyncWatermarkResult}.
	 */
	AsyncWatermarkResult asWatermark();

	/**
	 * Return this async result as a async result collection.
	 *
	 * @param <T> Type of the result collection's elements
	 * @return this result as a {@link AsyncCollectionResult}.
	 */
	<T> AsyncCollectionResult<T> asResultCollection();
}
