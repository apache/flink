/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem.bucketers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A bucketer is used with a {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}
 * to determine the {@link org.apache.flink.streaming.api.functions.sink.filesystem.Bucket} each incoming element
 * should be put into.
 *
 * <p>The {@code StreamingFileSink} can be writing to many buckets at a time, and it is responsible for managing
 * a set of active buckets. Whenever a new element arrives it will ask the {@code Bucketer} for the bucket the
 * element should fall in. The {@code Bucketer} can, for example, determine buckets based on system time.
 */
@PublicEvolving
public interface Bucketer<T> extends Serializable {

	/**
	 * Returns the identifier of the bucket the provided element should be put into.
	 * @param element The current element being processed.
	 * @param context The {@link SinkFunction.Context context} used by the
	 *                {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink sink}.
	 *
	 * @return A string representing the identifier of the bucket the element should be put into.
	 * This actual path to the bucket will result from the concatenation of the returned string
	 * and the {@code base path} provided during the initialization of the
	 * {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink sink}.
	 */
	String getBucketId(T element, Context context);

	/**
	 * Context that the {@link Bucketer} can use for getting additional data about
	 * an input record.
	 *
	 * <p>The context is only valid for the duration of a {@link Bucketer#getBucketId(Object, Context)} call.
	 * Do not store the context and use afterwards!
	 */
	@PublicEvolving
	interface Context {

		/**
		 * Returns the current processing time.
		 */
		long currentProcessingTime();

		/**
		 * Returns the current event-time watermark.
		 */
		long currentWatermark();

		/**
		 * Returns the timestamp of the current input record or
		 * {@code null} if the element does not have an assigned timestamp.
		 */
		@Nullable
		Long timestamp();
	}
}
