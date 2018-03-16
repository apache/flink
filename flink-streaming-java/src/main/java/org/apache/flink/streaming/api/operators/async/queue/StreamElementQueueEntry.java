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
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Entry class for the {@link StreamElementQueue}. The stream element queue entry stores the
 * {@link StreamElement} for which the stream element queue entry has been instantiated.
 * Furthermore, it allows to register callbacks for when the queue entry is completed.
 *
 * @param <T> Type of the result
 */
@Internal
public abstract class StreamElementQueueEntry<T> implements AsyncResult {

	private final StreamElement streamElement;

	public StreamElementQueueEntry(StreamElement streamElement) {
		this.streamElement = Preconditions.checkNotNull(streamElement);
	}

	public StreamElement getStreamElement() {
		return streamElement;
	}

	/**
	 * True if the stream element queue entry has been completed; otherwise false.
	 *
	 * @return True if the stream element queue entry has been completed; otherwise false.
	 */
	public boolean isDone() {
		return getFuture().isDone();
	}

	/**
	 * Register the given complete function to be called once this queue entry has been completed.
	 *
	 * @param completeFunction to call when the queue entry has been completed
	 * @param executor to run the complete function
	 */
	public void onComplete(
			final Consumer<StreamElementQueueEntry<T>> completeFunction,
			Executor executor) {
		final StreamElementQueueEntry<T> thisReference = this;

		getFuture().whenCompleteAsync(
			// call the complete function for normal completion as well as exceptional completion
			// see FLINK-6435
			(value, throwable) -> completeFunction.accept(thisReference),
			executor);
	}

	protected abstract CompletableFuture<T> getFuture();

	@Override
	public final boolean isWatermark() {
		return AsyncWatermarkResult.class.isAssignableFrom(getClass());
	}

	@Override
	public final boolean isResultCollection() {
		return AsyncCollectionResult.class.isAssignableFrom(getClass());
	}

	@Override
	public final AsyncWatermarkResult asWatermark() {
		return (AsyncWatermarkResult) this;
	}

	@Override
	public final <T> AsyncCollectionResult<T> asResultCollection() {
		return (AsyncCollectionResult<T>) this;
	}
}
