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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Result of an asynchronous operation.
 *
 * @param <V> type of the result value
 */
public class AsynchronousOperationResult<V> implements AsynchronouslyCreatedResource<V> {

	private static final String FIELD_NAME_STATUS = "status";

	public static final String FIELD_NAME_OPERATION = "operation";

	@JsonProperty(FIELD_NAME_STATUS)
	private final QueueStatus queueStatus;

	@JsonProperty(FIELD_NAME_OPERATION)
	@Nullable
	private final V value;

	@JsonCreator
	private AsynchronousOperationResult(
			@JsonProperty(FIELD_NAME_STATUS) QueueStatus queueStatus,
			@JsonProperty(FIELD_NAME_OPERATION) @Nullable V value) {
		this.queueStatus = Preconditions.checkNotNull(queueStatus);
		this.value = value;
	}

	@Override
	public QueueStatus queueStatus() {
		return queueStatus;
	}

	@Nullable
	@Override
	public V resource() {
		return value;
	}

	public static <V> AsynchronousOperationResult<V> inProgress() {
		return new AsynchronousOperationResult<>(QueueStatus.inProgress(), null);
	}

	public static <V> AsynchronousOperationResult<V> completed(V value) {
		return new AsynchronousOperationResult<>(QueueStatus.completed(), value);
}
}
