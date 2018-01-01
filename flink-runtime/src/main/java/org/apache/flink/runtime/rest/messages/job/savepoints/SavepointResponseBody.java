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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Status of the savepoint and savepoint information if available.
 */
public class SavepointResponseBody implements AsynchronouslyCreatedResource<SavepointInfo> {

	private static final String FIELD_NAME_STATUS = "status";

	private static final String FIELD_NAME_SAVEPOINT = "savepoint";

	@JsonProperty(FIELD_NAME_STATUS)
	private final QueueStatus status;

	@JsonProperty(FIELD_NAME_SAVEPOINT)
	@Nullable
	private final SavepointInfo savepoint;

	@JsonCreator
	public SavepointResponseBody(
			@JsonProperty(FIELD_NAME_STATUS) QueueStatus status,
			@Nullable @JsonProperty(FIELD_NAME_SAVEPOINT) SavepointInfo savepoint) {
		this.status = requireNonNull(status);
		this.savepoint = savepoint;
	}

	public static SavepointResponseBody inProgress() {
		return new SavepointResponseBody(QueueStatus.inProgress(), null);
	}

	public static SavepointResponseBody completed(final SavepointInfo savepoint) {
		requireNonNull(savepoint);
		return new SavepointResponseBody(QueueStatus.inProgress(), savepoint);
	}

	public QueueStatus getStatus() {
		return status;
	}

	@Nullable
	public SavepointInfo getSavepoint() {
		return savepoint;
	}

	//-------------------------------------------------------------------------
	// AsynchronouslyCreatedResource
	//-------------------------------------------------------------------------

	@JsonIgnore
	@Override
	public QueueStatus queueStatus() {
		return status;
	}

	@JsonIgnore
	@Override
	public SavepointInfo resource() {
		return savepoint;
	}
}
