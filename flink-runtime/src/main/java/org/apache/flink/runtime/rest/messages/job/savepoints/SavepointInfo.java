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

import org.apache.flink.runtime.rest.messages.json.SerializedThrowableDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Represents information about a finished savepoint.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SavepointInfo {

	private static final String FIELD_NAME_REQUEST_ID = "request-id";

	private static final String FIELD_NAME_LOCATION = "location";

	private static final String FIELD_NAME_FAILURE_CAUSE = "failure-cause";

	@JsonProperty(FIELD_NAME_REQUEST_ID)
	private final SavepointTriggerId requestId;

	@JsonProperty(FIELD_NAME_LOCATION)
	@Nullable
	private final String location;

	@JsonProperty(FIELD_NAME_FAILURE_CAUSE)
	@JsonSerialize(using = SerializedThrowableSerializer.class)
	@JsonDeserialize(using = SerializedThrowableDeserializer.class)
	@Nullable
	private final SerializedThrowable failureCause;

	@JsonCreator
	public SavepointInfo(
			@JsonProperty(FIELD_NAME_REQUEST_ID) final SavepointTriggerId requestId,
			@JsonProperty(FIELD_NAME_LOCATION) @Nullable final String location,
			@JsonProperty(FIELD_NAME_FAILURE_CAUSE)
			@JsonDeserialize(using = SerializedThrowableDeserializer.class)
			@Nullable final SerializedThrowable failureCause) {
		checkArgument(
			location != null ^ failureCause != null,
			"Either location or failureCause must be set");

		this.requestId = requireNonNull(requestId);
		this.location = location;
		this.failureCause = failureCause;
	}

	public SavepointTriggerId getRequestId() {
		return requestId;
	}

	@Nullable
	public String getLocation() {
		return location;
	}

	@Nullable
	public SerializedThrowable getFailureCause() {
		return failureCause;
	}

}
