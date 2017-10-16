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

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response to the triggering of a savepoint.
 */
public class SavepointTriggerResponseBody implements ResponseBody {

	private static final String FIELD_NAME_STATUS = "status";

	private static final String FIELD_NAME_LOCATION = "location";

	private static final String FIELD_NAME_REQUEST_ID = "request-id";

	@JsonProperty(FIELD_NAME_STATUS)
	public final String status;

	@JsonProperty(FIELD_NAME_LOCATION)
	public final String location;

	@JsonProperty(FIELD_NAME_REQUEST_ID)
	public final String requestId;

	@JsonCreator
	public SavepointTriggerResponseBody(
			@JsonProperty(FIELD_NAME_STATUS) String status,
			@JsonProperty(FIELD_NAME_LOCATION) String location,
			@JsonProperty(FIELD_NAME_REQUEST_ID) String requestId) {
		this.status = status;
		this.location = Preconditions.checkNotNull(location);
		this.requestId = requestId;
	}

	@Override
	public int hashCode() {
		return 79 * location.hashCode();
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof SavepointTriggerResponseBody) {
			SavepointTriggerResponseBody other = (SavepointTriggerResponseBody) object;
			return this.location.equals(other.location);
		}
		return false;
	}
}
