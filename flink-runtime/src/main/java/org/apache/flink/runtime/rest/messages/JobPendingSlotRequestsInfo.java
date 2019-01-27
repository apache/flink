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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.messages.job.JobPendingSlotRequestDetail;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

/**
 * Class containing a collection of {@link JobPendingSlotRequestDetail}.
 */
public class JobPendingSlotRequestsInfo implements ResponseBody {

	public static final String FIELD_NAME_PENDING_SLOT_REQUESTS = "pending-slot-requests";

	@JsonProperty(FIELD_NAME_PENDING_SLOT_REQUESTS)
	private final Collection<JobPendingSlotRequestDetail> pendingSlotRequestDetails;

	@JsonCreator
	public JobPendingSlotRequestsInfo(
			@JsonProperty(FIELD_NAME_PENDING_SLOT_REQUESTS) Collection<JobPendingSlotRequestDetail> pendingSlotRequestDetails) {
		this.pendingSlotRequestDetails = Preconditions.checkNotNull(pendingSlotRequestDetails);
	}

	@JsonIgnore
	public Collection<JobPendingSlotRequestDetail> getPendingSlotRequestDetails() {
		return pendingSlotRequestDetails;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobPendingSlotRequestsInfo that = (JobPendingSlotRequestsInfo) o;
		return Objects.equals(pendingSlotRequestDetails, that.pendingSlotRequestDetails);
	}

	@Override
	public int hashCode() {
		return Objects.hash(pendingSlotRequestDetails);
	}
}
