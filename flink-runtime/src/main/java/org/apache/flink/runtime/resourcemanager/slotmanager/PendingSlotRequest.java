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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

public class PendingSlotRequest {

	private final SlotRequest slotRequest;

	@Nullable
	private CompletableFuture<Acknowledge> requestFuture;

	@Nullable
	private UUID timeoutIdentifier;

	@Nullable
	private ScheduledFuture<?> timeoutFuture;

	public PendingSlotRequest(SlotRequest slotRequest) {
		this.slotRequest = Preconditions.checkNotNull(slotRequest);
	}

	// ------------------------------------------------------------------------

	public AllocationID getAllocationId() {
		return slotRequest.getAllocationId();
	}

	public ResourceProfile getResourceProfile() {
		return slotRequest.getResourceProfile();
	}

	@Nullable
	public UUID getTimeoutIdentifier() {
		return timeoutIdentifier;
	}

	public JobID getJobId() {
		return slotRequest.getJobId();
	}

	public String getTargetAddress() {
		return slotRequest.getTargetAddress();
	}

	public boolean isAssigned() {
		return null != requestFuture;
	}

	public void setRequestFuture(@Nullable CompletableFuture<Acknowledge> requestFuture) {
		this.requestFuture = requestFuture;
	}

	@Nullable
	public CompletableFuture<Acknowledge> getRequestFuture() {
		return requestFuture;
	}

	public void cancelTimeout() {
		if (timeoutFuture != null) {
			timeoutFuture.cancel(true);

			timeoutIdentifier = null;
			timeoutFuture = null;
		}
	}

	public void registerTimeout(ScheduledFuture<?> newTimeoutFuture, UUID newTimeoutIdentifier) {
		cancelTimeout();

		timeoutFuture = newTimeoutFuture;
		timeoutIdentifier = newTimeoutIdentifier;
	}
}
