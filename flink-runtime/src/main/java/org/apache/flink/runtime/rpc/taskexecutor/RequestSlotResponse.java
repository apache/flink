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
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.runtime.rpc.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.AllocationID;

import java.util.UUID;

/**
 * base class  for response from TaskManager to a requestSlot from resourceManager
 */
public abstract class RequestSlotResponse {
	private final AllocationID allocationID;

	private final UUID resourceManagerLeaderId;

	public RequestSlotResponse(AllocationID allocationID, UUID resourceManagerLeaderId) {
		this.allocationID = allocationID;
		this.resourceManagerLeaderId = resourceManagerLeaderId;
	}

	public AllocationID getAllocationID() {
		return allocationID;
	}

	public UUID getResourceManagerLeaderId() {
		return resourceManagerLeaderId;
	}

	@Override
	public String toString() {
		return "allocationID=" + allocationID +
		       ", resourceManagerLeaderId=" + resourceManagerLeaderId;
	}

	/**
	 * ack a slot request.
	 */
	public static final class Success extends RequestSlotResponse {

		public Success(AllocationID allocationID, UUID resourceManagerLeaderId) {
			super(allocationID, resourceManagerLeaderId);
		}

		@Override
		public String toString() {
			return "Success:" + super.toString();
		}
	}

	// ----------------------------------------------------------------------------

	/**
	 * decline a slot request
	 */
	public static final class Decline extends RequestSlotResponse {

		private final String reason;

		public Decline(AllocationID allocationID, UUID resourceManagerLeaderId, String reason) {
			super(allocationID, resourceManagerLeaderId);
			this.reason = reason;
		}

		public String getReason() {
			return reason;
		}

		@Override
		public String toString() {
			return "Decline:" +
			       "reason='" + reason + '\'' + super.toString();
		}
	}
}
