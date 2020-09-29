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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.Collection;
import java.util.Set;

/**
 * Represents a bulk of physical slot requests.
 */
public interface PhysicalSlotRequestBulk {
	/**
	 * Returns {@link ResourceProfile}s of pending physical slot requests.
	 *
	 * <p>If a request is pending, it is not fulfilled and vice versa.
	 * {@link #getAllocationIdsOfFulfilledRequests()} should not return a pending request.
	 */
	Collection<ResourceProfile> getPendingRequests();

	/**
	 * Returns {@link AllocationID}s of fulfilled physical slot requests.
	 *
	 * <p>If a request is fulfilled, it is not pending and vice versa.
	 * {@link #getPendingRequests()} should not return a fulfilled request.
	 */
	Set<AllocationID> getAllocationIdsOfFulfilledRequests();

	/**
	 * Cancels all requests of this bulk.
	 *
	 * <p>Canceled bulk is not valid and should not be used afterwards.
	 */
	void cancel(Throwable cause);
}
