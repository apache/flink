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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * The bulk slot provider serves physical slot requests.
 */
public interface BulkSlotProvider {
	/**
	 * Allocates a bulk of physical slots. The allocation will be completed
	 * normally only when all the requests are fulfilled.
	 *
	 * @param physicalSlotRequests requests for physical slots
	 * @param timeout indicating how long it is accepted that the slot requests can be unfulfillable
	 * @return future of the results of slot requests
	 */
	CompletableFuture<Collection<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
		Collection<PhysicalSlotRequest> physicalSlotRequests,
		Time timeout);

	/**
	 * Cancels the slot request with the given {@link SlotRequestId}. If the request is already fulfilled
	 * with a physical slot, the slot will be released.
	 *
	 * @param slotRequestId identifying the slot request to cancel
	 * @param cause of the cancellation
	 */
	void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause);
}
