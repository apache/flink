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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;

/**
 * This class tracks a fulfillability timeout of a bulk of physical slot requests.
 *
 * <p>The check stops when all pending physical slot requests of {@link PhysicalSlotRequestBulk} are fulfilled
 * by available or newly allocated slots. The bulk is fulfillable if all its physical slot requests can be fulfilled
 * either by available or newly allocated slots or slots which currently used by other job subtasks.
 * The bulk gets canceled if the timeout occurs and the bulk is not fulfillable.
 * The timeout timer is not running while the bulk is fulfillable but not fulfilled yet.
 */
public interface PhysicalSlotRequestBulkChecker {
	/**
	 * Starts the bulk checker by initializing the main thread executor.
	 *
	 * @param mainThreadExecutor the main thread executor of the job master
	 */
	void start(ComponentMainThreadExecutor mainThreadExecutor);

	/**
	 * Starts tracking the fulfillability of a {@link PhysicalSlotRequestBulk} with timeout.
	 *
	 * @param bulk {@link PhysicalSlotRequestBulk} to track
	 * @param timeout timeout after which the bulk should be canceled if it is still not fulfillable.
	 */
	void schedulePendingRequestBulkTimeoutCheck(PhysicalSlotRequestBulk bulk, Time timeout);
}
