/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;

/**
 * Interface for components that want to listen to updates to the status of a slot.
 *
 * <p>This interface must only be used for updating data-structures, NOT for initiating new resource allocations. The
 * event that caused the state transition may also have triggered a series of transitions, which new allocations would
 * interfere with.
 */
interface SlotStatusUpdateListener {

	/**
	 * Notification for the status of a slot having changed.
	 *
	 * <p>If the slot is being freed ({@code current == FREE} then {@code jobId} is that of the job the slot was
	 * allocated for. If the slot was already acquired by a job ({@code current != FREE}, then {@code jobId} is the ID of
	 * this very job.
	 *
	 * @param slot slot whose status has changed
	 * @param previous state before the change
	 * @param current state after the change
	 * @param jobId job for which the slot was/is allocated for
	 */
	void notifySlotStatusChange(TaskManagerSlotInformation slot, SlotState previous, SlotState current, JobID jobId);
}
