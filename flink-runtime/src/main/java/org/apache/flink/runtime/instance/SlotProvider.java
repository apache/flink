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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;

/**
 * The slot provider is responsible for preparing slots for ready-to-run tasks.
 * 
 * <p>It supports two allocating modes:
 * <ul>
 *     <li>Immediate allocating: A request for a task slot immediately gets satisfied, we can call
 *         {@link Future#getNow(Object)} to get the allocated slot.</li>
 *     <li>Queued allocating: A request for a task slot is queued and returns a future that will be
 *         fulfilled as soon as a slot becomes available.</li>
 * </ul>
 */
public interface SlotProvider {

	/**
	 * Allocating slot with specific requirement.
	 *
	 * @param task         The task to allocate the slot for
	 * @param allowQueued  Whether allow the task be queued if we do not have enough resource
	 * @return The future of the allocation
	 * 
	 * @throws NoResourceAvailableException
	 */
	Future<SimpleSlot> allocateSlot(ScheduledUnit task, boolean allowQueued) throws NoResourceAvailableException;
}
