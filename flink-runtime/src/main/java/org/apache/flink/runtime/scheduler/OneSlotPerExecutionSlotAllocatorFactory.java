/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Factory for {@link OneSlotPerExecutionSlotAllocator}.
 */
class OneSlotPerExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {

	private final SlotProvider slotProvider;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final Time allocationTimeout;

	OneSlotPerExecutionSlotAllocatorFactory(
			final SlotProvider slotProvider,
			final boolean slotWillBeOccupiedIndefinitely,
			final Time allocationTimeout) {
		this.slotProvider = checkNotNull(slotProvider);
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.allocationTimeout = checkNotNull(allocationTimeout);
	}

	@Override
	public ExecutionSlotAllocator createInstance(final PreferredLocationsRetriever preferredLocationsRetriever) {
		return new OneSlotPerExecutionSlotAllocator(
			slotProvider,
			preferredLocationsRetriever,
			slotWillBeOccupiedIndefinitely,
			allocationTimeout);
	}
}
