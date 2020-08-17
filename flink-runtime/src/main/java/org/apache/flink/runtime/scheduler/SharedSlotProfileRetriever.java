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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Computes a {@link SlotProfile} to allocate a slot for executions, sharing the slot.
 */
@FunctionalInterface
interface SharedSlotProfileRetriever {
	/**
	 * Computes a {@link SlotProfile} of an execution slot sharing group.
	 *
	 * @param executionSlotSharingGroup executions sharing the slot.
	 * @param physicalSlotResourceProfile {@link ResourceProfile} of the slot.
	 * @return a future of the {@link SlotProfile} to allocate for the {@code executionSlotSharingGroup}.
	 */
	CompletableFuture<SlotProfile> getSlotProfileFuture(
		ExecutionSlotSharingGroup executionSlotSharingGroup,
		ResourceProfile physicalSlotResourceProfile);

	@FunctionalInterface
	interface SharedSlotProfileRetrieverFactory {
		SharedSlotProfileRetriever createFromBulk(Set<ExecutionVertexID> bulk);
	}
}
