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

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test utils for {@link ExecutionSlotAllocator}.
 */
class ExecutionSlotAllocatorTestUtils {

	static List<ExecutionVertexSchedulingRequirements> createSchedulingRequirements(
			final ExecutionVertexID... executionVertexIds) {

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
			new ArrayList<>(executionVertexIds.length);

		for (ExecutionVertexID executionVertexId : executionVertexIds) {
			schedulingRequirements.add(
				new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(executionVertexId)
					.withSlotSharingGroupId(new SlotSharingGroupId())
					.build());
		}
		return schedulingRequirements;
	}

	static SlotExecutionVertexAssignment findSlotAssignmentByExecutionVertexId(
			final ExecutionVertexID executionVertexId,
			final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments.stream()
				.filter(assignment -> assignment.getExecutionVertexId().equals(executionVertexId))
				.findFirst()
				.orElseThrow(
					() ->
						new IllegalArgumentException(String.format(
							"SlotExecutionVertexAssignment with execution vertex id %s not found",
							executionVertexId)));
	}

	private ExecutionSlotAllocatorTestUtils() {
	}
}
