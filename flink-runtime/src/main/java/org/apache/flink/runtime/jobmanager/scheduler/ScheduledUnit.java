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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * ScheduledUnit contains the information necessary to allocate a slot for the given
 * {@link JobVertexID}.
 */
public class ScheduledUnit {

	@Nullable
	private final Execution vertexExecution;

	private final JobVertexID jobVertexId;

	@Nullable
	private final SlotSharingGroupId slotSharingGroupId;

	@Nullable
	private final CoLocationConstraint coLocationConstraint;
	
	// --------------------------------------------------------------------------------------------
	
	public ScheduledUnit(Execution task) {
		this(
			Preconditions.checkNotNull(task),
			task.getVertex().getJobvertexId(),
			null,
			null);
	}
	
	public ScheduledUnit(Execution task, @Nullable SlotSharingGroupId slotSharingGroupId) {
		this(
			Preconditions.checkNotNull(task),
			task.getVertex().getJobvertexId(),
			slotSharingGroupId,
			null);
	}
	
	public ScheduledUnit(
			Execution task,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint) {
		this(
			Preconditions.checkNotNull(task),
			task.getVertex().getJobvertexId(),
			slotSharingGroupId,
			coLocationConstraint);
	}

	public ScheduledUnit(
			JobVertexID jobVertexId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint) {
		this(
			null,
			jobVertexId,
			slotSharingGroupId,
			coLocationConstraint);
	}

	public ScheduledUnit(
		@Nullable Execution task,
		JobVertexID jobVertexId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		@Nullable CoLocationConstraint coLocationConstraint) {

		this.vertexExecution = task;
		this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
		this.slotSharingGroupId = slotSharingGroupId;
		this.coLocationConstraint = coLocationConstraint;

	}

	// --------------------------------------------------------------------------------------------
	
	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	@Nullable
	public Execution getTaskToExecute() {
		return vertexExecution;
	}

	@Nullable
	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	@Nullable
	public CoLocationConstraint getCoLocationConstraint() {
		return coLocationConstraint;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "{task=" + vertexExecution.getVertexWithAttempt() + ", sharingUnit=" + slotSharingGroupId +
				", locationConstraint=" + coLocationConstraint + '}';
	}
}
