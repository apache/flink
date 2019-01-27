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

package org.apache.flink.runtime.jobmaster.message;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * The response of pending slot-requests request to SlotPool.
 */
public class PendingSlotRequest implements Serializable {

	private static final long serialVersionUID = 5448945800436094025L;

	private final SlotRequestId slotRequestId;

	private final ResourceProfile resourceProfile;

	private final long startTimestamp;

	private final List<PendingScheduledUnit> pendingScheduledUnits;

	public PendingSlotRequest(
			final SlotRequestId slotRequestId,
			final ResourceProfile resourceProfile,
			final long startTimestamp,
			final List<PendingScheduledUnit> pendingScheduledUnits) {
		this.slotRequestId = slotRequestId;
		this.pendingScheduledUnits = pendingScheduledUnits;
		this.resourceProfile = resourceProfile;
		this.startTimestamp = startTimestamp;
	}

	public SlotRequestId getSlotRequestId() {
		return slotRequestId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public List<PendingScheduledUnit> getPendingScheduledUnits() {
		return pendingScheduledUnits;
	}

	/**
	 * Represents the shadow of a {@link ScheduledUnit} that's slot request is
	 * in pending state.
	 */
	public static class PendingScheduledUnit implements Serializable {

		private static final long serialVersionUID = 8348131373293816273L;

		private final JobVertexID jobVertexId;

		@Nullable
		private final SlotSharingGroupId slotSharingGroupId;

		@Nullable
		private final AbstractID coLocationGroupId;

		private final String taskName;

		private final int subTaskIndex;

		private final int subTaskAttempt;

		public PendingScheduledUnit(
				JobVertexID jobVertexId,
				@Nullable SlotSharingGroupId slotSharingGroupId,
				@Nullable AbstractID coLocationGroupId,
				String taskName,
				int subTaskIndex,
				int subTaskAttempt) {

			this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
			this.slotSharingGroupId = slotSharingGroupId;
			this.coLocationGroupId = coLocationGroupId;
			this.taskName = taskName;
			this.subTaskIndex = subTaskIndex;
			this.subTaskAttempt = subTaskAttempt;
		}

		public static PendingScheduledUnit of(ScheduledUnit scheduledUnit) {
			CoLocationConstraint coLocationConstraint = scheduledUnit.getCoLocationConstraint();
			Execution subTask = scheduledUnit.getTaskToExecute();

			return new PendingScheduledUnit(
					scheduledUnit.getJobVertexId(),
					scheduledUnit.getSlotSharingGroupId(),
					(coLocationConstraint != null) ? coLocationConstraint.getGroup().getId() : null,
					subTask.getVertex().getTaskName(),
					subTask.getParallelSubtaskIndex(),
					subTask.getAttemptNumber());
		}

		// --------------------------------------------------------------------------------------------

		public JobVertexID getJobVertexId() {
			return jobVertexId;
		}

		@Nullable
		public SlotSharingGroupId getSlotSharingGroupId() {
			return slotSharingGroupId;
		}

		@Nullable
		public AbstractID getCoLocationGroupId() {
			return coLocationGroupId;
		}

		public int getSubTaskIndex() {
			return subTaskIndex;
		}

		public int getSubTaskAttempt() {
			return subTaskAttempt;
		}

		public String getTaskName() {
			return taskName;
		}

		// --------------------------------------------------------------------------------------------

		@Override
		public String toString() {
			return "{task={subTaskIndex=" + subTaskIndex + ", subTaskAttempt=" + subTaskAttempt + "}, sharingUnit=" +
					slotSharingGroupId + ", coLocationGroupId=" + coLocationGroupId + '}';
		}
	}
}
