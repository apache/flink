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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;

/**
 * A collection of safe drop-in replacements for existing {@link ComponentMetricGroup}s.
 */
public class UnregisteredMetricGroups {

	private UnregisteredMetricGroups() {
	}

	public static ProcessMetricGroup createUnregisteredProcessMetricGroup() {
		return new UnregisteredProcessMetricGroup();
	}

	public static ResourceManagerMetricGroup createUnregisteredResourceManagerMetricGroup() {
		return new UnregisteredResourceManagerMetricGroup();
	}

	public static SlotManagerMetricGroup createUnregisteredSlotManagerMetricGroup() {
		return new UnregisteredSlotManagerMetricGroup();
	}

	public static JobManagerMetricGroup createUnregisteredJobManagerMetricGroup() {
		return new UnregisteredJobManagerMetricGroup();
	}

	public static JobManagerJobMetricGroup createUnregisteredJobManagerJobMetricGroup() {
		return new UnregisteredJobManagerJobMetricGroup();
	}

	public static TaskManagerMetricGroup createUnregisteredTaskManagerMetricGroup() {
		return new UnregisteredTaskManagerMetricGroup();
	}

	public static TaskManagerJobMetricGroup createUnregisteredTaskManagerJobMetricGroup() {
		return new UnregisteredTaskManagerJobMetricGroup();
	}

	public static TaskMetricGroup createUnregisteredTaskMetricGroup() {
		return new UnregisteredTaskMetricGroup();
	}

	public static OperatorMetricGroup createUnregisteredOperatorMetricGroup() {
		return new UnregisteredOperatorMetricGroup();
	}

	/**
	 * A safe drop-in replacement for {@link ProcessMetricGroup ProcessMetricGroups}.
	 */
	public static class UnregisteredProcessMetricGroup extends ProcessMetricGroup {
		private static final String UNREGISTERED_HOST = "UnregisteredHost";

		public UnregisteredProcessMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, UNREGISTERED_HOST);
		}
	}

	/**
	 * A safe drop-in replacement for {@link ResourceManagerMetricGroup ResourceManagerMetricGroups}.
	 */
	public static class UnregisteredResourceManagerMetricGroup extends ResourceManagerMetricGroup {
		private static final String UNREGISTERED_HOST = "UnregisteredHost";

		UnregisteredResourceManagerMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, UNREGISTERED_HOST);
		}
	}

	/**
	 * A safe drop-in replacement for {@link SlotManagerMetricGroup SlotManagerMetricGroups}.
	 */
	public static class UnregisteredSlotManagerMetricGroup extends SlotManagerMetricGroup {
		private static final String UNREGISTERED_HOST = "UnregisteredHost";

		UnregisteredSlotManagerMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, UNREGISTERED_HOST);
		}
	}

	/**
	 * A safe drop-in replacement for {@link JobManagerMetricGroup}s.
	 */
	public static class UnregisteredJobManagerMetricGroup extends JobManagerMetricGroup {
		private static final String DEFAULT_HOST_NAME = "UnregisteredHost";

		private UnregisteredJobManagerMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, DEFAULT_HOST_NAME);
		}

		@Override
		public JobManagerJobMetricGroup addJob(JobGraph job) {
			return createUnregisteredJobManagerJobMetricGroup();
		}
	}

	/**
	 * A safe drop-in replacement for {@link JobManagerJobMetricGroup}s.
	 */
	public static class UnregisteredJobManagerJobMetricGroup extends JobManagerJobMetricGroup {
		private static final JobID DEFAULT_JOB_ID = new JobID(0, 0);
		private static final String DEFAULT_JOB_NAME = "UnregisteredJob";

		protected UnregisteredJobManagerJobMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, new UnregisteredJobManagerMetricGroup(), DEFAULT_JOB_ID, DEFAULT_JOB_NAME);
		}
	}

	/**
	 * A safe drop-in replacement for {@link TaskManagerMetricGroup}s.
	 */
	public static class UnregisteredTaskManagerMetricGroup extends TaskManagerMetricGroup {
		private static final String DEFAULT_HOST_NAME = "UnregisteredHost";
		private static final String DEFAULT_TASKMANAGER_ID = "0";

		protected UnregisteredTaskManagerMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, DEFAULT_HOST_NAME, DEFAULT_TASKMANAGER_ID);
		}

		@Override
		public TaskMetricGroup addTaskForJob(
			final JobID jobId,
			final String jobName,
			final JobVertexID jobVertexId,
			final ExecutionAttemptID executionAttemptId,
			final String taskName,
			final int subtaskIndex,
			final int attemptNumber) {
			return createUnregisteredTaskMetricGroup();
		}
	}

	/**
	 * A safe drop-in replacement for {@link TaskManagerJobMetricGroup}s.
	 */
	public static class UnregisteredTaskManagerJobMetricGroup extends TaskManagerJobMetricGroup {
		private static final JobID DEFAULT_JOB_ID = new JobID(0, 0);
		private static final String DEFAULT_JOB_NAME = "UnregisteredJob";

		public UnregisteredTaskManagerJobMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, new UnregisteredTaskManagerMetricGroup(), DEFAULT_JOB_ID, DEFAULT_JOB_NAME);
		}

		@Override
		public TaskMetricGroup addTask(
			final JobVertexID jobVertexId,
			final ExecutionAttemptID executionAttemptID,
			final String taskName,
			final int subtaskIndex,
			final int attemptNumber) {
			return createUnregisteredTaskMetricGroup();
		}
	}

	/**
	 * A safe drop-in replacement for {@link TaskMetricGroup}s.
	 */
	public static class UnregisteredTaskMetricGroup extends TaskMetricGroup {
		private static final JobVertexID DEFAULT_VERTEX_ID = new JobVertexID(0, 0);
		private static final ExecutionAttemptID DEFAULT_ATTEMPT_ID = new ExecutionAttemptID(0, 0);
		private static final String DEFAULT_TASK_NAME = "UnregisteredTask";

		protected UnregisteredTaskMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, new UnregisteredTaskManagerJobMetricGroup(),
				DEFAULT_VERTEX_ID, DEFAULT_ATTEMPT_ID, DEFAULT_TASK_NAME, 0, 0);
		}

		@Override
		public OperatorMetricGroup getOrAddOperator(OperatorID operatorID, String name) {
			return createUnregisteredOperatorMetricGroup();
		}
	}

	/**
	 * A safe drop-in replacement for {@link OperatorMetricGroup}s.
	 */
	public static class UnregisteredOperatorMetricGroup extends OperatorMetricGroup {
		private static final OperatorID DEFAULT_OPERATOR_ID = new OperatorID(0, 0);
		private static final String DEFAULT_OPERATOR_NAME = "UnregisteredOperator";

		protected UnregisteredOperatorMetricGroup() {
			super(NoOpMetricRegistry.INSTANCE, new UnregisteredTaskMetricGroup(), DEFAULT_OPERATOR_ID, DEFAULT_OPERATOR_NAME);
		}
	}
}
