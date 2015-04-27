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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

/**
 * This message is sent from the {@link org.apache.flink.runtime.jobmanager.JobManager} to the
 * {@link org.apache.flink.runtime.taskmanager.TaskManager} to tell a task that the checkpoint
 * has been confirmed and that the task can commit the checkpoint to the outside world.
 */
public class AbortCheckpoint extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = 2094094662279578953L;

	public AbortCheckpoint(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		super(job, taskExecutionId, checkpointId);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		return this == o || ( (o instanceof AbortCheckpoint) && super.equals(o));
	}

	@Override
	public String toString() {
		return String.format("AbortCheckpoint %d for (%s/%s)", 
				getCheckpointId(), getJob(), getTaskExecutionId());
	}
}
