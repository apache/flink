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

package org.apache.flink.runtime.checkpoint;

/**
 * Various reasons why a checkpoint was failure.
 */
public enum CheckpointFailureReason {

	COORDINATOR_SHUTDOWN("Checkpoint coordinator is shut down."),

	PERIODIC_SCHEDULER_SHUTDOWN("Periodic checkpoint scheduler is shut down."),

	ALREADY_QUEUED("Another checkpoint request has already been queued."),

	TOO_MANY_CONCURRENT_CHECKPOINTS("The maximum number of concurrent checkpoints is exceeded"),

	MINIMUM_TIME_BETWEEN_CHECKPOINTS("The minimum time between checkpoints is still pending. " +
			"Checkpoint will be triggered after the minimum time."),

	NOT_ALL_REQUIRED_TASKS_RUNNING("Not all required tasks are currently running."),

	EXCEPTION("An Exception occurred while triggering the checkpoint."),

	EXPIRED("The checkpoint expired before triggering was complete"),

	CHECKPOINT_EXPIRED("Checkpoint expired before completing."),

	CHECKPOINT_SUBSUMED("Checkpoint has been subsumed."),

	CHECKPOINT_DECLINED("Checkpoint was declined (tasks not ready)."),

	CHECKPOINT_COORDINATOR_SHUTDOWN("CheckpointCoordinator shutdown."),

	CHECKPOINT_COORDINATOR_SUSPEND("Checkpoint Coordinator is suspending."),

	JOB_FAILURE("The job has failed."),

	JOB_FAILOVER_REGION("FailoverRegion is restarting."),

	TASK_CHECKPOINT_FAILURE("Task local checkpoint failure."),

	FINALIZE_CHECKPOINT_FAILURE("Failure to finalize checkpoint."),

	TRIGGER_CHECKPOINT_FAILURE("Trigger checkpoint failure.");

	// ------------------------------------------------------------------------

	private final String message;

	CheckpointFailureReason(String message) {
		this.message = message;
	}

	public String message() {
		return message;
	}
}
