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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The result of triggering a checkpoint. May either be a declined checkpoint
 * trigger attempt, or a pending checkpoint.
 */
public class CheckpointTriggerResult {

	/** If success, the pending checkpoint created after the successfully trigger, otherwise null */
	private final PendingCheckpoint success;

	/** If failure, the reason why the triggering was declined, otherwise null. */
	private final CheckpointDeclineReason failure;

	// ------------------------------------------------------------------------

	/**
	 * Creates a successful checkpoint trigger result.
	 * 
	 * @param success The pending checkpoint created after the successfully trigger.
	 */
	CheckpointTriggerResult(PendingCheckpoint success) {
		this.success = checkNotNull(success);
		this.failure = null;
	}

	/**
	 * Creates a failed checkpoint trigger result. 
	 * 
	 * @param failure The reason why the checkpoint could not be triggered.
	 */
	CheckpointTriggerResult(CheckpointDeclineReason failure) {
		this.success = null;
		this.failure = checkNotNull(failure);
	}

	// ------------------------------------------------------------------------

	public boolean isSuccess() {
		return success != null;
	}

	public boolean isFailure() {
		return failure != null;
	}

	public PendingCheckpoint getPendingCheckpoint() {
		if (success != null) {
			return success;
		} else {
			throw new IllegalStateException("Checkpoint triggering failed");
		}
	}

	public CheckpointDeclineReason getFailureReason() {
		if (failure != null) {
			return failure;
		} else {
			throw new IllegalStateException("Checkpoint triggering was successful");
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "CheckpointTriggerResult(" +
				(isSuccess() ?
						("success: " + success) :
						("failure: " + failure.message())) + ")";
	}
}
