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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointType;

/**
 * A synchronization primitive used by the {@link StreamTask} to wait
 * for the completion of a {@link CheckpointType#SYNC_SAVEPOINT}.
 */
class SynchronousSavepointLatch {

	private static final long NOT_SET_CHECKPOINT_ID = -1L;

	// these are mutually exclusive
	private volatile boolean waiting;
	private volatile boolean completed;
	private volatile boolean canceled;

	private volatile boolean wasAlreadyCompleted;

	private final Object synchronizationPoint;

	private volatile long checkpointId;

	SynchronousSavepointLatch() {
		this.synchronizationPoint = new Object();

		this.waiting = false;
		this.completed = false;
		this.canceled = false;
		this.checkpointId = NOT_SET_CHECKPOINT_ID;
	}

	long getCheckpointId() {
		return checkpointId;
	}

	void setCheckpointId(final long checkpointId) {
		if (this.checkpointId == NOT_SET_CHECKPOINT_ID) {
			this.checkpointId = checkpointId;
		}
	}

	boolean blockUntilCheckpointIsAcknowledged() throws Exception {
		synchronized (synchronizationPoint) {
			if (isSet() && !isDone()) {
				waiting = true;
				synchronizationPoint.wait();
				waiting = false;
			}

			if (!isCanceled() && !wasAlreadyCompleted) {
				wasAlreadyCompleted = true;
				return true;
			}

			return false;
		}
	}

	void acknowledgeCheckpointAndTrigger(final long checkpointId) {
		synchronized (synchronizationPoint) {
			if (isSet() && !isDone() && this.checkpointId == checkpointId) {
				completed = true;
				synchronizationPoint.notifyAll();
			}
		}
	}

	void cancelCheckpointLatch() {
		synchronized (synchronizationPoint) {
			if (!isDone()) {
				canceled = true;
				synchronizationPoint.notifyAll();
			}
		}
	}

	private boolean isDone () {
		return canceled || completed;
	}

	@VisibleForTesting
	boolean isWaiting() {
		return waiting;
	}

	@VisibleForTesting
	boolean isCompleted() {
		return completed;
	}

	@VisibleForTesting
	boolean isCanceled() {
		return canceled;
	}

	@VisibleForTesting
	boolean isSet() {
		return checkpointId != NOT_SET_CHECKPOINT_ID;
	}
}
