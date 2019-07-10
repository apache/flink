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
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * A synchronization primitive used by the {@link StreamTask} to wait
 * for the completion of a {@link CheckpointType#SYNC_SAVEPOINT}.
 */
class SynchronousSavepointLatch {

	private static final long NOT_SET_CHECKPOINT_ID = -1L;

	enum CompletionResult {
		COMPLETED,
		CANCELED,
	}

	@GuardedBy("synchronizationPoint")
	private volatile boolean waiting;

	@GuardedBy("synchronizationPoint")
	@Nullable
	private volatile CompletionResult completionResult;

	private final Object synchronizationPoint;

	private volatile long checkpointId;

	SynchronousSavepointLatch() {
		this.synchronizationPoint = new Object();

		this.waiting = false;
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

	void blockUntilCheckpointIsAcknowledged() throws InterruptedException {
		synchronized (synchronizationPoint) {
			if (isSet()) {
				while (completionResult == null) {
					waiting = true;
					synchronizationPoint.wait();
				}
				waiting = false;
			}
		}
	}

	void acknowledgeCheckpointAndTrigger(final long checkpointId, RunnableWithException runnable) throws Exception {
		synchronized (synchronizationPoint) {
			if (completionResult == null && this.checkpointId == checkpointId) {
				completionResult = CompletionResult.COMPLETED;
				try {
					runnable.run();
				} finally {
					synchronizationPoint.notifyAll();
				}
			}
		}
	}

	void cancelCheckpointLatch() {
		synchronized (synchronizationPoint) {
			if (completionResult == null) {
				completionResult = CompletionResult.CANCELED;
				synchronizationPoint.notifyAll();
			}
		}
	}

	@VisibleForTesting
	boolean isWaiting() {
		return waiting;
	}

	@VisibleForTesting
	boolean isCompleted() {
		return completionResult == CompletionResult.COMPLETED;
	}

	@VisibleForTesting
	boolean isCanceled() {
		return completionResult == CompletionResult.CANCELED;
	}

	boolean isSet() {
		return checkpointId != NOT_SET_CHECKPOINT_ID;
	}
}
