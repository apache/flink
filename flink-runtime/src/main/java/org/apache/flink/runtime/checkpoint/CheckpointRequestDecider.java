/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;

import java.util.ArrayDeque;
import java.util.Optional;

class CheckpointRequestDecider {
	/**
	 * A queue to cache those trigger requests which can't be trigger immediately.
	 */
	private final ArrayDeque<CheckpointTriggerRequest> triggerRequestQueue = new ArrayDeque<>();
	private final Object lock;

	CheckpointRequestDecider(Object lock) {
		this.lock = lock;
	}

	public Optional<CheckpointTriggerRequest> chooseRequestToExecute(boolean isTriggering, CheckpointTriggerRequest checkpointTriggerRequest) {
		synchronized (lock) {
			if (isTriggering || !triggerRequestQueue.isEmpty()) {
				// we can't trigger checkpoint directly if there is a trigger request being processed
				// or queued
				triggerRequestQueue.add(checkpointTriggerRequest);
				return Optional.empty();
			}
			return Optional.of(checkpointTriggerRequest);
		}
	}

	Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute() {
		synchronized (lock) {
			return getTriggerRequestQueue().isEmpty() ? Optional.empty() : Optional.ofNullable(getTriggerRequestQueue().poll());
		}
	}

	ArrayDeque<CheckpointTriggerRequest> getTriggerRequestQueue() {
		return triggerRequestQueue;
	}

	void abortAll(CheckpointException exception) {
		CheckpointTriggerRequest request;
		while ((request = getTriggerRequestQueue().poll()) != null) {
			request.completeExceptionally(exception);
		}
	}
}
