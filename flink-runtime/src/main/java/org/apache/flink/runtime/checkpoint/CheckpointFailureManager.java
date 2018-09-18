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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The checkpoint failure manager to manage how to process checkpoint failure.
 */
public class CheckpointFailureManager {

	private final boolean failOnCheckpointingErrors;
	private final int tolerableCpFailureNumber;
	private final AtomicInteger continuousFailureCounter;
	private final ExecutionGraph executionGraph;
	private final Object lock = new Object();

	public CheckpointFailureManager(
		boolean failOnCheckpointingErrors,
		int tolerableCpFailureNumber,
		ExecutionGraph executionGraph) {
		this.failOnCheckpointingErrors = failOnCheckpointingErrors;
		this.tolerableCpFailureNumber = tolerableCpFailureNumber;
		this.continuousFailureCounter = new AtomicInteger(0);
		this.executionGraph = checkNotNull(executionGraph);
	}

	@VisibleForTesting
	public AtomicInteger getContinuousFailureCounter() {
		return continuousFailureCounter;
	}

	public void resetCounter() {
		continuousFailureCounter.set(0);
	}

	public void tryHandleFailure(String reason, long checkpointId) {
		synchronized (lock) {
			if (failOnCheckpointingErrors ||
				continuousFailureCounter.incrementAndGet() > tolerableCpFailureNumber) {
				executionGraph.failGlobal(new Throwable(reason));
			}
		}
	}

	public void tryHandleFailure(Throwable cause, long checkpointId) {
		synchronized (lock) {
			if (failOnCheckpointingErrors ||
				continuousFailureCounter.incrementAndGet() > tolerableCpFailureNumber) {
				executionGraph.failGlobal(cause);
			}
		}
	}

	public void tryHandleFailure(CheckpointDeclineReason reason, long checkpointId) {
		synchronized (lock) {
			if (failOnCheckpointingErrors ||
				continuousFailureCounter.incrementAndGet() > tolerableCpFailureNumber) {
				executionGraph.failGlobal(
					new Throwable("Checkpoint : " + checkpointId + reason.message()));
			}
		}
	}

}
