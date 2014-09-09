/**
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

package org.apache.flink.runtime.executiongraph;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.runtime.instance.AllocatedSlot;

public class ExecutionAttempt {

	private final AtomicBoolean finished = new AtomicBoolean();
	
	private final ExecutionAttemptID attemptId;
	
	private final AllocatedSlot assignedResource;
	
	private final int attemptNumber;
	
	private final long startTimestamp;
	
	private volatile long endTimestamp;
	
	private volatile Throwable failureCause;
	
	// --------------------------------------------------------------------------------------------
	
	public ExecutionAttemptID getAttemptId() {
		return attemptId;
	}
	
	public ExecutionAttempt(ExecutionAttemptID attemptId, AllocatedSlot assignedResource, int attemptNumber, long startTimestamp) {
		this.attemptId = attemptId;
		this.assignedResource = assignedResource;
		this.attemptNumber = attemptNumber;
		this.startTimestamp = startTimestamp;
	}
	
	// --------------------------------------------------------------------------------------------

	public AllocatedSlot getAssignedResource() {
		return assignedResource;
	}
	
	public int getAttemptNumber() {
		return attemptNumber;
	}
	
	public long getStartTimestamp() {
		return startTimestamp;
	}
	
	public long getEndTimestamp() {
		return endTimestamp;
	}
	
	public Throwable getFailureCause() {
		return failureCause;
	}
	
	public boolean isFinished() {
		return finished.get();
	}
	
	public boolean isFailed() {
		return finished.get() && failureCause != null;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public boolean finish() {
		if (finished.compareAndSet(false, true)) {
			endTimestamp = System.currentTimeMillis();
			return true;
		} else {
			return false;
		}
	}
	
	public boolean fail(Throwable error) {
		if (finished.compareAndSet(false, true)) {
			failureCause = error;
			return true;
		} else {
			return false;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("Attempt #%d (%s) @ %s - started %d %s", attemptNumber, attemptId,
				assignedResource.toString(), startTimestamp, isFinished() ? "finished " + endTimestamp : "[RUNNING]");
	}
}
