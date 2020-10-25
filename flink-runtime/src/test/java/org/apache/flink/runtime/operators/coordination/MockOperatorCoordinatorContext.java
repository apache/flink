/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MockOperatorCoordinatorContext implements OperatorCoordinator.Context {
	private final OperatorID operatorID;
	private final int numSubtasks;
	private final boolean failEventSending;

	private final Map<Integer, List<OperatorEvent>> eventsToOperator;
	private boolean jobFailed;

	public MockOperatorCoordinatorContext(OperatorID operatorID, int numSubtasks) {
		this(operatorID, numSubtasks, true);
	}

	public MockOperatorCoordinatorContext(OperatorID operatorID, int numSubtasks, boolean failEventSending) {
		this.operatorID = operatorID;
		this.numSubtasks = numSubtasks;
		this.eventsToOperator = new HashMap<>();
		this.jobFailed = false;
		this.failEventSending = failEventSending;
	}

	@Override
	public OperatorID getOperatorId() {
		return operatorID;
	}

	@Override
	public CompletableFuture<Acknowledge> sendEvent(
			OperatorEvent evt,
			int targetSubtask) throws TaskNotRunningException {
		eventsToOperator.computeIfAbsent(targetSubtask, subtaskId -> new ArrayList<>()).add(evt);
		if (failEventSending) {
			CompletableFuture<Acknowledge> future = new CompletableFuture<>();
			future.completeExceptionally(new FlinkRuntimeException("Testing Exception to fail event sending."));
			return future;
		} else {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	@Override
	public void failJob(Throwable cause) {
		jobFailed = true;
	}

	@Override
	public int currentParallelism() {
		return numSubtasks;
	}

	// -------------------------------

	public List<OperatorEvent> getEventsToOperatorBySubtaskId(int subtaskId) {
		return eventsToOperator.get(subtaskId);
	}

	public Map<Integer, List<OperatorEvent>> getEventsToOperator() {
		return eventsToOperator;
	}

	public boolean isJobFailed() {
		return jobFailed;
	}
}
