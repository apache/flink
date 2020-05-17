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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.jobgraph.OperatorID;

/**
 * A testing mock implementation of the {@link OperatorCoordinatorCheckpointContext}.
 */
public class MockOperatorCoordinatorCheckpointContext implements OperatorCoordinatorCheckpointContext {

	private final OperatorID operatorId;
	private final OperatorCoordinator coordinator;
	private final int parallelism;
	private final int maxParallelism;

	public MockOperatorCoordinatorCheckpointContext() {
		this(new OperatorID(), new MockOperatorCoordinator(), 50, 256);
	}

	public MockOperatorCoordinatorCheckpointContext(
			OperatorID operatorId,
			OperatorCoordinator coordinator,
			int parallelism,
			int maxParallelism) {
		this.operatorId = operatorId;
		this.coordinator = coordinator;
		this.parallelism = parallelism;
		this.maxParallelism = maxParallelism;
	}

	@Override
	public OperatorCoordinator coordinator() {
		return coordinator;
	}

	@Override
	public OperatorID operatorId() {
		return operatorId;
	}

	@Override
	public int maxParallelism() {
		return maxParallelism;
	}

	@Override
	public int currentParallelism() {
		return parallelism;
	}

	@Override
	public void onCallTriggerCheckpoint(long checkpointId) {}

	@Override
	public void onCheckpointStateFutureComplete(long checkpointId) {}

	@Override
	public void afterSourceBarrierInjection(long checkpointId) {}

	@Override
	public void abortCurrentTriggering() {}
}
