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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link OperatorCoordinator} and its contextual information needed to trigger and
 * acknowledge a checkpoint.
 */
public final class OperatorCoordinatorCheckpointContext {

	private final OperatorCoordinator coordinator;

	private final OperatorID operatorId;

	private final int maxParallelism;

	private final int currentParallelism;

	public OperatorCoordinatorCheckpointContext(
			OperatorCoordinator coordinator,
			OperatorID operatorId,
			int maxParallelism,
			int currentParallelism) {

		this.coordinator = checkNotNull(coordinator);
		this.operatorId = checkNotNull(operatorId);
		this.maxParallelism = maxParallelism;
		this.currentParallelism = currentParallelism;
	}

	public OperatorCoordinator coordinator() {
		return coordinator;
	}

	public OperatorID operatorId() {
		return operatorId;
	}

	public int maxParallelism() {
		return maxParallelism;
	}

	public int currentParallelism() {
		return currentParallelism;
	}

	public static Collection<OperatorID> getIds(Collection<OperatorCoordinatorCheckpointContext> infos) {
		return infos.stream()
			.map(OperatorCoordinatorCheckpointContext::operatorId)
			.collect(Collectors.toList());
	}
}
