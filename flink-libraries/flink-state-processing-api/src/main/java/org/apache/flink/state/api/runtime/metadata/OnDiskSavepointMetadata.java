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

package org.apache.flink.state.api.runtime.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.SavepointLoader;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns metadata about an existing savepoint.
 */
@Internal
public class OnDiskSavepointMetadata implements SavepointMetadata {

	private static final long serialVersionUID = 3623389893479485802L;

	private final int maxParallelism;

	private final Collection<MasterState> masterStates;

	private final Map<OperatorID, OperatorState> operatorStateIndex;

	public OnDiskSavepointMetadata(String path) throws IOException {
		Savepoint savepoint = SavepointLoader.loadSavepoint(path);

		this.maxParallelism = savepoint
			.getOperatorStates()
			.stream()
			.map(OperatorState::getMaxParallelism)
			.max(Comparator.naturalOrder())
			.orElseThrow(() -> new RuntimeException("Savepoint's must contain at least one operator"));

		this.masterStates = savepoint.getMasterStates();

		Collection<OperatorState> operatorStates = savepoint.getOperatorStates();
		this.operatorStateIndex = new HashMap<>(operatorStates.size());
		operatorStates.forEach(state -> this.operatorStateIndex.put(state.getOperatorID(), state));
	}

	/**
	 * @return The max parallelism for the savepoint.
	 */
	@Override
	public int maxParallelism() {
		return maxParallelism;
	}

	/**
	 * @return Masters states for the savepoint.
	 */
	@Override
	public Collection<MasterState> getMasterStates() {
		return masterStates;
	}

	/**
	 * @return Operator state for the given UID.
	 */
	@Override
	public OperatorState getOperatorState(String uid) throws IOException {
		OperatorID operatorID = OperatorIDGenerator.fromUid(uid);

		OperatorState operatorState = operatorStateIndex.get(operatorID);
		if (operatorState == null) {
			throw new IOException("Savepoint does not contain state with operator uid " + uid);
		}

		return operatorState;
	}
}
