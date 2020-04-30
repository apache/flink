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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.runtime.BootstrapTransformationWithID;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;

/**
 * Savepoint metadata that can be modified.
 */
@Internal
public class SavepointMetadata {

	private final int maxParallelism;

	private final Collection<MasterState> masterStates;

	private final Map<OperatorID, OperatorStateSpec> operatorStateIndex;

	public SavepointMetadata(int maxParallelism, Collection<MasterState> masterStates, Collection<OperatorState> initialStates) {
		Preconditions.checkArgument(maxParallelism > 0
				&& maxParallelism <= UPPER_BOUND_MAX_PARALLELISM,
			"Maximum parallelism must be between 1 and " + UPPER_BOUND_MAX_PARALLELISM
				+ ". Found: " + maxParallelism);
		this.maxParallelism = maxParallelism;

		this.masterStates = Preconditions.checkNotNull(masterStates);

		this.operatorStateIndex = new HashMap<>(initialStates.size());
		initialStates.forEach(existingState -> operatorStateIndex.put(
			existingState.getOperatorID(),
			OperatorStateSpec.existing(existingState)));
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public Collection<MasterState> getMasterStates() {
		return masterStates;
	}

	/**
	 * @return Operator state for the given UID.
	 *
	 * @throws IOException If the savepoint does not contain operator state with the given uid.
	 */
	public OperatorState getOperatorState(String uid) throws IOException {
		OperatorID operatorID = OperatorIDGenerator.fromUid(uid);

		OperatorStateSpec operatorState = operatorStateIndex.get(operatorID);
		if (operatorState == null || operatorState.isNewStateTransformation()) {
			throw new IOException("Savepoint does not contain state with operator uid " + uid);
		}

		return operatorState.asExistingState();
	}

	public void removeOperator(String uid) {
		operatorStateIndex.remove(OperatorIDGenerator.fromUid(uid));
	}

	public void addOperator(String uid, BootstrapTransformation<?> transformation) {
		OperatorID id = OperatorIDGenerator.fromUid(uid);

		if (operatorStateIndex.containsKey(id)) {
			throw new IllegalArgumentException("The savepoint already contains uid " + uid + ". All uid's must be unique");
		}

		operatorStateIndex.put(id, OperatorStateSpec.newWithTransformation(new BootstrapTransformationWithID<>(id, transformation)));
	}

	/**
	 * @return List of {@link OperatorState} that already exists within the savepoint.
	 */
	public List<OperatorState> getExistingOperators() {
		return operatorStateIndex
			.values()
			.stream()
			.filter(OperatorStateSpec::isExistingState)
			.map(OperatorStateSpec::asExistingState)
			.collect(Collectors.toList());
	}

	/**
	 * @return List of new operator states for the savepoint, represented by their target {@link OperatorID} and {@link BootstrapTransformation}.
	 */
	public List<BootstrapTransformationWithID<?>> getNewOperators() {
		return operatorStateIndex
			.values()
			.stream()
			.filter(OperatorStateSpec::isNewStateTransformation)
			.map(OperatorStateSpec::asNewStateTransformation)
			.collect(Collectors.toList());
	}
}
