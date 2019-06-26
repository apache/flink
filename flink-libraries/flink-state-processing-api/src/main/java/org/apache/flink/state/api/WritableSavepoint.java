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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.output.OperatorStateReducer;
import org.apache.flink.state.api.output.OperatorSubtaskStateReducer;
import org.apache.flink.state.api.output.SavepointOutputFormat;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Any savepoint that can be written to from a batch context.
 * @param <F> The implementation type.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public abstract class WritableSavepoint<F extends WritableSavepoint> {

	protected final Map<String, BootstrapTransformation> transformations;

	protected final List<String> droppedOperators;

	WritableSavepoint() {
		this.transformations  = new HashMap<>();
		this.droppedOperators = new ArrayList<>();
	}

	/**
	 * Drop an existing operator from the savepoint.
	 * @param uid The uid of the operator.
	 * @return A modified savepoint.
	 */
	@SuppressWarnings("unchecked")
	public F removeOperator(String uid) {
		droppedOperators.add(uid);
		transformations.remove(uid);
		return (F) this;
	}

	/**
	 * Adds a new operator to the savepoint.
	 * @param uid The uid of the operator.
	 * @param transformation The operator to be included.
	 * @return The modified savepoint.
	 */
	@SuppressWarnings("unchecked")
	public <T> F withOperator(String uid, BootstrapTransformation<T> transformation) {
		if (transformations.containsKey(uid)) {
			throw new IllegalArgumentException("The savepoint already contains uid " + uid + ". All uid's must be unique");
		}

		transformations.put(uid, transformation);
		return (F) this;
	}

	/**
	 * Write out a new or updated savepoint.
	 * @param path The path to where the savepoint should be written.
	 */
	public abstract void write(String path);

	protected static void write(
		Path savepointPath,
		Map<String, BootstrapTransformation> transformations,
		StateBackend stateBackend,
		SavepointMetadata metadata,
		@Nullable DataSet<OperatorState> existingOperators) {

		DataSet<OperatorState> newOperatorStates = transformations
			.entrySet()
			.stream()
			.map(entry -> getOperatorStates(savepointPath, entry.getKey(), stateBackend, entry.getValue(), metadata))
			.reduce(DataSet::union)
			.orElseThrow(() -> new IllegalStateException("Savepoint's must contain at least one operator"));

		DataSet<OperatorState> finalOperatorStates;
		if (existingOperators == null) {
			finalOperatorStates = newOperatorStates;
		} else {
			finalOperatorStates = newOperatorStates.union(existingOperators);
		}

		finalOperatorStates
			.reduceGroup(new OperatorStateReducer(metadata))
			.name("reduce(OperatorState)")
			.output(new SavepointOutputFormat(savepointPath))
			.name(savepointPath.toString());
	}

	@SuppressWarnings("unchecked")
	private static DataSet<OperatorState> getOperatorStates(
		Path savepointPath,
		String uid,
		StateBackend stateBackend,
		BootstrapTransformation operator,
		SavepointMetadata metadata) {

		return operator
			.getOperatorSubtaskStates(uid, stateBackend, metadata, savepointPath)
			.reduceGroup(new OperatorSubtaskStateReducer(uid, operator.getMaxParallelism(metadata)))
			.name("reduce(OperatorSubtaskState)");
	}
}
