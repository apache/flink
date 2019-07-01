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
import org.apache.flink.state.api.output.MergeOperatorStates;
import org.apache.flink.state.api.output.SavepointOutputFormat;
import org.apache.flink.state.api.runtime.metadata.ModifiableSavepointMetadata;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Any savepoint that can be written to from a batch context.
 * @param <F> The implementation type.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public abstract class WritableSavepoint<F extends WritableSavepoint> {

	protected final ModifiableSavepointMetadata metadata;

	protected final StateBackend stateBackend;

	WritableSavepoint(ModifiableSavepointMetadata metadata, StateBackend stateBackend) {
		Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
		Preconditions.checkNotNull(stateBackend, "The state backend must not be null");
		this.metadata = metadata;
		this.stateBackend = stateBackend;
	}

	/**
	 * Drop an existing operator from the savepoint.
	 * @param uid The uid of the operator.
	 * @return A modified savepoint.
	 */
	@SuppressWarnings("unchecked")
	public F removeOperator(String uid) {
		metadata.removeOperator(uid);
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
		metadata.addOperator(uid, transformation);
		return (F) this;
	}

	/**
	 * Write out a new or updated savepoint.
	 * @param path The path to where the savepoint should be written.
	 */
	public final void write(String path) {
		final Path savepointPath = new Path(path);

		DataSet<OperatorState> newOperatorStates = getOperatorStates(savepointPath);

		List<OperatorState> existingOperators = getExistingOperatorStates();

		DataSet<OperatorState> finalOperatorStates = unionOperatorStates(newOperatorStates, existingOperators);

		finalOperatorStates
			.reduceGroup(new MergeOperatorStates(metadata))
			.name("reduce(OperatorState)")
			.output(new SavepointOutputFormat(savepointPath))
			.name(path);
	}

	private DataSet<OperatorState> unionOperatorStates(DataSet<OperatorState> newOperatorStates, List<OperatorState> existingOperators) {
		DataSet<OperatorState> finalOperatorStates;
		if (existingOperators.isEmpty()) {
			finalOperatorStates = newOperatorStates;
		} else {
			DataSet<OperatorState> wrappedCollection = newOperatorStates
				.getExecutionEnvironment()
				.fromCollection(existingOperators);

			finalOperatorStates = newOperatorStates.union(wrappedCollection);
		}
		return finalOperatorStates;
	}

	private List<OperatorState> getExistingOperatorStates() {
		return metadata
				.getOperatorStates()
				.entrySet()
				.stream()
				.filter(entry -> entry.getValue().isLeft())
				.map(entry -> entry.getValue().left())
				.collect(Collectors.toList());
	}

	private DataSet<OperatorState> getOperatorStates(Path savepointPath) {
		return metadata
				.getOperatorStates()
				.entrySet()
				.stream()
				.filter(entry -> entry.getValue().isRight())
				.map(entry -> entry.getValue().right().writeOperatorState(entry.getKey(), stateBackend, metadata, savepointPath))
				.reduce(DataSet::union)
				.orElseThrow(() -> new IllegalStateException("Savepoint's must contain at least one operator"));
	}
}
