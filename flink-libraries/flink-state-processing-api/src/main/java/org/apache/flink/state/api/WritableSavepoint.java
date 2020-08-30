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
import org.apache.flink.state.api.output.FileCopyFunction;
import org.apache.flink.state.api.output.MergeOperatorStates;
import org.apache.flink.state.api.output.SavepointOutputFormat;
import org.apache.flink.state.api.output.StatePathExtractor;
import org.apache.flink.state.api.runtime.BootstrapTransformationWithID;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * A {@code WritableSavepoint} is any savepoint that can be written to from a batch context.
 * Internally, a {@link SavepointMetadata} object is maintained that keeps track of the set
 * of existing operator states in the savepoint, as well as newly added operator states defined by their
 * {@link BootstrapTransformation}.
 *
 * @param <F> The implementation type.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public abstract class WritableSavepoint<F extends WritableSavepoint> {

	/** The savepoint metadata, which maintains the current set of existing / newly added operator states. */
	protected final SavepointMetadata metadata;

	/** The state backend to use when writing this savepoint. */
	protected final StateBackend stateBackend;

	WritableSavepoint(SavepointMetadata metadata, StateBackend stateBackend) {
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

		List<BootstrapTransformationWithID<?>> newOperatorTransformations = metadata.getNewOperators();
		DataSet<OperatorState> newOperatorStates = writeOperatorStates(newOperatorTransformations, savepointPath);

		List<OperatorState> existingOperators = metadata.getExistingOperators();

		DataSet<OperatorState> finalOperatorStates;
		if (existingOperators.isEmpty()) {
			finalOperatorStates = newOperatorStates;
		} else {
			DataSet<OperatorState> existingOperatorStates = newOperatorStates.getExecutionEnvironment()
				.fromCollection(existingOperators);

			existingOperatorStates
				.flatMap(new StatePathExtractor())
				.setParallelism(1)
				.output(new FileCopyFunction(path));

			finalOperatorStates = newOperatorStates.union(existingOperatorStates);

		}
		finalOperatorStates
			.reduceGroup(new MergeOperatorStates(metadata.getMasterStates()))
			.name("reduce(OperatorState)")
			.output(new SavepointOutputFormat(savepointPath))
			.name(path);
	}

	private DataSet<OperatorState> writeOperatorStates(
		List<BootstrapTransformationWithID<?>> newOperatorStates,
		Path savepointWritePath) {
		return newOperatorStates
			.stream()
			.map(newOperatorState -> newOperatorState
				.getBootstrapTransformation()
				.writeOperatorState(newOperatorState.getOperatorID(), stateBackend, metadata.getMaxParallelism(), savepointWritePath))
			.reduce(DataSet::union)
			.orElseThrow(() -> new IllegalStateException("Savepoint must contain at least one operator"));
	}
}
