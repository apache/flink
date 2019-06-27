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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.output.BoundedOneInputStreamTaskRunner;
import org.apache.flink.state.api.output.OperatorSubtaskStateReducer;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.output.operators.BroadcastStateBootstrapOperator;
import org.apache.flink.state.api.output.partitioner.HashSelector;
import org.apache.flink.state.api.output.partitioner.KeyGroupRangePartitioner;
import org.apache.flink.state.api.runtime.BoundedStreamConfig;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.OptionalInt;

/**
 * Bootstrapped data that can be written into a {@code Savepoint}.
 * @param <T> The input type of the transformation.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class BootstrapTransformation<T> {
	private final DataSet<T> dataSet;

	private final SavepointWriterOperatorFactory factory;

	@Nullable
	private final HashSelector<T> keySelector;

	@Nullable
	private final TypeInformation<?> keyType;

	private final OptionalInt operatorMaxParallelism;

	BootstrapTransformation(
		DataSet<T> dataSet,
		OptionalInt operatorMaxParallelism,
		SavepointWriterOperatorFactory factory) {
		this.dataSet = dataSet;
		this.operatorMaxParallelism = operatorMaxParallelism;
		this.factory = factory;
		this.keySelector = null;
		this.keyType = null;
	}

	<K> BootstrapTransformation(
		DataSet<T> dataSet,
		OptionalInt operatorMaxParallelism,
		SavepointWriterOperatorFactory factory,
		@Nonnull KeySelector<T, K> keySelector,
		@Nonnull TypeInformation<K> keyType) {
		this.dataSet = dataSet;
		this.operatorMaxParallelism = operatorMaxParallelism;
		this.factory = factory;
		this.keySelector = new HashSelector<>(keySelector);
		this.keyType = keyType;
	}

	/**
	 * @return The max parallelism for this operator.
	 */
	int getMaxParallelism(SavepointMetadata metadata) {
		return operatorMaxParallelism.orElse(metadata.maxParallelism());
	}

	/**
	 * @param operatorID The operator id for the stream operator.
	 * @param stateBackend The state backend for the job.
	 * @param metadata Metadata about the resulting savepoint.
	 * @param savepointPath The path where the savepoint will be written.
	 * @return The operator subtask states for this bootstrap transformation.
	 */
	DataSet<OperatorState> writeOperatorState(
		OperatorID operatorID,
		StateBackend stateBackend,
		SavepointMetadata metadata,
		Path savepointPath) {
		int localMaxParallelism = getMaxParallelism(metadata);

		return writeOperatorSubtaskStates(operatorID, stateBackend, savepointPath, localMaxParallelism)
			.reduceGroup(new OperatorSubtaskStateReducer(operatorID, localMaxParallelism))
			.name("reduce(OperatorSubtaskState)");
	}

	@VisibleForTesting
	MapPartitionOperator<T, TaggedOperatorSubtaskState> writeOperatorSubtaskStates(
		OperatorID operatorID,
		StateBackend stateBackend,
		Path savepointPath,
		int localMaxParallelism) {

		DataSet<T> input = dataSet;
		if (keySelector != null) {
			input = dataSet.partitionCustom(new KeyGroupRangePartitioner(localMaxParallelism), keySelector);
		}

		final StreamConfig config;
		if (keyType == null) {
			config = new BoundedStreamConfig();
		} else {
			TypeSerializer<?> keySerializer = keyType.createSerializer(dataSet.getExecutionEnvironment().getConfig());
			config = new BoundedStreamConfig(keySerializer, keySelector);
		}

		StreamOperator<TaggedOperatorSubtaskState> operator = factory.getOperator(
			System.currentTimeMillis(),
			savepointPath);

		operator = dataSet.clean(operator);
		config.setStreamOperator(operator);

		config.setOperatorName(operatorID.toHexString());
		config.setOperatorID(operatorID);
		config.setStateBackend(stateBackend);

		BoundedOneInputStreamTaskRunner<T> operatorRunner = new BoundedOneInputStreamTaskRunner<>(
			config,
			localMaxParallelism
		);

		MapPartitionOperator<T, TaggedOperatorSubtaskState> subtaskStates = input
			.mapPartition(operatorRunner)
			.name(operatorID.toHexString());

		if (operator instanceof BroadcastStateBootstrapOperator) {
			subtaskStates = subtaskStates.setParallelism(1);
		} else {
			int currentParallelism = getParallelism(subtaskStates);
			if (currentParallelism > localMaxParallelism) {
				subtaskStates.setParallelism(localMaxParallelism);
			}
		}
		return subtaskStates;
	}

	private static <T> int getParallelism(MapPartitionOperator<T, TaggedOperatorSubtaskState> subtaskStates) {
		int parallelism = subtaskStates.getParallelism();
		if (parallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
			parallelism = subtaskStates.getExecutionEnvironment().getParallelism();
		}

		return parallelism;
	}
}
