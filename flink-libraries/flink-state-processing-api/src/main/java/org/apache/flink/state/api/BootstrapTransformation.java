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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.output.BoundedOneInputStreamTaskRunner;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.output.operators.BroadcastStateBootstrapOperator;
import org.apache.flink.state.api.output.partitioner.HashSelector;
import org.apache.flink.state.api.output.partitioner.KeyGroupRangePartitioner;
import org.apache.flink.state.api.runtime.BoundedStreamConfig;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

	BootstrapTransformation(
		DataSet<T> dataSet,
		SavepointWriterOperatorFactory factory) {
		this.dataSet = dataSet;
		this.factory = factory;
		this.keySelector = null;
		this.keyType = null;
	}

	<K> BootstrapTransformation(
		DataSet<T> dataSet,
		SavepointWriterOperatorFactory factory,
		@Nonnull KeySelector<T, K> keySelector,
		@Nonnull TypeInformation<K> keyType) {
		this.dataSet = dataSet;
		this.factory = factory;
		this.keySelector = new HashSelector<>(keySelector);
		this.keyType = keyType;
	}

	DataSet<TaggedOperatorSubtaskState> getOperatorSubtaskStates(
		String uid,
		StateBackend stateBackend,
		SavepointMetadata metadata,
		Path savepointPath) {
		DataSet<T> input = dataSet;
		if (keySelector != null) {
			input = dataSet.partitionCustom(new KeyGroupRangePartitioner(metadata.maxParallelism()), keySelector);
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

		config.setOperatorName(uid);
		config.setOperatorID(OperatorIDGenerator.fromUid(uid));
		config.setStateBackend(stateBackend);

		BoundedOneInputStreamTaskRunner<T> operatorRunner = new BoundedOneInputStreamTaskRunner<>(
			config,
			metadata
		);

		MapPartitionOperator<T, TaggedOperatorSubtaskState> subtaskStates = input
			.mapPartition(operatorRunner)
			.name(uid);

		if (operator instanceof BroadcastStateBootstrapOperator) {
			subtaskStates = subtaskStates.setParallelism(1);
		} else {
			int currentParallelism = subtaskStates.getParallelism();
			if (currentParallelism > metadata.maxParallelism()) {
				subtaskStates.setParallelism(metadata.maxParallelism());
			}
		}

		return subtaskStates;
	}
}
