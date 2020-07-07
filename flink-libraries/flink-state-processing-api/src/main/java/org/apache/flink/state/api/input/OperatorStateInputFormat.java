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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.RoundRobinOperatorStateRepartitioner;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.state.api.input.splits.OperatorStateInputSplit;
import org.apache.flink.streaming.api.operators.BackendRestorerProcedure;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.reDistributePartitionableStates;

/**
 * The base input format for reading operator state from a {@link CheckpointMetadata}.
 *
 * @param <OT> The type of the input.
 */
@Internal
abstract class OperatorStateInputFormat<OT> extends RichInputFormat<OT, OperatorStateInputSplit> {

	private static final long serialVersionUID = -2286490341042373742L;

	private final OperatorState operatorState;

	private final boolean isUnionType;

	private transient OperatorStateBackend restoredBackend;

	private transient CloseableRegistry registry;

	private transient Iterator<OT> elements;

	OperatorStateInputFormat(OperatorState operatorState, boolean isUnionType) {
		Preconditions.checkNotNull(operatorState, "The operator state cannot be null");

		this.operatorState = operatorState;
		this.isUnionType = isUnionType;
	}

	protected abstract Iterable<OT> getElements(OperatorStateBackend restoredBackend) throws Exception;

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return cachedStatistics;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(OperatorStateInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	public OperatorStateInputSplit[] createInputSplits(int minNumSplits) {
		OperatorStateInputSplit[] splits = getOperatorStateInputSplits(minNumSplits);

		if (isUnionType) {
			return subPartitionSingleSplit(minNumSplits, splits);
		} else {
			return splits;
		}
	}

	private OperatorStateInputSplit[] subPartitionSingleSplit(int minNumSplits, OperatorStateInputSplit[] splits) {
		if (splits.length == 0) {
			return splits;
		}

		// We only want to output a single instance of the union state so we only need
		// to transform a single input split. An arbitrary split is chosen and
		// sub-partitioned for better data distribution across the cluster.
		return CollectionUtil.mapWithIndex(
			CollectionUtil.partition(splits[0].getPrioritizedManagedOperatorState().get(0).asList(), minNumSplits),
			(state, index) ->  new OperatorStateInputSplit(new StateObjectCollection<>(new ArrayList<>(state)), index)
		).toArray(OperatorStateInputSplit[]::new);
	}

	private OperatorStateInputSplit[] getOperatorStateInputSplits(int minNumSplits) {
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates = reDistributePartitionableStates(
			singletonList(operatorState),
			minNumSplits,
			singletonList(OperatorIDPair.generatedIDOnly(operatorState.getOperatorID())),
			OperatorSubtaskState::getManagedOperatorState,
			RoundRobinOperatorStateRepartitioner.INSTANCE);

		return CollectionUtil.mapWithIndex(
			newManagedOperatorStates.values(),
			(handles, index) -> new OperatorStateInputSplit(new StateObjectCollection<>(handles), index)
		).toArray(OperatorStateInputSplit[]::new);
	}

	@Override
	public void open(OperatorStateInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				(handles) -> createOperatorStateBackend(getRuntimeContext(), handles, registry),
				registry,
				operatorState.getOperatorID().toString()
			);

		try {
			restoredBackend = backendRestorer.createAndRestore(split.getPrioritizedManagedOperatorState());
		} catch (Exception exception) {
			throw new IOException("Failed to restore state backend", exception);
		}

		try {
			elements = getElements(restoredBackend).iterator();
		} catch (Exception e) {
			throw new IOException("Failed to read operator state from restored state backend", e);
		}
	}

	@Override
	public void close() {
		registry.unregisterCloseable(restoredBackend);
		IOUtils.closeQuietly(restoredBackend);
		IOUtils.closeQuietly(registry);
	}

	@Override
	public boolean reachedEnd() {
		return !elements.hasNext();
	}

	@Override
	public OT nextRecord(OT reuse) {
		return elements.next();
	}

	private static OperatorStateBackend createOperatorStateBackend(
		RuntimeContext runtimeContext,
		Collection<OperatorStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry){

		try {
			return new DefaultOperatorStateBackendBuilder(
				runtimeContext.getUserCodeClassLoader(),
				runtimeContext.getExecutionConfig(),
				false,
				stateHandles,
				cancelStreamRegistry
			).build();
		} catch (BackendBuildingException e) {
			throw new RuntimeException(e);
		}
	}
}
