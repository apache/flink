/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.streaming.api.graph.StreamConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility to convert state between operator id mapped and chain mapped.
 */
public class OperatorIDMappedStateToChainConverter {

	public static TaskStateHandles convert(
		TaskStateSnapshot subtaskStates,
		StreamConfig streamConfig,
		int chainLength) {

		List<OperatorID> operatorIDsInChainOrder = new ArrayList<>(chainLength);
		operatorIDsInChainOrder.add(streamConfig.getOperatorID());
		Map<Integer, StreamConfig> chainedTaskConfigs =
			streamConfig.getTransitiveChainedTaskConfigs(streamConfig.getClass().getClassLoader());
		for (int i = 1; i < chainLength; ++i) {
			operatorIDsInChainOrder.add(chainedTaskConfigs.get(i).getOperatorID());
		}
		return convert(subtaskStates, operatorIDsInChainOrder);
	}

	public static TaskStateHandles convert(TaskStateSnapshot subtaskStates, List<OperatorID> operatorIDsInChainOrder) {
		final int chainLength = operatorIDsInChainOrder.size();

		List<StreamStateHandle> legacyStateChain = new ArrayList<>(chainLength);
		List<Collection<OperatorStateHandle>> managedOpState = new ArrayList<>(chainLength);
		List<Collection<OperatorStateHandle>> rawOpState = new ArrayList<>(chainLength);

		for (int i = 1; i < chainLength; ++i) {
			OperatorSubtaskState subtaskState = subtaskStates.getSubtaskStateByOperatorID(operatorIDsInChainOrder.get(i));
			legacyStateChain.add(subtaskState.getLegacyOperatorState());
			managedOpState.add(singletonListOrNull(subtaskState.getManagedOperatorState()));
			rawOpState.add(singletonListOrNull(subtaskState.getRawOperatorState()));
		}

		OperatorSubtaskState subtaskState = subtaskStates.getSubtaskStateByOperatorID(operatorIDsInChainOrder.get(0));
		legacyStateChain.add(subtaskState.getLegacyOperatorState());
		managedOpState.add(singletonListOrNull(subtaskState.getManagedOperatorState()));
		rawOpState.add(singletonListOrNull(subtaskState.getRawOperatorState()));

		ChainedStateHandle<StreamStateHandle> legacyChainedStateHandle = new ChainedStateHandle<>(legacyStateChain);

		TaskStateHandles taskStateHandles = new TaskStateHandles(
			legacyChainedStateHandle,
			managedOpState,
			rawOpState,
			singletonListOrNull(subtaskState.getManagedKeyedState()),
			singletonListOrNull(subtaskState.getRawKeyedState())
		);

		return taskStateHandles;
	}

	private static <T> List<T> singletonListOrNull(T item) {
		return item != null ? Collections.singletonList(item) : null;
	}
}
