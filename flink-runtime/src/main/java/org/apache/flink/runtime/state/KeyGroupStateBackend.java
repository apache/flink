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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KeyGroupStateBackend extends AbstractStateBackend {
	private static final long serialVersionUID = 8734855408735321441L;

	private final KeyGroupAssigner<?> keyGroupAssigner;

	private final Map<Integer, AbstractStateBackend> keyGroupStateBackends;

	private final StateBackendFactory<?> stateBackendFactory;

	private final Configuration configuration;

	private transient Environment environment;

	private String operatorIdentifier;

	public KeyGroupStateBackend(
		KeyGroupAssigner<?> keyGroupAssigner,
		StateBackendFactory<?> stateBackendFactory,
		Configuration configuration) {
		this.keyGroupAssigner = keyGroupAssigner;
		this.keyGroupStateBackends = new HashMap<>();
		this.stateBackendFactory = stateBackendFactory;
		this.configuration = configuration;
	}

	@Override
	public void initializeForJob(
		Environment env,
		String operatorIdentifier,
		TypeSerializer<?> keySerializer) throws Exception {
		super.initializeForJob(env, operatorIdentifier, keySerializer);
		this.operatorIdentifier = operatorIdentifier;
		this.environment = env;
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		for (AbstractStateBackend stateBackend: keyGroupStateBackends.values()) {
			stateBackend.disposeAllStateForCurrentJob();
		}
	}

	@Override
	public void close() throws Exception {
		for (AbstractStateBackend stateBackend: keyGroupStateBackends.values()) {
			stateBackend.close();
		}
	}

	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception {
		return new KeyGroupValueState<>(this, keyGroupAssigner, namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		return new KeyGroupListState<>(this, keyGroupAssigner, namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		return new KeyGroupReducingState<>(this, keyGroupAssigner, namespaceSerializer, stateDesc);
	}

	@Override
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		return new KeyGroupFoldingState<>(this, keyGroupAssigner, namespaceSerializer, stateDesc);
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		return null;
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(S state, long checkpointID, long timestamp) throws Exception {
		return null;
	}

	AbstractStateBackend getBackend(int virtualPartitionID) {
		if (keyGroupStateBackends.containsKey(virtualPartitionID)) {
			return keyGroupStateBackends.get(virtualPartitionID);
		} else {
			AbstractStateBackend stateBackend;

			try {
				stateBackend = stateBackendFactory.createFromConfig(configuration);

				stateBackend.initializeForJob(environment, operatorIdentifier, keySerializer);
			} catch (Exception exception) {
				throw new RuntimeException("Could not create state backend.", exception);
			}

			keyGroupStateBackends.put(virtualPartitionID, stateBackend);

			return stateBackend;
		}
	}

	@Override
	public void dispose(StateDescriptor<?, ?> stateDescriptor) {
		for (AbstractStateBackend stateBackend: keyGroupStateBackends.values()) {
			stateBackend.dispose(stateDescriptor);
		}
	}
}
