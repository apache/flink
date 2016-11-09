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

package org.apache.flink.migration.runtime.state.filesystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.state.AbstractHeapState;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for partitioned {@link ListState} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 * @param <S> The type of State
 * @param <SD> The type of StateDescriptor for the State S
 */
public abstract class AbstractFsState<K, N, SV, S extends State, SD extends StateDescriptor<S, ?>>
		extends AbstractHeapState<K, N, SV, S, SD, FsStateBackend> {

	/** The file system state backend backing snapshots of this state */
	private final FsStateBackend backend;

	public AbstractFsState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc) {
		super(keySerializer, namespaceSerializer, stateSerializer, stateDesc);
		this.backend = backend;
	}

	public AbstractFsState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		HashMap<N, Map<K, SV>> state) {
		super(keySerializer, namespaceSerializer, stateSerializer, stateDesc, state);
		this.backend = backend;
	}

	public abstract KvStateSnapshot<K, N, S, SD, FsStateBackend> createHeapSnapshot(Path filePath);
}
