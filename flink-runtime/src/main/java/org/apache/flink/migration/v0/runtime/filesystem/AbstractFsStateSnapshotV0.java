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

package org.apache.flink.migration.v0.runtime.filesystem;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.migration.v0.runtime.memory.AbstractMigrationRestoreStrategy;
import org.apache.flink.migration.v0.runtime.memory.MigrationRestoreSnapshot;
import org.apache.flink.migration.v0.api.StateDescriptorV0;
import org.apache.flink.migration.v0.runtime.KvStateSnapshotV0;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.StateTable;

import java.io.IOException;

/**
 * A snapshot for keyed states of FsStateBackend in SavepointV0.
 */
@Deprecated
@SuppressWarnings("deprecation")
public abstract class AbstractFsStateSnapshotV0<K, N, SV, S extends State, SD extends StateDescriptorV0<S, ?>>
		extends AbstractFileStateHandleV0 implements KvStateSnapshotV0<K, N, S, SD>, MigrationRestoreSnapshot<K, N, SV> {

	private static final long serialVersionUID = 1L;

	/** Key Serializer */
	protected final TypeSerializer<K> keySerializer;

	/** Namespace Serializer */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state value */
	protected final TypeSerializer<SV> stateSerializer;

	/** StateDescriptor, for sanity checks */
	protected final SD stateDesc;

	public AbstractFsStateSnapshotV0(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		Path filePath) {
		super(filePath);
		this.stateDesc = stateDesc;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.namespaceSerializer = namespaceSerializer;

	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializer<SV> getStateSerializer() {
		return stateSerializer;
	}

	public SD getStateDesc() {
		return stateDesc;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StateTable<K, N, SV> deserialize(
			String stateName,
			HeapKeyedStateBackend<K> stateBackend) throws IOException {

		final FileSystem fs = getFilePath().getFileSystem();
		try (FSDataInputStream inStream = fs.open(getFilePath())) {
			final DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inStream);
			AbstractMigrationRestoreStrategy<K, N, SV> restoreStrategy =
					new AbstractMigrationRestoreStrategy<K, N, SV>(keySerializer, namespaceSerializer, stateSerializer) {
						@Override
						protected DataInputView openDataInputView() throws IOException {
							return inView;
						}
					};
			return restoreStrategy.deserialize(stateName, stateBackend);
		}
	}
}
