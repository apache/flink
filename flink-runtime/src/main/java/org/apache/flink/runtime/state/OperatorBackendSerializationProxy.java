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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.IdentitySerializerIndex;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization proxy for all meta data in operator state backends. In the future we might also requiresMigration the actual state
 * serialization logic here.
 */
public class OperatorBackendSerializationProxy extends VersionedIOReadableWritable {

	public static final int VERSION = 2;

	private List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> stateMetaInfoSnapshots;
	private ClassLoader userCodeClassLoader;

	private boolean excludeSerializers;

	public OperatorBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public OperatorBackendSerializationProxy(
			List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> stateMetaInfoSnapshots,
			boolean excludeSerializers) {

		this.stateMetaInfoSnapshots = Preconditions.checkNotNull(stateMetaInfoSnapshots);
		Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);

		this.excludeSerializers = excludeSerializers;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public int[] getCompatibleVersions() {
		// we are compatible with version 2 (Flink 1.3.x) and version 1 (Flink 1.2.x)
		return new int[] {VERSION, 1};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeBoolean(excludeSerializers);

		IdentitySerializerIndex serializerIndex = null;
		if (!excludeSerializers) {
			serializerIndex = buildSerializerIndex();
			serializerIndex.write(out);
		}

		out.writeShort(stateMetaInfoSnapshots.size());
		for (RegisteredOperatorBackendStateMetaInfo.Snapshot<?> kvState : stateMetaInfoSnapshots) {
			OperatorBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(VERSION, kvState)
				.writeStateMetaInfo(out, serializerIndex);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		IdentitySerializerIndex serializerIndex = null;

		// the excludeSerializers flag and serializer index was introduced only after version >= 2 (since Flink 1.3.x)
		if (getReadVersion() >= 2) {
			excludeSerializers = in.readBoolean();

			if (!excludeSerializers) {
				serializerIndex = new IdentitySerializerIndex(userCodeClassLoader);
				serializerIndex.read(in);
			}
		} else {
			excludeSerializers = false;
		}

		int numKvStates = in.readShort();
		stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; i++) {
			stateMetaInfoSnapshots.add(
				OperatorBackendStateMetaInfoSnapshotReaderWriters
					.getReaderForVersion(getReadVersion(), userCodeClassLoader)
					.readStateMetaInfo(in, serializerIndex));
		}
	}

	public List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> getStateMetaInfoSnapshots() {
		return stateMetaInfoSnapshots;
	}

	private IdentitySerializerIndex buildSerializerIndex() {
		final IdentitySerializerIndex serializerIndex = new IdentitySerializerIndex();

		for (RegisteredOperatorBackendStateMetaInfo.Snapshot<?> stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			serializerIndex.index(stateMetaInfoSnapshot.getPartitionStateSerializer());
		}

		return serializerIndex;
	}
}
