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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationProxy;
import org.apache.flink.core.io.VersionMismatchException;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization proxy for all meta data in keyed state backends. In the future we might also requiresMigration the actual state
 * serialization logic here.
 */
public class KeyedBackendSerializationProxy extends VersionedIOReadableWritable {

	public static final int VERSION = 3;

	private TypeSerializer<?> keySerializer;
	private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots;

	private int restoredVersion;
	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public KeyedBackendSerializationProxy(
			TypeSerializer<?> keySerializer,
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) {

		this.keySerializer = Preconditions.checkNotNull(keySerializer);

		Preconditions.checkNotNull(stateMetaInfoSnapshots);
		Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
		this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;

		this.restoredVersion = VERSION;
	}

	public List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> getStateMetaInfoSnapshots() {
		return stateMetaInfoSnapshots;
	}

	public TypeSerializer<?> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	public int getRestoredVersion() {
		return restoredVersion;
	}

	@Override
	protected void resolveVersionRead(int foundVersion) throws VersionMismatchException {
		super.resolveVersionRead(foundVersion);
		this.restoredVersion = foundVersion;
	}

	@Override
	public boolean isCompatibleVersion(int version) {
		// we are compatible with version 3 (Flink 1.3.x) and version 1 & 2 (Flink 1.2.x)
		return super.isCompatibleVersion(version) || version == 2 || version == 1;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		new TypeSerializerSerializationProxy<>(keySerializer).write(out);

		out.writeShort(stateMetaInfoSnapshots.size());

		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> metaInfo : stateMetaInfoSnapshots) {
			KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(VERSION, metaInfo)
				.writeStateMetaInfo(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		final TypeSerializerSerializationProxy<?> keySerializerProxy =
			new TypeSerializerSerializationProxy<>(userCodeClassLoader);
		keySerializerProxy.read(in);
		this.keySerializer = keySerializerProxy.getTypeSerializer();

		int numKvStates = in.readShort();
		stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; i++) {
			stateMetaInfoSnapshots.add(
				KeyedBackendStateMetaInfoSnapshotReaderWriters
					.getReaderForVersion(restoredVersion, userCodeClassLoader)
					.readStateMetaInfo(in));
		}
	}
}
