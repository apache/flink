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
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationProxy;
import org.apache.flink.api.common.typeutils.TypeSerializerUtil;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
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
	private TypeSerializerConfigSnapshot keySerializerConfigSnapshot;

	private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots;

	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public KeyedBackendSerializationProxy(
			TypeSerializer<?> keySerializer,
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) {

		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializer.snapshotConfiguration());

		Preconditions.checkNotNull(stateMetaInfoSnapshots);
		Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
		this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
	}

	public List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> getStateMetaInfoSnapshots() {
		return stateMetaInfoSnapshots;
	}

	public TypeSerializer<?> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public int[] getCompatibleVersions() {
		// we are compatible with version 3 (Flink 1.3.x) and version 1 & 2 (Flink 1.2.x)
		return new int[] {VERSION, 2, 1};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		// write in a way to be fault tolerant of read failures when deserializing the key serializer
		try (
			ByteArrayOutputStreamWithPos buffer = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper bufferWrapper = new DataOutputViewStreamWrapper(buffer)){

			new TypeSerializerSerializationProxy<>(keySerializer).write(bufferWrapper);

			// write offset of key serializer's configuration snapshot
			out.writeInt(buffer.getPosition());
			TypeSerializerUtil.writeSerializerConfigSnapshot(bufferWrapper, keySerializerConfigSnapshot);

			// flush buffer
			out.writeInt(buffer.getPosition());
			out.write(buffer.getBuf(), 0, buffer.getPosition());
		}

		// write individual registered keyed state metainfos
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

		// only starting from version 3, we have the key serializer and its config snapshot written
		if (getReadVersion() >= 3) {
			int keySerializerConfigSnapshotOffset = in.readInt();
			int numBufferedBytes = in.readInt();
			byte[] keySerializerAndConfigBytes = new byte[numBufferedBytes];
			in.readFully(keySerializerAndConfigBytes);

			try (
				ByteArrayInputStreamWithPos buffer = new ByteArrayInputStreamWithPos(keySerializerAndConfigBytes);
				DataInputViewStreamWrapper bufferWrapper = new DataInputViewStreamWrapper(buffer)) {

				try {
					keySerializerProxy.read(bufferWrapper);
					this.keySerializer = keySerializerProxy.getTypeSerializer();
				} catch (IOException e) {
					this.keySerializer = null;
				}

				buffer.setPosition(keySerializerConfigSnapshotOffset);
				this.keySerializerConfigSnapshot =
					TypeSerializerUtil.readSerializerConfigSnapshot(bufferWrapper, userCodeClassLoader);
			}
		} else {
			keySerializerProxy.read(in);
			this.keySerializer = keySerializerProxy.getTypeSerializer();
			this.keySerializerConfigSnapshot = null;
		}

		int numKvStates = in.readShort();
		stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; i++) {
			stateMetaInfoSnapshots.add(
				KeyedBackendStateMetaInfoSnapshotReaderWriters
					.getReaderForVersion(getReadVersion(), userCodeClassLoader)
					.readStateMetaInfo(in));
		}
	}
}
