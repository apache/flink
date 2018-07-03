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

import org.apache.flink.api.common.typeutils.BackwardsCompatibleConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshotSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class KeyedBackendSerializationProxy<K> extends VersionedIOReadableWritable {

	public static final int VERSION = 5;

	//TODO allow for more (user defined) compression formats + backwards compatibility story.
	/** This specifies if we use a compressed format write the key-groups */
	private boolean usingKeyGroupCompression;

	/** This specifies whether or not to use dummy {@link UnloadableDummyTypeSerializer} when serializers cannot be read. */
	private boolean isSerializerPresenceRequired;

	private TypeSerializerConfigSnapshot<K> keySerializerConfigSnapshot;

	private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots;

	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader, boolean isSerializerPresenceRequired) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		this.isSerializerPresenceRequired = isSerializerPresenceRequired;
	}

	public KeyedBackendSerializationProxy(
			TypeSerializer<K> keySerializer,
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots,
			boolean compression) {

		this.usingKeyGroupCompression = compression;

		Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializer.snapshotConfiguration());

		Preconditions.checkNotNull(stateMetaInfoSnapshots);
		Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
		this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
	}

	public List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> getStateMetaInfoSnapshots() {
		return stateMetaInfoSnapshots;
	}

	public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	public boolean isUsingKeyGroupCompression() {
		return usingKeyGroupCompression;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public int[] getCompatibleVersions() {
		// we are compatible with version 4 (Flink 1.5.x), version 3 (Flink 1.3.x+) and version 1 & 2 (Flink 1.2.x)
		return new int[] {VERSION, 4, 3, 2, 1};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		// write the compression format used to write each key-group
		out.writeBoolean(usingKeyGroupCompression);

		TypeSerializerConfigSnapshotSerializationUtil.writeSerializerConfigSnapshot(out, keySerializerConfigSnapshot);

		// write individual registered keyed state metainfos
		out.writeShort(stateMetaInfoSnapshots.size());
		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> metaInfo : stateMetaInfoSnapshots) {
			KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(VERSION, metaInfo)
				.writeStateMetaInfo(out);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		final int readVersion = getReadVersion();

		if (readVersion >= 4) {
			usingKeyGroupCompression = in.readBoolean();
		} else {
			usingKeyGroupCompression = false;
		}

		if (readVersion >= 5) {
			this.keySerializerConfigSnapshot = TypeSerializerConfigSnapshotSerializationUtil
				.readSerializerConfigSnapshot(in, userCodeClassLoader);
		} else if (readVersion >= 3) {
			// versions 3 and 4 still write serializers in checkpoints
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> keySerializerAndConfig =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader).get(0);

			this.keySerializerConfigSnapshot = new BackwardsCompatibleConfigSnapshot<K>(
				keySerializerAndConfig.f1,
				(TypeSerializer<K>) keySerializerAndConfig.f0);
		} else {
			// read through the serializer
			TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true);

			// there is not config snapshot in either older versions (<= 2)
			this.keySerializerConfigSnapshot = null;
		}

		int numKvStates = in.readShort();
		stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; i++) {
			RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> snapshot = KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getReaderForVersion(getReadVersion(), userCodeClassLoader)
				.readStateMetaInfo(in);

			if (isSerializerPresenceRequired) {
				checkSerializerPresence(snapshot.getNamespaceSerializerConfigSnapshot());
				checkSerializerPresence(snapshot.getStateSerializerConfigSnapshot());
			}
			stateMetaInfoSnapshots.add(snapshot);
		}
	}

	private void checkSerializerPresence(TypeSerializerConfigSnapshot<?> snapshot) throws IOException {
		if (snapshot.restoreSerializer() instanceof UnloadableDummyTypeSerializer) {
			throw new IOException("Unable to restore keyed state, because a previous serializer" +
				" of the keyed state is not present. The serializer could have been removed from the classpath, " +
				" or its implementation have changed and could not be loaded. This is a temporary restriction that will" +
				" be fixed in future versions.");
		}
	}
}
