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
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;

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

	private TypeSerializer<K> keySerializer;
	private TypeSerializerConfigSnapshot keySerializerConfigSnapshot;

	private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader, boolean isSerializerPresenceRequired) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		this.isSerializerPresenceRequired = isSerializerPresenceRequired;
	}

	public KeyedBackendSerializationProxy(
			TypeSerializer<K> keySerializer,
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
			boolean compression) {

		this.usingKeyGroupCompression = compression;

		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializer.snapshotConfiguration());

		Preconditions.checkNotNull(stateMetaInfoSnapshots);
		Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
		this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
	}

	public List<StateMetaInfoSnapshot> getStateMetaInfoSnapshots() {
		return stateMetaInfoSnapshots;
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
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
		return new int[]{VERSION, 4, 3, 2, 1};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		// write the compression format used to write each key-group
		out.writeBoolean(usingKeyGroupCompression);

		// write in a way to be fault tolerant of read failures when deserializing the key serializer
		TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
				out,
				Collections.singletonList(new Tuple2<>(keySerializer, keySerializerConfigSnapshot)));

		// write individual registered keyed state metainfos
		out.writeShort(stateMetaInfoSnapshots.size());
		for (StateMetaInfoSnapshot metaInfoSnapshot : stateMetaInfoSnapshots) {
			StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(metaInfoSnapshot, out);
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

		// only starting from version 3, we have the key serializer and its config snapshot written
		if (readVersion >= 3) {
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> keySerializerAndConfig =
					TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader).get(0);
			this.keySerializer = (TypeSerializer<K>) keySerializerAndConfig.f0;
			this.keySerializerConfigSnapshot = keySerializerAndConfig.f1;
		} else {
			this.keySerializer = TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true);
			this.keySerializerConfigSnapshot = null;
		}

		if (isSerializerPresenceRequired) {
			checkSerializerPresence(keySerializer);
		}

		int metaInfoVersion = readVersion > 4 ? CURRENT_STATE_META_INFO_SNAPSHOT_VERSION : readVersion;
		final StateMetaInfoReader stateMetaInfoReader = StateMetaInfoSnapshotReadersWriters.getReader(
			metaInfoVersion,
			StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);

		int numKvStates = in.readShort();
		stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; i++) {
			StateMetaInfoSnapshot snapshot = stateMetaInfoReader.readStateMetaInfoSnapshot(in, userCodeClassLoader);

			if (isSerializerPresenceRequired) {
				checkSerializerPresence(
					snapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER));
				checkSerializerPresence(
					snapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
			}
			stateMetaInfoSnapshots.add(snapshot);
		}
	}

	private void checkSerializerPresence(TypeSerializer<?> serializer) throws IOException {
		if (serializer instanceof UnloadableDummyTypeSerializer) {
			throw new IOException("Unable to restore keyed state, because a previous serializer" +
				" of the keyed state is not present The serializer could have been removed from the classpath, " +
				" or its implementation have changed and could not be loaded. This is a temporary restriction that will" +
				" be fixed in future versions.");
		}
	}
}
