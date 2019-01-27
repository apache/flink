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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Serialization proxy for all meta data in internal state backends. In the future we might also requiresMigration the actual state
 * serialization logic here.
 */
public class InternalBackendSerializationProxy extends VersionedIOReadableWritable {

	public static final int VERSION = 1;

	//TODO allow for more (user defined) compression formats + backward compatibility story.
	/** This specifies if we use a compressed format write the key-groups */
	private boolean usingKeyGroupCompression;

	/** This specifies whether or not to use dummy {@link UnloadableDummyTypeSerializer} when serializers cannot be read. */
	private boolean isSerializerPresenceRequired;

	private List<StateMetaInfoSnapshot> keyedStateMetaInfoSnapshots;

	private List<StateMetaInfoSnapshot> subKeyedStateMetaInfoSnapshots;

	private ClassLoader userCodeClassLoader;

	public InternalBackendSerializationProxy(ClassLoader userCodeClassLoader, boolean isSerializerPresenceRequired) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		this.isSerializerPresenceRequired = isSerializerPresenceRequired;
	}

	public InternalBackendSerializationProxy(
		List<StateMetaInfoSnapshot> keyedStateMetaSnapshots,
		List<StateMetaInfoSnapshot> subKeyedStateMetaSnapshots,
		boolean compression) {

		this.keyedStateMetaInfoSnapshots = Preconditions.checkNotNull(keyedStateMetaSnapshots);
		this.subKeyedStateMetaInfoSnapshots = Preconditions.checkNotNull(subKeyedStateMetaSnapshots);
		this.usingKeyGroupCompression = compression;
	}

	public List<StateMetaInfoSnapshot> getKeyedStateMetaSnapshots() {
		return keyedStateMetaInfoSnapshots;
	}

	public List<StateMetaInfoSnapshot> getSubKeyedStateMetaSnapshots() {
		return subKeyedStateMetaInfoSnapshots;
	}

	public boolean isUsingKeyGroupCompression() {
		return usingKeyGroupCompression;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		// write the compression format used to write each key-group
		out.writeBoolean(usingKeyGroupCompression);

		writeStateMetaInfoSnapshot(out, keyedStateMetaInfoSnapshots, true);
		writeStateMetaInfoSnapshot(out, subKeyedStateMetaInfoSnapshots, false);
	}

	private void writeStateMetaInfoSnapshot(DataOutputView out, List<StateMetaInfoSnapshot> stateMetaInfoSnapshots, boolean isKeyedState) throws IOException {
		out.writeShort(stateMetaInfoSnapshots.size());
		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			out.writeInt(stateMetaInfoSnapshot.getStateType().ordinal());
			out.writeUTF(stateMetaInfoSnapshot.getName());

			if (isKeyedState) {
				TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
					out,
					Arrays.asList(
						new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
							stateMetaInfoSnapshot.getKeySerializer(), stateMetaInfoSnapshot.getKeySerializerConfigSnapshot()),
						new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
							stateMetaInfoSnapshot.getValueSerializer(), stateMetaInfoSnapshot.getValueSerializerConfigSnapshot())));
			} else {
				TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
					out,
					Arrays.asList(
						new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
							stateMetaInfoSnapshot.getKeySerializer(), stateMetaInfoSnapshot.getKeySerializerConfigSnapshot()),
						new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
							stateMetaInfoSnapshot.getValueSerializer(), stateMetaInfoSnapshot.getValueSerializerConfigSnapshot()),
						new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
							stateMetaInfoSnapshot.getNamespaceSerializer(), stateMetaInfoSnapshot.getNamespaceSerializerConfigSnapshot())));
			}
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		usingKeyGroupCompression = in.readBoolean();

		keyedStateMetaInfoSnapshots = readStateMetaInfoSnapshot(in, true);
		subKeyedStateMetaInfoSnapshots = readStateMetaInfoSnapshot(in, false);
	}

	private List<StateMetaInfoSnapshot> readStateMetaInfoSnapshot(DataInputView in, boolean isKeyedState) throws IOException {
		short stateSize = in.readShort();
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();
		Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> keySerializerInfo;
		Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> valueSerializerInfo;
		Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> namespaceSerializerInfo;

		for (int i = 0; i < stateSize; i++) {
			InternalStateType stateType = InternalStateType.values()[in.readInt()];
			String stateName = in.readUTF();

			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader);
			if (isKeyedState) {
				Preconditions.checkState(serializersAndConfigs.size() == 2,
					"Expected serialized serializer and config size as 2 for keyed state");
				Preconditions.checkState(stateType.isKeyedState(), "Expected keyed state here.");

				keySerializerInfo = serializersAndConfigs.get(0);
				valueSerializerInfo = serializersAndConfigs.get(1);
				namespaceSerializerInfo = null;
				stateMetaInfoSnapshots.add(new StateMetaInfoSnapshot(stateType,
					stateName,
					keySerializerInfo.f0,
					valueSerializerInfo.f0,
					VoidNamespaceSerializer.INSTANCE,
					keySerializerInfo.f1,
					valueSerializerInfo.f1,
					VoidNamespaceSerializer.SNAPSHOT));
			} else {
				Preconditions.checkState(serializersAndConfigs.size() == 3,
					"Expected serialized serializer and config size as 3 for subKeyed state.");
				Preconditions.checkState(!stateType.isKeyedState(), "Expected subKeyed state here.");

				keySerializerInfo = serializersAndConfigs.get(0);
				valueSerializerInfo = serializersAndConfigs.get(1);
				namespaceSerializerInfo = serializersAndConfigs.get(2);

				stateMetaInfoSnapshots.add(new StateMetaInfoSnapshot(stateType,
					stateName,
					keySerializerInfo.f0,
					valueSerializerInfo.f0,
					namespaceSerializerInfo.f0,
					keySerializerInfo.f1,
					valueSerializerInfo.f1,
					namespaceSerializerInfo.f1));
			}

			if (isSerializerPresenceRequired) {
				checkSerializerPresence(keySerializerInfo.f0);
				checkSerializerPresence(valueSerializerInfo.f0);
				if (namespaceSerializerInfo != null) {
					checkSerializerPresence(namespaceSerializerInfo.f0);
				}
			}
		}
		return stateMetaInfoSnapshots;
	}

	@Override
	public int[] getCompatibleVersions() {
		return new int[] {VERSION};
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
