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
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization proxy for all meta data in keyed state backends. In the future we might also migrate the actual state
 * serialization logic here.
 */
public class KeyedBackendSerializationProxy extends VersionedIOReadableWritable {

	private static final int VERSION = 1;

	private TypeSerializer<?> keySerializer;

	private List<StateMetaInfo> namedStateSerializationProxies;

	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public KeyedBackendSerializationProxy(TypeSerializer<?> keySerializer, List<StateMetaInfo> namedStateSerializationProxies) {
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namedStateSerializationProxies = Preconditions.checkNotNull(namedStateSerializationProxies);
		Preconditions.checkArgument(namedStateSerializationProxies.size() <= Short.MAX_VALUE);
	}

	public List<StateMetaInfo> getNamedStateSerializationProxies() {
		return namedStateSerializationProxies;
	}

	public TypeSerializer<?> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public boolean isCompatibleVersion(int version) {
		return VERSION == version;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		DataOutputViewStream dos = new DataOutputViewStream(out);
		InstantiationUtil.serializeObject(dos, keySerializer);

		out.writeShort(namedStateSerializationProxies.size());
		Map<String, Integer> kVStateToId = new HashMap<>(namedStateSerializationProxies.size());

		for (StateMetaInfo kvState : namedStateSerializationProxies) {
			kvState.write(out);
			kVStateToId.put(kvState.getName(), kVStateToId.size());
		}
	}

	@Override
	public void read(DataInputView inView) throws IOException {
		super.read(inView);

		DataInputViewStream dis = new DataInputViewStream(inView);

		try {
			keySerializer = InstantiationUtil.deserializeObject(dis, userCodeClassLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}

		int numKvStates = inView.readShort();
		namedStateSerializationProxies = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; ++i) {
			StateMetaInfo stateSerializationProxy = new StateMetaInfo(userCodeClassLoader);
			stateSerializationProxy.read(inView);
			namedStateSerializationProxies.add(stateSerializationProxy);
		}
	}

//----------------------------------------------------------------------------------------------------------------------

	/**
	 * This is the serialization proxy for {@link RegisteredBackendStateMetaInfo} for a single registered state in a
	 * keyed backend.
	 */
	public static class StateMetaInfo implements IOReadableWritable {

		private String name;
		private TypeSerializer<?> namespaceSerializer;
		private TypeSerializer<?> stateSerializer;
		private ClassLoader userClassLoader;

		private StateMetaInfo(ClassLoader userClassLoader) {
			this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		}

		public StateMetaInfo(
				String name, TypeSerializer<?> namespaceSerializer, TypeSerializer<?> stateSerializer) {

			this.name = Preconditions.checkNotNull(name);
			this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
			this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public TypeSerializer<?> getNamespaceSerializer() {
			return namespaceSerializer;
		}

		public void setNamespaceSerializer(TypeSerializer<?> namespaceSerializer) {
			this.namespaceSerializer = namespaceSerializer;
		}

		public TypeSerializer<?> getStateSerializer() {
			return stateSerializer;
		}

		public void setStateSerializer(TypeSerializer<?> stateSerializer) {
			this.stateSerializer = stateSerializer;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeUTF(getName());
			DataOutputViewStream dos = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(dos, getNamespaceSerializer());
			InstantiationUtil.serializeObject(dos, getStateSerializer());
		}

		@Override
		public void read(DataInputView in) throws IOException {
			setName(in.readUTF());
			DataInputViewStream dis = new DataInputViewStream(in);
			try {
				TypeSerializer<?> namespaceSerializer = InstantiationUtil.deserializeObject(dis, userClassLoader);
				TypeSerializer<?> stateSerializer = InstantiationUtil.deserializeObject(dis, userClassLoader);
				setNamespaceSerializer(namespaceSerializer);
				setStateSerializer(stateSerializer);
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			}
		}
	}
}