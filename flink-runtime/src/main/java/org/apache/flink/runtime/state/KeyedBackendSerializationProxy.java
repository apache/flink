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
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
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

	private TypeSerializerSerializationProxy<?> keySerializerProxy;
	private List<StateMetaInfo<?, ?>> namedStateSerializationProxies;

	private ClassLoader userCodeClassLoader;

	public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public KeyedBackendSerializationProxy(TypeSerializer<?> keySerializer, List<StateMetaInfo<?, ?>> namedStateSerializationProxies) {
		this.keySerializerProxy = new TypeSerializerSerializationProxy<>(Preconditions.checkNotNull(keySerializer));
		this.namedStateSerializationProxies = Preconditions.checkNotNull(namedStateSerializationProxies);
		Preconditions.checkArgument(namedStateSerializationProxies.size() <= Short.MAX_VALUE);
	}

	public List<StateMetaInfo<?, ?>> getNamedStateSerializationProxies() {
		return namedStateSerializationProxies;
	}

	public TypeSerializerSerializationProxy<?> getKeySerializerProxy() {
		return keySerializerProxy;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		keySerializerProxy.write(out);

		out.writeShort(namedStateSerializationProxies.size());
		Map<String, Integer> kVStateToId = new HashMap<>(namedStateSerializationProxies.size());

		for (StateMetaInfo<?, ?> kvState : namedStateSerializationProxies) {
			kvState.write(out);
			kVStateToId.put(kvState.getName(), kVStateToId.size());
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		keySerializerProxy = new TypeSerializerSerializationProxy<>(userCodeClassLoader);
		keySerializerProxy.read(in);

		int numKvStates = in.readShort();
		namedStateSerializationProxies = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; ++i) {
			StateMetaInfo<?, ?> stateSerializationProxy = new StateMetaInfo<>(userCodeClassLoader);
			stateSerializationProxy.read(in);
			namedStateSerializationProxies.add(stateSerializationProxy);
		}
	}

//----------------------------------------------------------------------------------------------------------------------

	/**
	 * This is the serialization proxy for {@link RegisteredBackendStateMetaInfo} for a single registered state in a
	 * keyed backend.
	 */
	public static class StateMetaInfo<N, S> implements IOReadableWritable {

		private String name;
		private TypeSerializerSerializationProxy<N> namespaceSerializerSerializationProxy;
		private TypeSerializerSerializationProxy<S> stateSerializerSerializationProxy;

		private ClassLoader userClassLoader;

		private StateMetaInfo(ClassLoader userClassLoader) {
			this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		}

		public StateMetaInfo(
				String name, TypeSerializer<N> namespaceSerializer, TypeSerializer<S> stateSerializer) {

			this.name = Preconditions.checkNotNull(name);
			this.namespaceSerializerSerializationProxy = new TypeSerializerSerializationProxy<>(Preconditions.checkNotNull(namespaceSerializer));
			this.stateSerializerSerializationProxy = new TypeSerializerSerializationProxy<>(Preconditions.checkNotNull(stateSerializer));
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public TypeSerializerSerializationProxy<N> getNamespaceSerializerSerializationProxy() {
			return namespaceSerializerSerializationProxy;
		}

		public void setNamespaceSerializerSerializationProxy(TypeSerializerSerializationProxy<N> namespaceSerializerSerializationProxy) {
			this.namespaceSerializerSerializationProxy = namespaceSerializerSerializationProxy;
		}

		public TypeSerializerSerializationProxy<S> getStateSerializerSerializationProxy() {
			return stateSerializerSerializationProxy;
		}

		public void setStateSerializerSerializationProxy(TypeSerializerSerializationProxy<S> stateSerializerSerializationProxy) {
			this.stateSerializerSerializationProxy = stateSerializerSerializationProxy;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeUTF(getName());

			getNamespaceSerializerSerializationProxy().write(out);
			getStateSerializerSerializationProxy().write(out);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			setName(in.readUTF());

			namespaceSerializerSerializationProxy = new TypeSerializerSerializationProxy<>(userClassLoader);
			namespaceSerializerSerializationProxy.read(in);

			stateSerializerSerializationProxy = new TypeSerializerSerializationProxy<>(userClassLoader);
			stateSerializerSerializationProxy.read(in);
		}
	}
}