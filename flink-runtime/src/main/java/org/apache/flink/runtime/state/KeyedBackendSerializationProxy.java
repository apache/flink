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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyedBackendSerializationProxy extends VersionedIOReadableWritable {

	private static final long VERSION = 1L;

	private TypeSerializer<?> keySerializer;
	private List<MetaData> namedStateSerializationProxies;
	private ClassLoader userCodeClassLoader;

	@Override
	public long getVersion() {
		return VERSION;
	}

	@Override
	public boolean isCompatibleVersion(long version) {
		return VERSION == version;
	}

	static class MetaData implements IOReadableWritable {

		String name;
		TypeSerializer<?> namespaceSerializer;
		TypeSerializer<?> stateSerializer;
		ClassLoader userClassLoader;

		public MetaData(ClassLoader userClassLoader) {
			this.userClassLoader = userClassLoader;
		}

		public MetaData(
				String name, TypeSerializer<?> namespaceSerializer, TypeSerializer<?> stateSerializer) {

			this.name = name;
			this.namespaceSerializer = namespaceSerializer;
			this.stateSerializer = stateSerializer;
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

	@Override
	public void write(DataOutputView out) throws IOException {
		Preconditions.checkArgument(namedStateSerializationProxies.size() < Short.MAX_VALUE);
		super.write(out);

		DataOutputViewStream dos = new DataOutputViewStream(out);
		InstantiationUtil.serializeObject(dos, keySerializer);

		out.writeShort(namedStateSerializationProxies.size());
		Map<String, Integer> kVStateToId = new HashMap<>(namedStateSerializationProxies.size());

		for (MetaData kvState : namedStateSerializationProxies) {
			kvState.write(out);
			kVStateToId.put(kvState.getName(), kVStateToId.size());
		}
	}

	@Override
	public void read(DataInputView inView) throws IOException {
		super.read(inView);

		int numKvStates = inView.readShort();

		for (int i = 0; i < numKvStates; ++i) {
			MetaData stateSerializationProxy = new MetaData(userCodeClassLoader);
			stateSerializationProxy.read(inView);
			namedStateSerializationProxies.add(stateSerializationProxy);
		}
	}
}