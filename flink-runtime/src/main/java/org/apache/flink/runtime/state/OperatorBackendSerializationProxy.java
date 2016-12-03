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

import org.apache.flink.annotation.VisibleForTesting;
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
import java.util.List;

/**
 * Serialization proxy for all meta data in operator state backends. In the future we might also migrate the actual state
 * serialization logic here.
 */
public class OperatorBackendSerializationProxy extends VersionedIOReadableWritable {

	private static final int VERSION = 1;

	private List<StateMetaInfo<?>> namedStateSerializationProxies;
	private ClassLoader userCodeClassLoader;

	public OperatorBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public OperatorBackendSerializationProxy(List<StateMetaInfo<?>> namedStateSerializationProxies) {
		this.namedStateSerializationProxies = Preconditions.checkNotNull(namedStateSerializationProxies);
		Preconditions.checkArgument(namedStateSerializationProxies.size() <= Short.MAX_VALUE);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeShort(namedStateSerializationProxies.size());

		for (StateMetaInfo<?> kvState : namedStateSerializationProxies) {
			kvState.write(out);
		}
	}

	@Override
	public void read(DataInputView out) throws IOException {
		super.read(out);

		int numKvStates = out.readShort();
		namedStateSerializationProxies = new ArrayList<>(numKvStates);
		for (int i = 0; i < numKvStates; ++i) {
			StateMetaInfo<?> stateSerializationProxy = new StateMetaInfo<>(userCodeClassLoader);
			stateSerializationProxy.read(out);
			namedStateSerializationProxies.add(stateSerializationProxy);
		}
	}

	public List<StateMetaInfo<?>> getNamedStateSerializationProxies() {
		return namedStateSerializationProxies;
	}

	//----------------------------------------------------------------------------------------------------------------------

	public static class StateMetaInfo<S> implements IOReadableWritable {

		private String name;
		private TypeSerializer<S> stateSerializer;
		private OperatorStateHandle.Mode mode;

		private ClassLoader userClassLoader;

		@VisibleForTesting
		public StateMetaInfo(ClassLoader userClassLoader) {
			this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		}

		public StateMetaInfo(String name, TypeSerializer<S> stateSerializer, OperatorStateHandle.Mode mode) {
			this.name = Preconditions.checkNotNull(name);
			this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
			this.mode = Preconditions.checkNotNull(mode);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public TypeSerializer<S> getStateSerializer() {
			return stateSerializer;
		}

		public void setStateSerializer(TypeSerializer<S> stateSerializer) {
			this.stateSerializer = stateSerializer;
		}

		public OperatorStateHandle.Mode getMode() {
			return mode;
		}

		public void setMode(OperatorStateHandle.Mode mode) {
			this.mode = mode;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeUTF(getName());
			out.writeByte(getMode().ordinal());
			DataOutputViewStream dos = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(dos, getStateSerializer());
		}

		@Override
		public void read(DataInputView in) throws IOException {
			setName(in.readUTF());
			setMode(OperatorStateHandle.Mode.values()[in.readByte()]);
			DataInputViewStream dis = new DataInputViewStream(in);
			try {
				TypeSerializer<S> stateSerializer = InstantiationUtil.deserializeObject(dis, userClassLoader);
				setStateSerializer(stateSerializer);
			} catch (ClassNotFoundException exception) {
				throw new IOException(exception);
			}
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			StateMetaInfo<?> metaInfo = (StateMetaInfo<?>) o;

			if (!getName().equals(metaInfo.getName())) {
				return false;
			}

			if (!getStateSerializer().equals(metaInfo.getStateSerializer())) {
				return false;
			}

			return getMode() == metaInfo.getMode();
		}

		@Override
		public int hashCode() {
			int result = getName().hashCode();
			result = 31 * result + getStateSerializer().hashCode();
			result = 31 * result + getMode().hashCode();
			return result;
		}
	}
}
