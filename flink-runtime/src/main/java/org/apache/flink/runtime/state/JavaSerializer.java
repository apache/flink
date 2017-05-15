/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

@Internal
final class JavaSerializer<T extends Serializable> extends TypeSerializerSingleton<T> {

	private static final long serialVersionUID = 5067491650263321234L;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public T createInstance() {
		return null;
	}

	@Override
	public T copy(T from) {
		try {
			return InstantiationUtil.clone(from, Thread.currentThread().getContextClassLoader());
		} catch (IOException | ClassNotFoundException e) {
			throw new FlinkRuntimeException("Could not copy element via serialization: " + from, e);
		}
	}

	@Override
	public T copy(T from, T reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		InstantiationUtil.serializeObject(new DataOutputViewStream(target), record);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		try {
			return InstantiationUtil.deserializeObject(
					new DataInputViewStream(source),
					Thread.currentThread().getContextClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not deserialize object.", e);
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		T tmp = deserialize(source);
		serialize(tmp, target);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof JavaSerializer;
	}
}
