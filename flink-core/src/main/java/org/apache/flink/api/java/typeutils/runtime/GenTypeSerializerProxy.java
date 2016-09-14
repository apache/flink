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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;

/**
 * This class wraps a generated serializer. It contains both an instance of the serializer and its source code.
 * The instance of the wrapped serializer is not serialized. During deserialization the source code is compiled.
 * Note that, the compilation is cached inside InstantiationUtil.
 *
 * The reason why the wrapped serializer is not serialized is that, it should be possible to send this class in a
 * serialized form to a JVM which do not have the class of the wrapped object compiled yet.
 */
@Internal
public class GenTypeSerializerProxy<T> extends TypeSerializer<T> {
	private final String code;
	private final String name;
	private final Class<T> clazz;
	private final TypeSerializer<?>[] fieldSerializers;
	private final ExecutionConfig config;

	transient private TypeSerializer<T> impl = null;

	private void compile() {
		try {
			assert impl == null;
			Class<?> serializerClazz = InstantiationUtil.compile(clazz.getClassLoader(), name, code);
			Constructor<?>[] ctors = serializerClazz.getConstructors();
			assert ctors.length == 1;
			impl = (TypeSerializer<T>) ctors[0].newInstance(clazz, fieldSerializers, config);
		} catch (Exception e) {
			throw new RuntimeException("Unable to generate serializer: " + name, e);
		}
	}

	public GenTypeSerializerProxy(Class<T> clazz, String name, String code, TypeSerializer<?>[] fields,
									ExecutionConfig config) {
		this.name = name;
		this.code = code;
		this.clazz = clazz;
		this.config = config;
		this.fieldSerializers = fields;
		compile();
	}

	private GenTypeSerializerProxy(GenTypeSerializerProxy<T> other) {
		this.name = other.name;
		this.code = other.code;
		this.clazz = other.clazz;
		this.config = other.config;
		this.fieldSerializers = new TypeSerializer[other.fieldSerializers.length];
		for (int i = 0; i < this.fieldSerializers.length; i++) {
			this.fieldSerializers[i] = other.fieldSerializers[i].duplicate();
		}
		this.impl = other.impl.duplicate();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		compile();
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public GenTypeSerializerProxy<T> duplicate() {
		return new GenTypeSerializerProxy<>(this);
	}

	@Override
	public T createInstance() {
		return impl.createInstance();
	}

	@Override
	public T copy(T from) {
		return impl.copy(from);
	}

	@Override
	public T copy(T from, T reuse) {
		return impl.copy(from, reuse);
	}

	@Override
	public int getLength() {
		return impl.getLength();
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		impl.serialize(value, target);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		return impl.deserialize(source);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return impl.deserialize(reuse, source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		impl.copy(source, target);
	}

	@Override
	public int hashCode() {
		return impl.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GenTypeSerializerProxy) {
			return impl.equals(((GenTypeSerializerProxy) obj).impl);
		}
		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof GenTypeSerializerProxy;
	}
}
