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

package org.apache.flink.formats.protobuf.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistrationSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import com.esotericsoftware.kryo.Serializer;
import com.google.protobuf.Message;
import org.apache.commons.lang3.exception.CloneFailedException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer that serializes types via Protobuf.
 *
 * <p>The serializer instantiates that depending on the class of the type it should serialize.
 *
 * @param <T> The type to be serialized.
 */
public class ProtobufSerializer<T extends Message> extends TypeSerializer<T> {
	private static final long serialVersionUID = 1L;

	private Method parseFromMethod;

	private T defaultInstance;

	/**
	 * Default serializers used by Kryo for specified classes. We keep these here to rebuild KryoSerializer
	 * in case we have to migrate old data in savepoint which serialized by chill-protobuf and kryo.
	 */
	private final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultKryoUsedSerializers;

	/**
	 * The specified classes for Kryo using {@link #defaultKryoUsedSerializers}. We keep these here to rebuild
	 * KryoSerializer in case we have to migrate old data in savepoint which serialized by chill-protobuf and kryo.
	 */
	private final LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses;

	// -------- configuration fields, serializable -----------

	/** The class of the type that is serialized by this serializer.
	 */
	private final Class<T> type;

	public ProtobufSerializer(Class<T> type, ExecutionConfig executionConfig) {
		this.type = Preconditions.checkNotNull(type);

		this.defaultKryoUsedSerializers = executionConfig.getDefaultKryoSerializers();
		this.defaultKryoSerializerClasses = executionConfig.getDefaultKryoSerializerClasses();
	}

	private ProtobufSerializer(ProtobufSerializer<T> toCopy) {

		this.type = checkNotNull(toCopy.type, "Type class cannot be null.");
		this.defaultKryoSerializerClasses = toCopy.defaultKryoSerializerClasses;
		this.defaultKryoUsedSerializers = new LinkedHashMap<>(toCopy.defaultKryoUsedSerializers.size());

		// deep copy the serializer instances in defaultSerializers
		for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> entry :
			toCopy.defaultKryoUsedSerializers.entrySet()) {

			this.defaultKryoUsedSerializers.put(entry.getKey(), deepCopySerializer(entry.getValue()));
		}
	}

	private ExecutionConfig.SerializableSerializer<? extends Serializer<?>> deepCopySerializer(
		ExecutionConfig.SerializableSerializer<? extends Serializer<?>> original) {
		try {
			return InstantiationUtil.clone(original, Thread.currentThread().getContextClassLoader());
		} catch (IOException | ClassNotFoundException ex) {
			throw new CloneFailedException(
				"Could not clone serializer instance of class " + original.getClass(),
				ex);
		}
	}

	private Method getParse(Class<T> clazz) throws NoSuchMethodException {
		if (parseFromMethod == null) {
			this.parseFromMethod = clazz.getMethod("parseFrom", byte[].class);
		}
		return parseFromMethod;
	}

	private T getDefaultInstance(Class<T> clazz) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		if (defaultInstance == null) {
			this.defaultInstance = (T) clazz.getMethod("getDefaultInstance").invoke(null);
		}
		return defaultInstance;
	}

	// ------------------------------------------------------------------------

	public Class<T> getType() {
		return type;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public TypeSerializer<T> duplicate() {
		return new ProtobufSerializer<>(this);
	}

	@Override
	public T createInstance() {
		try {
			return getDefaultInstance(type);
		} catch (Exception e) {
			throw new RuntimeException("Fail to crate instance of " + type, e);
		}
	}

	@Override
	public T copy(T from) {
		return from;
	}

	@Override
	public T copy(T from, T reuse) {
		return from;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		byte[] bytes = record.toByteArray();
		target.writeInt(bytes.length);
		target.write(bytes);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		int size = source.readInt();
		byte[] bytes = new byte[size];
		source.read(bytes);
		try {
			return (T) getParse(type).invoke(null, bytes);
		} catch (Exception e) {
			throw new DeserializationException("Could not create " + type, e);
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int size = source.readInt();
		target.writeInt(size);
		target.write(source, size);
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {

		if (configSnapshot instanceof KryoRegistrationSerializerConfigSnapshot) {

			LinkedHashMap<String, KryoRegistration> kryoRegistrations =
				((KryoRegistrationSerializerConfigSnapshot) configSnapshot).getKryoRegistrations();

			// rebuild the serializer by assuring that classes which were previously registered are registered again.
			return CompatibilityResult.requiresMigration(
				new KryoSerializer<>(
					getType(),
					this.defaultKryoUsedSerializers,
					this.defaultKryoSerializerClasses,
					kryoRegistrations
				));
		}
		return CompatibilityResult.requiresMigration();

	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj.getClass() == ProtobufSerializer.class) {
			final ProtobufSerializer that = (ProtobufSerializer) obj;
			return this.type == that.type &&
				Objects.equals(defaultKryoUsedSerializers, that.defaultKryoUsedSerializers) &&
				Objects.equals(defaultKryoSerializerClasses, that.defaultKryoSerializerClasses);
		}
		else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj.getClass() == this.getClass();
	}

	@Override
	public int hashCode() {
		int result = 31 + type.hashCode();
		result = 31 * result + (defaultKryoUsedSerializers.hashCode());
		result = 31 * result + (defaultKryoSerializerClasses.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return getClass().getName() + " (" + getType().getName() + ')';
	}

	@Override
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		return new ProtobufSerializerSnapshot<>(type);
	}
}
