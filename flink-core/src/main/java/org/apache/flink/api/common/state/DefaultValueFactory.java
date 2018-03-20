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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @deprecated Used only to support the deprecated feature where specific default values
 * (rather than default value suppliers) are passed to the state descriptors.
 */
@Deprecated
@Internal
class DefaultValueFactory<T> implements SerializableSupplier<T> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	private final byte[] serializedDefaultValue;

	private transient T defaultValue;

	private DefaultValueFactory(TypeSerializer<T> serializer, byte[] serializedDefaultValue, T defaultValue) {
		this.serializer = serializer;
		this.serializedDefaultValue = serializedDefaultValue;
		this.defaultValue = defaultValue;
	}

	@Override
	public T get() {
		return serializer.copy(defaultValue);
	}

	// ------------------------------------------------------------------------

	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		// read the non-transient fields
		in.defaultReadObject();

		// populate the defaultValue
		DataInputDeserializer deser = new DataInputDeserializer(serializedDefaultValue);
		defaultValue = serializer.deserialize(deser);
	}

	// ------------------------------------------------------------------------

	@Nullable
	static <T> DefaultValueFactory<T> create(TypeSerializer<T> serializer, @Nullable T value) {
		checkNotNull(serializer, "serializer must not be null");

		if (value == null) {
			return null;
		}

		byte[] serializedValue;
		try {
			DataOutputSerializer out = new DataOutputSerializer(32);
			serializer.serialize(value, out);
			serializedValue = out.getCopyOfBuffer();
		}
		catch (IOException e) {
			throw new FlinkRuntimeException(e.getMessage(), e);
		}

		return new DefaultValueFactory<>(serializer, serializedValue, value);
	}

	@Nullable
	static <T> DefaultValueFactory<T> create(TypeInformation<T> type, @Nullable T value) {
		checkNotNull(type, "type information must not be null");

		if (value == null) {
			return null;
		}

		// !!! WARNING !!!
		//
		// We create the DefaultValueFactory's serializer here without proper ExecutionConfig!
		// That is against the general contract and is a pattern that should in no way be imitated
		// in other parts of the code. It is okay in this specific case here only, because the
		// DefaultValueFactory stores no data in state, but only needs the serializer to clone records!
		//
		// Because the factory still serializes the default value for distribution, this can still
		// fail, for example due to missing default serializers in Kryo.
		// We tolerate that here and only here because this whole class is only used to support
		// a deprecated feature.

		return create(type.createSerializer(new ExecutionConfig()), value);
	}

	@Nullable
	static <T> DefaultValueFactory<T> create(Class<T> type, @Nullable T value) {
		checkNotNull(type, "type class must not be null");

		if (value == null) {
			return null;
		}

		TypeInformation<T> typeInfo;
		try {
			typeInfo = TypeExtractor.createTypeInfo(type);
		}
		catch (Exception e) {
			throw new FlinkRuntimeException(
					"Could not create the type information for '" + type.getName() + "'. " +
					"The most common reason is failure to infer the generic type information, due to Java's type erasure. " +
					"In that case, please pass a 'TypeHint' instead of a class to describe the type. " +
					"For example, to describe 'Tuple2<String, String>' as a generic type, use " +
					"'new XStateDescriptor<>(name, TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));'", e);
		}

		return create(typeInfo, value);
	}
}
