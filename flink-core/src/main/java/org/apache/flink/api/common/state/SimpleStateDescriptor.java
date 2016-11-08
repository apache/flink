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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Base class for the descriptors of simple states. A {@code SimpleStateDescriptor} is used
 * for creating partitioned simple states whose values are not composited.
 *
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <T> The type of the value in the state.
 * @param <S> The type of the created state.
 */
@PublicEvolving
public abstract class SimpleStateDescriptor<T, S extends State<T>> extends StateDescriptor<S> {
	private static final long serialVersionUID = 1L;

	/** The serializer for the type. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	protected TypeSerializer<T> typeSerializer;

	/** The type information describing the value type. Only used to lazily create the serializer
	 * and dropped during serialization */
	private transient TypeInformation<T> typeInfo;

	/** The default value returned by the state when no other value is bound to a key */
	protected transient T defaultValue;

	// ------------------------------------------------------------------------

	/**
	 * Create a new {@code SimpleStateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code SimpleStateDescriptor}.
	 * @param serializer The type serializer for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting a value before.
	 */
	protected SimpleStateDescriptor(String name, TypeSerializer<T> serializer, T defaultValue) {
		super(name);
		this.typeSerializer = requireNonNull(serializer, "serializer must not be null");
		this.defaultValue = defaultValue;
	}

	/**
	 * Create a new {@code SimpleStateDescriptor} with the given name and the given type information.
	 *
	 * @param name The name of the {@code SimpleStateDescriptor}.
	 * @param typeInfo The type information for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting a value before.
	 */
	protected SimpleStateDescriptor(String name, TypeInformation<T> typeInfo, T defaultValue) {
		super(name);
		this.typeInfo = requireNonNull(typeInfo, "type information must not be null");
		this.defaultValue = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #SimpleStateDescriptor(String, TypeInformation, Object)} constructor.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param type The class of the type of values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	protected SimpleStateDescriptor(String name, Class<T> type, T defaultValue) {
		super(name);

		requireNonNull(type, "type class must not be null");

		try {
			this.typeInfo = TypeExtractor.createTypeInfo(type);
		} catch (Exception e) {
			throw new RuntimeException("Cannot create full type information based on the given class. If the type has generics, please", e);
		}

		this.defaultValue = defaultValue;
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the default value.
	 */
	public T getDefaultValue() {
		if (defaultValue != null) {
			if (typeSerializer != null) {
				return typeSerializer.copy(defaultValue);
			} else {
				throw new IllegalStateException("Serializer not yet initialized.");
			}
		} else {
			return null;
		}
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 * Note that the serializer may initialized lazily and is only guaranteed to exist after
	 * calling {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 */
	public TypeSerializer<T> getSerializer() {
		if (typeSerializer != null) {
			return typeSerializer;
		} else {
			throw new IllegalStateException("Serializer not yet initialized.");
		}
	}

	// ------------------------------------------------------------------------
	@Override
	public boolean isSerializerInitialized() {
		return typeSerializer != null;
	}

	@Override
	public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
		if (typeSerializer == null) {
			if (typeInfo != null) {
				typeSerializer = typeInfo.createSerializer(executionConfig);
			} else {
				throw new IllegalStateException(
					"Cannot initialize serializer after TypeInformation was dropped during serialization");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	private void writeObject(final ObjectOutputStream out) throws IOException {
		// make sure we have a serializer before the type information gets lost
		initializeSerializerUnlessSet(new ExecutionConfig());

		// write all the non-transient fields
		out.defaultWriteObject();

		// write the non-serializable default value field
		if (defaultValue == null) {
			// we don't have a default value
			out.writeBoolean(false);
		} else {
			// we have a default value
			out.writeBoolean(true);

			byte[] serializedDefaultValue;
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos))
			{
				TypeSerializer<T> duplicateSerializer = typeSerializer.duplicate();
				duplicateSerializer.serialize(defaultValue, outView);

				outView.flush();
				serializedDefaultValue = baos.toByteArray();
			}
			catch (Exception e) {
				throw new IOException("Unable to serialize default value of type " +
					defaultValue.getClass().getSimpleName() + ".", e);
			}

			out.writeInt(serializedDefaultValue.length);
			out.write(serializedDefaultValue);
		}
	}

	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		// read the non-transient fields
		in.defaultReadObject();

		// read the default value field
		boolean hasDefaultValue = in.readBoolean();
		if (hasDefaultValue) {
			int size = in.readInt();

			byte[] buffer = new byte[size];

			in.readFully(buffer);

			try (ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais))
			{
				defaultValue = typeSerializer.deserialize(inView);
			}
			catch (Exception e) {
				throw new IOException("Unable to deserialize default value.", e);
			}
		} else {
			defaultValue = null;
		}
	}
}
