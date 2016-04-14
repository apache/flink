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
import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * {@link State} in stateful operations. This contains the name and can create an actual state
 * object given a {@link StateBackend} using {@link #bind(StateBackend)}.
 *
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 */
@PublicEvolving
public abstract class StateDescriptor<S extends State, T> implements Serializable {
	private static final long serialVersionUID = 1L;

	/** Name that uniquely identifies state created from this StateDescriptor. */
	private final String name;

	/** The serializer for the type. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	private TypeSerializer<T> serializer;

	/** The default value returned by the state when no other value is bound to a key */
	private transient T defaultValue;

	/** The type information describing the value type. Only used to lazily create the serializer
	 * and dropped during serialization */
	private transient TypeInformation<T> typeInfo;

	// ------------------------------------------------------------------------

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param serializer The type serializer for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	protected StateDescriptor(String name, TypeSerializer<T> serializer, T defaultValue) {
		this.name = requireNonNull(name, "name must not be null");
		this.serializer = requireNonNull(serializer, "serializer must not be null");
		this.defaultValue = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param typeInfo The type information for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.   
	 */
	protected StateDescriptor(String name, TypeInformation<T> typeInfo, T defaultValue) {
		this.name = requireNonNull(name, "name must not be null");
		this.typeInfo = requireNonNull(typeInfo, "type information must not be null");
		this.defaultValue = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #StateDescriptor(String, TypeInformation, Object)} constructor.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param type The class of the type of values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.   
	 */
	protected StateDescriptor(String name, Class<T> type, T defaultValue) {
		this.name = requireNonNull(name, "name must not be null");
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
	 * Returns the name of this {@code StateDescriptor}.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the default value.
	 */
	public T getDefaultValue() {
		if (defaultValue != null) {
			if (serializer != null) {
				return serializer.copy(defaultValue);
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
		if (serializer != null) {
			return serializer;
		} else {
			throw new IllegalStateException("Serializer not yet initialized.");
		}
	}

	/**
	 * Creates a new {@link State} on the given {@link StateBackend}.
	 *
	 * @param stateBackend The {@code StateBackend} on which to create the {@link State}.
	 */
	public abstract S bind(StateBackend stateBackend) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * Checks whether the serializer has been initialized. Serializer initialization is lazy,
	 * to allow parametrization of serializers with an {@link ExecutionConfig} via
	 * {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 *
	 * @return True if the serializers have been initialized, false otherwise.
	 */
	public boolean isSerializerInitialized() {
		return serializer != null;
	}

	/**
	 * Initializes the serializer, unless it has been initialized before.
	 *
	 * @param executionConfig The execution config to use when creating the serializer.
	 */
	public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
		if (serializer == null) {
			if (typeInfo != null) {
				serializer = typeInfo.createSerializer(executionConfig);
			} else {
				throw new IllegalStateException(
						"Cannot initialize serializer after TypeInformation was dropped during serialization");
			}
		}
	}

	/**
	 * This method should be called by subclasses prior to serialization. Because the TypeInformation is
	 * not always serializable, it is 'transient' and dropped during serialization. Hence, the descriptor
	 * needs to make sure that the serializer is created before the TypeInformation is dropped. 
	 */
	private void ensureSerializerCreated() {
		if (serializer == null) {
			if (typeInfo != null) {
				serializer = typeInfo.createSerializer(new ExecutionConfig());
			} else {
				throw new IllegalStateException(
						"Cannot initialize serializer after TypeInformation was dropped during serialization");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Standard Utils
	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return name.hashCode() + 41;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o == null || getClass() != o.getClass()) {
			return false;
		}
		else {
			StateDescriptor<?, ?> that = (StateDescriptor<?, ?>) o;
			return this.name.equals(that.name);
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() +
				"{name=" + name +
				", defaultValue=" + defaultValue +
				", serializer=" + serializer +
				'}';
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	private void writeObject(final ObjectOutputStream out) throws IOException {
		// make sure we have a serializer before the type information gets lost
		ensureSerializerCreated();

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
				TypeSerializer<T> duplicateSerializer = serializer.duplicate();
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
				defaultValue = serializer.deserialize(inView);
			}
			catch (Exception e) {
				throw new IOException("Unable to deserialize default value.", e);
			}
		} else {
			defaultValue = null;
		}
	}
}
