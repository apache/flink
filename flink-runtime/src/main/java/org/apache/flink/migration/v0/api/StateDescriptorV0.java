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

package org.apache.flink.migration.v0.api;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
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
 * The descriptor for {@link State} in SavepointV0.
 */
@Deprecated
@SuppressWarnings("deprecation")
public abstract class StateDescriptorV0<S extends State, T> implements Serializable {
	private static final long serialVersionUID = 1L;

	/** Name that uniquely identifies state created from this StateDescriptor. */
	protected final String name;

	/** The serializer for the type. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	protected TypeSerializer<T> serializer;

	/** The default value returned by the state when no other value is bound to a key */
	protected transient T defaultValue;

	// ------------------------------------------------------------------------

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param serializer The type serializer for the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	protected StateDescriptorV0(String name, TypeSerializer<T> serializer, T defaultValue) {
		this.name = requireNonNull(name, "name must not be null");
		this.serializer = requireNonNull(serializer, "serializer must not be null");
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
		return defaultValue;
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 */
	public TypeSerializer<T> getSerializer() {
		return serializer;
	}

	public abstract StateDescriptor<S, ?> convert();

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object o);

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
