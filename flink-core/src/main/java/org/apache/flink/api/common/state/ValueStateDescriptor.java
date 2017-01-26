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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A {@link StateDescriptor} for {@link ValueState}. This can be used to create keyed
 * value state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getState(ValueStateDescriptor)}.
 *
 * <p>If you don't use one of the constructors that set a default value the value that you
 * get when reading a {@link ValueState} using {@link ValueState#value()} will be {@code null}.
 *
 * @param <T> The type of the values that the value state can hold.
 */
@PublicEvolving
public class ValueStateDescriptor<T> extends SimpleStateDescriptor<T, ValueState<T>> {
	private static final long serialVersionUID = 1L;

	/** The default value returned by the state when no other value is bound to a key */
	protected transient T defaultValue;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name, type, and default value.
	 * 
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ValueStateDescriptor(String, TypeInformation, Object)} constructor.
	 *
	 * @deprecated Use {@link #ValueStateDescriptor(String, Class)} instead and manually manage
	 * the default value by checking whether the contents of the state is {@code null}.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeClass The type of the values in the state.   
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	@Deprecated
	public ValueStateDescriptor(String name, Class<T> typeClass, T defaultValue) {
		super(name, typeClass);

		this.defaultValue = defaultValue;
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @deprecated Use {@link #ValueStateDescriptor(String, TypeInformation)} instead and manually
	 * manage the default value by checking whether the contents of the state is {@code null}.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	@Deprecated
	public ValueStateDescriptor(String name, TypeInformation<T> typeInfo, T defaultValue) {
		super(name, typeInfo);

		this.defaultValue = defaultValue;
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name, default value, and the specific
	 * serializer.
	 *
	 * @deprecated Use {@link #ValueStateDescriptor(String, TypeSerializer)} instead and manually
	 * manage the default value by checking whether the contents of the state is {@code null}.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	@Deprecated
	public ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer, T defaultValue) {
		super(name, typeSerializer);

		this.defaultValue = defaultValue;
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and type
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ValueStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeClass The type of the values in the state.
	 */
	public ValueStateDescriptor(String name, Class<T> typeClass) {
		super(name, typeClass);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and type.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the values in the state.
	 */
	public ValueStateDescriptor(String name, TypeInformation<T> typeInfo) {
		super(name, typeInfo);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and the specific serializer.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer) {
		super(name, typeSerializer);
	}

	// ------------------------------------------------------------------------
	//  Value State Descriptor
	// ------------------------------------------------------------------------

	@Override
	public Type getType() {
		return Type.VALUE;
	}

	@Override
	public ValueState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createValueState(this);
	}

	/**
	 * Returns the default value.
	 */
	public T getDefaultValue() {
		if (defaultValue == null) {
			return null;
		}
		else {
			return getSerializer().copy(defaultValue);
		}
	}

	// ------------------------------------------------------------------------
	//  Standard Utils
	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ValueStateDescriptor<?> that = (ValueStateDescriptor<?>) o;
		return name.equals(that.name) && simpleStateDescrEquals(that);

	}

	@Override
	public int hashCode() {
		int result = simpleStateDescrHashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ValueStateDescriptor{" +
				"name=" + name +
				", defaultValue=" + defaultValue +
				", " + simpleStateDescrToString() +
				'}';
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	private void writeObject(final ObjectOutputStream out) throws IOException {
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
				// we duplicate the type serializer here, because the serializers may be asynchronously
				// serialized into asynchronous snapshots
				// Note: as of Flink 1.2, only the serializers are written, but not the entire state
				// descriptors any more, so we may be safe do drop this?
				TypeSerializer<T> duplicateSerializer = getSerializer().duplicate();
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
				defaultValue = getSerializer().deserialize(inView);
			}
			catch (Exception e) {
				throw new IOException("Unable to deserialize default value.", e);
			}
		} else {
			defaultValue = null;
		}
	}
}
