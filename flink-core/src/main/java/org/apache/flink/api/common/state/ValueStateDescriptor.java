/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.common.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static java.util.Objects.requireNonNull;

/**
 * {@link StateDescriptor} for {@link ValueState}. This can be used to create partitioned
 * value state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getPartitionedState(StateDescriptor)}.
 *
 * @param <T> The type of the values that the value state can hold.
 */
public class ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>> {
	private static final long serialVersionUID = 1L;

	private transient T defaultValue;

	private final TypeSerializer<T> serializer;

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 * @param serializer {@link TypeSerializer} for the state values.
	 */
	public ValueStateDescriptor(String name, T defaultValue, TypeSerializer<T> serializer) {
		super(requireNonNull(name));
		this.defaultValue = defaultValue;
		this.serializer = requireNonNull(serializer);
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();

		if (defaultValue == null) {
			// we don't have a default value
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper outView =
					new DataOutputViewStreamWrapper(new DataOutputStream(baos));

			try {
				serializer.serialize(defaultValue, outView);
			} catch (IOException ioe) {
				throw new RuntimeException("Unable to serialize default value of type " +
						defaultValue.getClass().getSimpleName() + ".", ioe);
			}

			outView.close();

			out.writeInt(baos.size());
			out.write(baos.toByteArray());
		}

	}

	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		boolean hasDefaultValue = in.readBoolean();

		if (hasDefaultValue) {
			int size = in.readInt();
			byte[] buffer = new byte[size];
			int bytesRead = in.read(buffer);

			if (bytesRead != size) {
				throw new RuntimeException("Read size does not match expected size.");
			}

			ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
			DataInputViewStreamWrapper inView =
					new DataInputViewStreamWrapper(new DataInputStream(bais));
			defaultValue = serializer.deserialize(inView);
		} else {
			defaultValue = null;
		}
	}

	@Override
	public ValueState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createValueState(this);
	}

	/**
	 * Returns the default value.
	 */
	public T getDefaultValue() {
		if (defaultValue != null) {
			return serializer.copy(defaultValue);
		} else {
			return null;
		}
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 */
	public TypeSerializer<T> getSerializer() {
		return serializer;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ValueStateDescriptor<?> that = (ValueStateDescriptor<?>) o;

		return serializer.equals(that.serializer) && name.equals(that.name);

	}

	@Override
	public int hashCode() {
		int result = serializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ValueStateDescriptor{" +
				"name=" + name +
				", defaultValue=" + defaultValue +
				", serializer=" + serializer +
				'}';
	}
}
