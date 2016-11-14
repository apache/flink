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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.IdentityHashMap;

/**
 * Type serializer which keeps track of the serialized objects so that each object is only
 * serialized once. If the same object shall be serialized again, then a reference handle is
 * written instead.
 *
 * Avoiding duplication is achieved by keeping an internal identity hash map. This map contains
 * all serialized objects. To make the serializer work it is important that the same serializer
 * is used for a coherent serialization run. After the serialization has stopped, the identity
 * hash map should be cleared.
 *
 * @param <T> Type of the element to be serialized
 */
public class NonDuplicatingTypeSerializer<T> extends TypeSerializer<T> {
	private static final long serialVersionUID = -7633631762221447524L;

	// underlying type serializer
	private final TypeSerializer<T> typeSerializer;

	// here we store the already serialized objects
	private transient IdentityHashMap<T, Integer> identityMap;

	// here we store the already deserialized objects
	private transient ArrayList<T> elementList;

	public NonDuplicatingTypeSerializer(final TypeSerializer<T> typeSerializer) {
		this.typeSerializer = typeSerializer;

		this.identityMap = new IdentityHashMap<>();
		this.elementList = new ArrayList<>();
	}

	public TypeSerializer<T> getTypeSerializer() {
		return typeSerializer;
	}

	/**
	 * Clears the data structures containing the already serialized/deserialized objects. This
	 * effectively resets the type serializer.
	 */
	public void clearReferences() {
		identityMap.clear();
		elementList.clear();
	}

	@Override
	public boolean isImmutableType() {
		return typeSerializer.isImmutableType();
	}

	@Override
	public TypeSerializer<T> duplicate() {
		return new NonDuplicatingTypeSerializer<>(typeSerializer);
	}

	@Override
	public T createInstance() {
		return typeSerializer.createInstance();
	}

	@Override
	public T copy(T from) {
		return typeSerializer.copy(from);
	}

	@Override
	public T copy(T from, T reuse) {
		return typeSerializer.copy(from, reuse);
	}

	@Override
	public int getLength() {
		return typeSerializer.getLength();
	}

	/**
	 * Serializes the given record.
	 * <p>
	 * First a boolean indicating whether a reference handle (true) or the object (false) is
	 * written. Then, either the reference handle or the object is written.
	 *
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 *
	 * @throws IOException
	 */
	public void serialize(T record, DataOutputView target) throws IOException {
		if (identityMap.containsKey(record)) {
			target.writeBoolean(true);
			target.writeInt(identityMap.get(record));
		} else {
			target.writeBoolean(false);
			typeSerializer.serialize(record, target);
		}
	}

	/**
	 * Deserializes an object from the input view.
	 * <p>
	 * First it reads a boolean indicating whether a reference handle or a serialized object
	 * follows.
	 *
	 * @param source The input view from which to read the data.
	 * @return The deserialized object
	 * @throws IOException
	 */
	public T deserialize(DataInputView source) throws IOException {
		boolean alreadyRead = source.readBoolean();

		if (alreadyRead) {
			int index = source.readInt();
			return elementList.get(index);
		} else {
			T element = typeSerializer.deserialize(source);
			elementList.add(element);

			return element;
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		boolean alreadyRead = source.readBoolean();

		if (alreadyRead) {
			int index = source.readInt();
			typeSerializer.serialize(elementList.get(index), target);
		} else {
			T element = typeSerializer.deserialize(source);
			typeSerializer.serialize(element, target);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NonDuplicatingTypeSerializer) {
			@SuppressWarnings("unchecked")
			NonDuplicatingTypeSerializer<T> other = (NonDuplicatingTypeSerializer<T>)obj;

			return (other.canEqual(this) && typeSerializer.equals(other.typeSerializer));
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof NonDuplicatingTypeSerializer;
	}

	@Override
	public int hashCode() {
		return typeSerializer.hashCode();
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		this.identityMap = new IdentityHashMap<>();
		this.elementList = new ArrayList<>();
	}
}
