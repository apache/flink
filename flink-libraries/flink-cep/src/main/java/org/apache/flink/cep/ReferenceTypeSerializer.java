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

public class ReferenceTypeSerializer<T> extends TypeSerializer<T> {
	private static final long serialVersionUID = -7633631762221447524L;

	private final TypeSerializer<T> typeSerializer;

	private transient IdentityHashMap<T, Integer> identityMap;
	private transient ArrayList<T> elementList;

	public ReferenceTypeSerializer(final TypeSerializer<T> typeSerializer) {
		this.typeSerializer = typeSerializer;

		this.identityMap = new IdentityHashMap<>();
		this.elementList = new ArrayList<>();
	}

	public TypeSerializer<T> getTypeSerializer() {
		return typeSerializer;
	}

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
		return new ReferenceTypeSerializer<>(typeSerializer);
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

	public void serialize(T record, DataOutputView target) throws IOException {
		if (identityMap.containsKey(record)) {
			target.writeBoolean(true);
			target.writeInt(identityMap.get(record));
		} else {
			target.writeBoolean(false);
			typeSerializer.serialize(record, target);
		}
	}

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
		if (obj instanceof ReferenceTypeSerializer) {
			@SuppressWarnings("unchecked")
			ReferenceTypeSerializer<T> other = (ReferenceTypeSerializer<T>)obj;

			return (other.canEqual(this) && typeSerializer.equals(other.typeSerializer));
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ReferenceTypeSerializer;
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
