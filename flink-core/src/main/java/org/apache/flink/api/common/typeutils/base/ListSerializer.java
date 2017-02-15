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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("ForLoopReplaceableByForEach")
public class ListSerializer<T> extends TypeSerializer<List<T>> {

	private static final long serialVersionUID = 1119562170939152304L;

	private final TypeSerializer<T> elementSerializer;

	public ListSerializer(TypeSerializer<T> elementSerializer) {
		this.elementSerializer = elementSerializer;
	}

	public TypeSerializer<T> getElementSerializer() {
		return elementSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<List<T>> duplicate() {
		TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
		return duplicateElement == elementSerializer ? this : new ListSerializer<T>(duplicateElement);
	}

	@Override
	public List<T> createInstance() {
		return new ArrayList<>();
	}

	@Override
	public List<T> copy(List<T> from) {
		List<T> newList = new ArrayList<>(from.size());
		for (int i = 0; i < from.size(); i++) {
			newList.add(elementSerializer.copy(from.get(i)));
		}
		return newList;
	}

	@Override
	public List<T> copy(List<T> from, List<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1; // var length
	}

	@Override
	public void serialize(List<T> list, DataOutputView target) throws IOException {
		final int size = list.size();
		target.writeInt(size);
		for (int i = 0; i < size; i++) {
			elementSerializer.serialize(list.get(i), target);
		}
	}

	@Override
	public List<T> deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		final List<T> list = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			list.add(elementSerializer.deserialize(source));
		}
		return list;
	}

	@Override
	public List<T> deserialize(List<T> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// copy number of elements
		final int num = source.readInt();
		target.writeInt(num);
		for (int i = 0; i < num; i++) {
			elementSerializer.copy(source, target);
		}
	}

	// --------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
				(obj != null && obj.getClass() == getClass() &&
						elementSerializer.equals(((ListSerializer<?>) obj).elementSerializer));
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

	@Override
	public int hashCode() {
		return elementSerializer.hashCode();
	}
}
