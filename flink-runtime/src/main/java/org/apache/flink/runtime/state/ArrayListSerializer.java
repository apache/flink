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
package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.CollectionSerializerConfigSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;

@SuppressWarnings("ForLoopReplaceableByForEach")
final public class ArrayListSerializer<T> extends TypeSerializer<ArrayList<T>> {

	private static final long serialVersionUID = 1119562170939152304L;

	private final TypeSerializer<T> elementSerializer;

	public ArrayListSerializer(TypeSerializer<T> elementSerializer) {
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
	public TypeSerializer<ArrayList<T>> duplicate() {
		TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
		return duplicateElement == elementSerializer ? this : new ArrayListSerializer<T>(duplicateElement);
	}

	@Override
	public ArrayList<T> createInstance() {
		return new ArrayList<>();
	}

	@Override
	public ArrayList<T> copy(ArrayList<T> from) {
		if (elementSerializer.isImmutableType()) {
			// fast track using memcopy for immutable types
			return new ArrayList<>(from);
		} else {
			// element-wise deep copy for mutable types
			ArrayList<T> newList = new ArrayList<>(from.size());
			for (int i = 0; i < from.size(); i++) {
				newList.add(elementSerializer.copy(from.get(i)));
			}
			return newList;
		}
	}

	@Override
	public ArrayList<T> copy(ArrayList<T> from, ArrayList<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1; // var length
	}

	@Override
	public void serialize(ArrayList<T> list, DataOutputView target) throws IOException {
		final int size = list.size();
		target.writeInt(size);
		for (int i = 0; i < size; i++) {
			elementSerializer.serialize(list.get(i), target);
		}
	}

	@Override
	public ArrayList<T> deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		final ArrayList<T> list = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			list.add(elementSerializer.deserialize(source));
		}
		return list;
	}

	@Override
	public ArrayList<T> deserialize(ArrayList<T> reuse, DataInputView source) throws IOException {
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
						elementSerializer.equals(((ArrayListSerializer<?>) obj).elementSerializer));
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

	@Override
	public int hashCode() {
		return elementSerializer.hashCode();
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new CollectionSerializerConfigSnapshot<>(elementSerializer);
	}

	@Override
	public CompatibilityResult<ArrayList<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof CollectionSerializerConfigSnapshot) {
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> previousElemSerializerAndConfig =
				((CollectionSerializerConfigSnapshot) configSnapshot).getSingleNestedSerializerAndConfig();

			CompatibilityResult<T> compatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousElemSerializerAndConfig.f0,
					UnloadableDummyTypeSerializer.class,
					previousElemSerializerAndConfig.f1,
					elementSerializer);

			if (!compatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else if (compatResult.getConvertDeserializer() != null) {
				return CompatibilityResult.requiresMigration(
					new ArrayListSerializer<>(new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer())));
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
