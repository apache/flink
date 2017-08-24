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

import org.apache.commons.collections4.multiset.AbstractMultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for Multisets. The serializer relies on an element serializer
 * for the serialization of the Multiset's elements.
 *
 * <p>The serialization format for the Multiset is as follows: four bytes for the length of the lost,
 * followed by the serialized representation of each element.
 *
 * @param <T> The type of element in the Multiset.
 */
@Internal
public final class MultisetSerializer<T> extends TypeSerializer<AbstractMultiSet<T>> {

	private static final long serialVersionUID = 12L;

	/** The serializer for the elements of the Multiset */
	private final TypeSerializer<T> elementSerializer;

	/**
	 * Creates a Multiset serializer that uses the given serializer to serialize the Multiset's elements.
	 *
	 * @param elementSerializer The serializer for the elements of the Multiset
	 */
	public MultisetSerializer(TypeSerializer<T> elementSerializer) {
		this.elementSerializer = checkNotNull(elementSerializer);
	}

	// ------------------------------------------------------------------------
	//  MultisetSerializer specific properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the serializer for the elements of the Multiset.
	 * @return The serializer for the elements of the Multiset
	 */
	public TypeSerializer<T> getElementSerializer() {
		return elementSerializer;
	}

	// ------------------------------------------------------------------------
	//  Type Serializer implementation
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<AbstractMultiSet<T>> duplicate() {
		TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
		return duplicateElement == elementSerializer ?
				this : new MultisetSerializer<>(duplicateElement);
	}

	@Override
	public AbstractMultiSet<T> createInstance() {
		return new HashMultiSet<>();
	}

	@Override
	public AbstractMultiSet<T> copy(AbstractMultiSet<T> from) {
		return new HashMultiSet<>(from);
	}

	@Override
	public AbstractMultiSet<T> copy(AbstractMultiSet<T> from, AbstractMultiSet<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1; // var length
	}

	@Override
	public void serialize(AbstractMultiSet<T> multiSet, DataOutputView target) throws IOException {
		final int size = multiSet.size();
		target.writeInt(size);

		for (T element : multiSet) {
			elementSerializer.serialize(element, target);
		}
	}

	@Override
	public AbstractMultiSet<T> deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		final AbstractMultiSet<T> multiSet = new HashMultiSet<>();
		for (int i = 0; i < size; i++) {
			multiSet.add(elementSerializer.deserialize(source));
		}
		return multiSet;
	}

	@Override
	public AbstractMultiSet<T> deserialize(AbstractMultiSet<T> reuse, DataInputView source)
			throws IOException {
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
						elementSerializer.equals(((MultisetSerializer<?>) obj).elementSerializer));
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
	public CollectionSerializerConfigSnapshot snapshotConfiguration() {
		return new CollectionSerializerConfigSnapshot<>(elementSerializer);
	}

	@Override
	public CompatibilityResult<AbstractMultiSet<T>> ensureCompatibility(
			TypeSerializerConfigSnapshot configSnapshot) {
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
					new MultisetSerializer<>(
							new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer())));
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
