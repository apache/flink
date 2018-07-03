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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Base class for composite serializers.
 *
 * <p>This class serializes a composite type using array of its field serializers.
 * Fields are indexed the same way as their serializers.
 *
 * @param <T> type of custom serialized value
 */
public abstract class CompositeSerializer<T> extends TypeSerializer<T> {
	private static final long serialVersionUID = 1L;

	/** Serializers for fields which constitute T. */
	protected final TypeSerializer<Object>[] fieldSerializers;

	/** Whether T is an immutable type. */
	final boolean immutableTargetType;

	/** Byte length of target object in serialized form. */
	private final int length;

	/** Whether any field serializer is stateful. */
	private final boolean stateful;

	private final int hashCode;

	@SuppressWarnings("unchecked")
	protected CompositeSerializer(boolean immutableTargetType, TypeSerializer<?> ... fieldSerializers) {
		Preconditions.checkNotNull(fieldSerializers);
		Preconditions.checkArgument(Arrays.stream(fieldSerializers).allMatch(Objects::nonNull));
		this.immutableTargetType = immutableTargetType &&
			Arrays.stream(fieldSerializers).allMatch(TypeSerializer::isImmutableType);
		this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;
		this.length = calcLength();
		this.stateful = isStateful();
		this.hashCode = Arrays.hashCode(fieldSerializers);
	}

	private boolean isStateful() {
		TypeSerializer[] duplicatedSerializers = duplicateFieldSerializers();
		return IntStream.range(0, fieldSerializers.length)
			.anyMatch(i -> fieldSerializers[i] != duplicatedSerializers[i]);
	}

	/** Create new instance from its fields.  */
	public abstract T createInstance(@Nonnull Object ... values);

	/** Modify field of existing instance. Supported only by mutable types. */
	protected abstract void setField(@Nonnull T value, int index, Object fieldValue);

	/** Get field of existing instance. */
	protected abstract Object getField(@Nonnull T value, int index);

	/** Factory for concrete serializer. */
	protected abstract CompositeSerializer<T> createSerializerInstance(TypeSerializer<?> ... originalSerializers);

	@Override
	public CompositeSerializer<T> duplicate() {
		return stateful ? createSerializerInstance(duplicateFieldSerializers()) : this;
	}

	private TypeSerializer[] duplicateFieldSerializers() {
		TypeSerializer[] duplicatedSerializers = new TypeSerializer[fieldSerializers.length];
		for (int index = 0; index < fieldSerializers.length; index++) {
			duplicatedSerializers[index] = fieldSerializers[index].duplicate();
		}
		return duplicatedSerializers;
	}

	@Override
	public boolean isImmutableType() {
		return immutableTargetType;
	}

	@Override
	public T createInstance() {
		Object[] fields = new Object[fieldSerializers.length];
		for (int index = 0; index < fieldSerializers.length; index++) {
			fields[index] = fieldSerializers[index].createInstance();
		}
		return createInstance(fields);
	}

	@Override
	public T copy(T from) {
		Preconditions.checkNotNull(from);
		if (isImmutableType()) {
			return from;
		}
		Object[] fields = new Object[fieldSerializers.length];
		for (int index = 0; index < fieldSerializers.length; index++) {
			fields[index] = fieldSerializers[index].copy(getField(from, index));
		}
		return createInstance(fields);
	}

	@Override
	public T copy(T from, T reuse) {
		Preconditions.checkNotNull(from);
		Preconditions.checkNotNull(reuse);
		if (isImmutableType()) {
			return from;
		}
		Object[] fields = new Object[fieldSerializers.length];
		for (int index = 0; index < fieldSerializers.length; index++) {
			fields[index] = fieldSerializers[index].copy(getField(from, index), getField(reuse, index));
		}
		return createInstanceWithReuse(fields, reuse);
	}

	@Override
	public int getLength() {
		return length;
	}

	private int calcLength() {
		int totalLength = 0;
		for (TypeSerializer<Object> fieldSerializer : fieldSerializers) {
			if (fieldSerializer.getLength() < 0) {
				return -1;
			}
			totalLength += fieldSerializer.getLength();
		}
		return totalLength;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		Preconditions.checkNotNull(record);
		Preconditions.checkNotNull(target);
		for (int index = 0; index < fieldSerializers.length; index++) {
			fieldSerializers[index].serialize(getField(record, index), target);
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		Preconditions.checkNotNull(source);
		Object[] fields = new Object[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			fields[i] = fieldSerializers[i].deserialize(source);
		}
		return createInstance(fields);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		Preconditions.checkNotNull(reuse);
		Preconditions.checkNotNull(source);
		Object[] fields = new Object[fieldSerializers.length];
		for (int index = 0; index < fieldSerializers.length; index++) {
			fields[index] = fieldSerializers[index].deserialize(getField(reuse, index), source);
		}
		return immutableTargetType ? createInstance(fields) : createInstanceWithReuse(fields, reuse);
	}

	private T createInstanceWithReuse(Object[] fields, T reuse) {
		for (int index = 0; index < fields.length; index++) {
			setField(reuse, index, fields[index]);
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		Preconditions.checkNotNull(source);
		Preconditions.checkNotNull(target);
		for (TypeSerializer typeSerializer : fieldSerializers) {
			typeSerializer.copy(source, target);
		}
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CompositeSerializer) {
			CompositeSerializer<?> other = (CompositeSerializer<?>) obj;
			return other.canEqual(this) && Arrays.equals(fieldSerializers, other.fieldSerializers);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof CompositeSerializer;
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new CompositeTypeSerializerConfigSnapshot(fieldSerializers) {
			@Override
			public int getVersion() {
				return 0;
			}
		};
	}

	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof CompositeTypeSerializerConfigSnapshot) {
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs =
				((CompositeTypeSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();
			if (previousSerializersAndConfigs.size() == fieldSerializers.length) {
				return ensureFieldCompatibility(previousSerializersAndConfigs);
			}
		}
		return CompatibilityResult.requiresMigration();
	}

	@SuppressWarnings("unchecked")
	private CompatibilityResult<T> ensureFieldCompatibility(
		List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs) {
		TypeSerializer<Object>[] convertSerializers = new TypeSerializer[fieldSerializers.length];
		boolean requiresMigration = false;
		for (int index = 0; index < previousSerializersAndConfigs.size(); index++) {
			CompatibilityResult<Object> compatResult =
				resolveFieldCompatibility(previousSerializersAndConfigs, index);
			if (compatResult.isRequiresMigration()) {
				requiresMigration = true;
				if (compatResult.getConvertDeserializer() != null) {
					convertSerializers[index] = new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer());
				} else {
					return CompatibilityResult.requiresMigration();
				}
			}
		}
		return requiresMigration ?
			CompatibilityResult.requiresMigration(createSerializerInstance(convertSerializers)) :
			CompatibilityResult.compatible();
	}

	private CompatibilityResult<Object> resolveFieldCompatibility(
		List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs, int index) {
		return CompatibilityUtil.resolveCompatibilityResult(
			previousSerializersAndConfigs.get(index).f0, UnloadableDummyTypeSerializer.class,
			previousSerializersAndConfigs.get(index).f1, fieldSerializers[index]);
	}
}
