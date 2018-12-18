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
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

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

	final PrecomputedParameters precomputed;

	/** Can be used for user facing constructor. */
	@SuppressWarnings("unchecked")
	protected CompositeSerializer(boolean immutableTargetType, TypeSerializer<?> ... fieldSerializers) {
		this(
			PrecomputedParameters.precompute(immutableTargetType, (TypeSerializer<Object>[]) fieldSerializers),
			fieldSerializers);
	}

	/** Can be used in createSerializerInstance for internal operations. */
	@SuppressWarnings("unchecked")
	protected CompositeSerializer(PrecomputedParameters precomputed, TypeSerializer<?> ... fieldSerializers) {
		this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;
		this.precomputed = precomputed;
	}

	/** Create new instance from its fields.  */
	public abstract T createInstance(@Nonnull Object ... values);

	/** Modify field of existing instance. Supported only by mutable types. */
	protected abstract void setField(@Nonnull T value, int index, Object fieldValue);

	/** Get field of existing instance. */
	protected abstract Object getField(@Nonnull T value, int index);

	/** Factory for concrete serializer. */
	protected abstract CompositeSerializer<T> createSerializerInstance(
		PrecomputedParameters precomputed,
		TypeSerializer<?> ... originalSerializers);

	@Override
	public CompositeSerializer<T> duplicate() {
		return precomputed.stateful ?
			createSerializerInstance(precomputed, duplicateFieldSerializers(fieldSerializers)) : this;
	}

	private static TypeSerializer[] duplicateFieldSerializers(TypeSerializer<Object>[] fieldSerializers) {
		TypeSerializer[] duplicatedSerializers = new TypeSerializer[fieldSerializers.length];
		for (int index = 0; index < fieldSerializers.length; index++) {
			duplicatedSerializers[index] = fieldSerializers[index].duplicate();
			assert duplicatedSerializers[index] != null;
		}
		return duplicatedSerializers;
	}

	@Override
	public boolean isImmutableType() {
		return precomputed.immutable;
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
		return precomputed.length;
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
		return precomputed.immutable ? createInstance(fields) : createInstanceWithReuse(fields, reuse);
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
		return 31 * Boolean.hashCode(precomputed.immutableTargetType) + Arrays.hashCode(fieldSerializers);
	}

	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	@Override
	public boolean equals(Object obj) {
		if (canEqual(obj)) {
			CompositeSerializer<?> other = (CompositeSerializer<?>) obj;
			return precomputed.immutable == other.precomputed.immutable
				&& Arrays.equals(fieldSerializers, other.fieldSerializers);
		}
		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		// as this is an abstract class, we allow equality only between instances of the same class
		return obj != null && getClass().equals(obj.getClass());
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new ConfigSnapshot(fieldSerializers);
	}

	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof ConfigSnapshot) {
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
		return requiresMigration ? createMigrationCompatResult(convertSerializers) : CompatibilityResult.compatible();
	}

	private CompatibilityResult<Object> resolveFieldCompatibility(
		List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs, int index) {
		return CompatibilityUtil.resolveCompatibilityResult(
			previousSerializersAndConfigs.get(index).f0, UnloadableDummyTypeSerializer.class,
			previousSerializersAndConfigs.get(index).f1, fieldSerializers[index]);
	}

	private CompatibilityResult<T> createMigrationCompatResult(TypeSerializer<Object>[] convertSerializers) {
		PrecomputedParameters precomputed =
			PrecomputedParameters.precompute(this.precomputed.immutableTargetType, convertSerializers);
		return CompatibilityResult.requiresMigration(createSerializerInstance(precomputed, convertSerializers));
	}

	/** This class holds composite serializer parameters which can be precomputed in advanced for better performance. */
	protected static class PrecomputedParameters implements Serializable {
		private static final long serialVersionUID = 1L;

		/** Whether target type is immutable. */
		final boolean immutableTargetType;

		/** Whether target type and its fields are immutable. */
		final boolean immutable;

		/** Byte length of target object in serialized form. */
		private final int length;

		/** Whether any field serializer is stateful. */
		final boolean stateful;

		private PrecomputedParameters(boolean immutableTargetType, boolean immutable, int length, boolean stateful) {
			this.immutableTargetType = immutableTargetType;
			this.immutable = immutable;
			this.length = length;
			this.stateful = stateful;
		}

		static PrecomputedParameters precompute(
			boolean immutableTargetType,
			TypeSerializer<Object>[] fieldSerializers) {
			Preconditions.checkNotNull(fieldSerializers);
			int totalLength = 0;
			boolean fieldsImmutable = true;
			boolean stateful = false;
			for (TypeSerializer<Object> fieldSerializer : fieldSerializers) {
				Preconditions.checkNotNull(fieldSerializer);
				if (fieldSerializer != fieldSerializer.duplicate()) {
					stateful = true;
				}
				if (!fieldSerializer.isImmutableType()) {
					fieldsImmutable = false;
				}
				if (fieldSerializer.getLength() < 0) {
					totalLength = -1;
				}
				totalLength = totalLength >= 0 ? totalLength + fieldSerializer.getLength() : totalLength;
			}
			return new PrecomputedParameters(immutableTargetType, fieldsImmutable, totalLength, stateful);
		}
	}

	/** Snapshot field serializers of composite type. */
	public static class ConfigSnapshot extends CompositeTypeSerializerConfigSnapshot {
		private static final int VERSION = 0;

		/** This empty nullary constructor is required for deserializing the configuration. */
		@SuppressWarnings("unused")
		public ConfigSnapshot() {
		}

		ConfigSnapshot(@Nonnull TypeSerializer<?>... nestedSerializers) {
			super(nestedSerializers);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
