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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.List;

/**
 * Serializer wrapper to add support of {@code null} value serialization.
 *
 * <p>If the target serializer does not support {@code null} values of its type,
 * you can use this class to wrap this serializer.
 * This is a generic treatment of {@code null} value serialization
 * which comes with the cost of additional byte in the final serialized value.
 * The {@code NullableSerializer} will intercept {@code null} value serialization case
 * and prepend the target serialized value with a boolean flag marking whether it is {@code null} or not.
 * <pre> {@code
 * TypeSerializer<T> originalSerializer = ...;
 * TypeSerializer<T> serializerWithNullValueSupport = NullableSerializer.wrap(originalSerializer);
 * // or
 * TypeSerializer<T> serializerWithNullValueSupport = NullableSerializer.wrapIfNullIsNotSupported(originalSerializer);
 * }}</pre>
 *
 * @param <T> type to serialize
 */
public class NullableSerializer<T> extends TypeSerializer<T> {
	private static final long serialVersionUID = 3335569358214720033L;

	@Nonnull
	private final TypeSerializer<T> originalSerializer;

	private NullableSerializer(@Nonnull TypeSerializer<T> originalSerializer) {
		Preconditions.checkNotNull(originalSerializer, "The original serializer cannot be null");
		this.originalSerializer = originalSerializer;
	}

	/**
	 * This method tries to serialize {@code null} value with the {@code originalSerializer}
	 * and wraps it in case of {@link NullPointerException}, otherwise it returns the {@code originalSerializer}.
	 *
	 * @param originalSerializer serializer to wrap and add {@code null} support
	 * @return serializer which supports {@code null} values
	 */
	public static <T> TypeSerializer<T> wrapIfNullIsNotSupported(@Nonnull TypeSerializer<T> originalSerializer) {
		return checkIfNullSupported(originalSerializer) ? originalSerializer : wrap(originalSerializer);
	}

	/**
	 * This method checks if {@code serializer} supports {@code null} value.
	 *
	 * @param serializer serializer to check
	 */
	public static <T> boolean checkIfNullSupported(@Nonnull TypeSerializer<T> serializer) {
		try {
			int length = serializer.getLength() > 0 ? serializer.getLength() : 1;
			DataOutputSerializer dos = new DataOutputSerializer(length);
			serializer.serialize(null, dos);
			DataInputDeserializer dis = new DataInputDeserializer(dos.getSharedBuffer());
			Preconditions.checkArgument(serializer.deserialize(dis) == null);
			Preconditions.checkArgument(serializer.copy(null) == null);
		} catch (IOException | RuntimeException e) {
			return false;
		}
		return true;
	}

	/**
	 * This method wraps the {@code originalSerializer} with the {@code NullableSerializer} if not already wrapped.
	 *
	 * @param originalSerializer serializer to wrap and add {@code null} support
	 * @return wrapped serializer which supports {@code null} values
	 */
	public static <T> TypeSerializer<T> wrap(@Nonnull TypeSerializer<T> originalSerializer) {
		return originalSerializer instanceof NullableSerializer ?
			originalSerializer : new NullableSerializer<>(originalSerializer);
	}

	@Override
	public boolean isImmutableType() {
		return originalSerializer.isImmutableType();
	}

	@Override
	public TypeSerializer<T> duplicate() {
		TypeSerializer<T> duplicateOriginalSerializer = originalSerializer.duplicate();
		return duplicateOriginalSerializer == originalSerializer ?
			this : new NullableSerializer<>(originalSerializer.duplicate());
	}

	@Override
	public T createInstance() {
		return originalSerializer.createInstance();
	}

	@Override
	public T copy(T from) {
		return from == null ? null : originalSerializer.copy(from);
	}

	@Override
	public T copy(T from, T reuse) {
		return from == null ? null :
			(reuse == null ? originalSerializer.copy(from) : originalSerializer.copy(from, reuse));
	}

	@Override
	public int getLength() {
		int len = originalSerializer.getLength();
		return len == 0 ? 1 : -1;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		if (record == null) {
			target.writeBoolean(true);
		} else {
			target.writeBoolean(false);
			originalSerializer.serialize(record, target);
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		boolean isNull = source.readBoolean();
		return isNull ? null : originalSerializer.deserialize(source);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		boolean isNull = source.readBoolean();
		return isNull ? null : (reuse == null ?
			originalSerializer.deserialize(source) : originalSerializer.deserialize(reuse, source));
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		boolean isNull = source.readBoolean();
		target.writeBoolean(isNull);
		if (!isNull) {
			originalSerializer.copy(source, target);
		}
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			(obj != null && obj.getClass() == getClass() &&
				originalSerializer.equals(((NullableSerializer) obj).originalSerializer));
	}

	@Override
	public boolean canEqual(Object obj) {
		return (obj != null && obj.getClass() == getClass() &&
			originalSerializer.canEqual(((NullableSerializer) obj).originalSerializer));
	}

	@Override
	public int hashCode() {
		return originalSerializer.hashCode();
	}

	@Override
	public NullableSerializerConfigSnapshot<T> snapshotConfiguration() {
		return new NullableSerializerConfigSnapshot<>(originalSerializer);
	}

	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof NullableSerializerConfigSnapshot) {
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousKvSerializersAndConfigs =
				((NullableSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

			CompatibilityResult<T> compatResult = CompatibilityUtil.resolveCompatibilityResult(
				previousKvSerializersAndConfigs.get(0).f0,
				UnloadableDummyTypeSerializer.class,
				previousKvSerializersAndConfigs.get(0).f1,
				originalSerializer);

			if (!compatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else if (compatResult.getConvertDeserializer() != null) {
				return CompatibilityResult.requiresMigration(
					new NullableSerializer<>(
						new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer())));
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	/**
	 * Configuration snapshot for serializers of nullable types, containing the
	 * configuration snapshot of its original serializer.
	 */
	@Internal
	public static class NullableSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {
		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		@SuppressWarnings("unused")
		public NullableSerializerConfigSnapshot() {}

		NullableSerializerConfigSnapshot(TypeSerializer<T> originalSerializer) {
			super(originalSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
