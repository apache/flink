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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * A {@code CompatibilityResult} contains information about whether or not data migration
 * is required in order to continue using new serializers for previously serialized data.
 *
 * @param <T> the type of the data being migrated.
 */
@PublicEvolving
public final class CompatibilityResult<T> {

	/** Whether or not migration is required. */
	private final boolean requiresMigration;

	/**
	 * The convert deserializer to use for reading previous data during migration,
	 * in the case that the preceding serializer cannot be found.
	 *
	 * <p>This is only relevant if migration is required.
	 */
	private final TypeDeserializer<T> convertDeserializer;

	/**
	 * Returns a result that signals that the new serializer is compatible and no migration is required.
	 *
	 * @return a result that signals migration is not required for the new serializer
	 */
	public static <T> CompatibilityResult<T> compatible() {
		return new CompatibilityResult<>(false, null);
	}

	/**
	 * Returns a result that signals migration to be performed, and in the case that the preceding serializer
	 * cannot be found or restored to read the previous data during migration, a provided convert deserializer
	 * can be used.
	 *
	 * @param convertDeserializer the convert deserializer to use, in the case that the preceding serializer
	 *                            cannot be found.
	 *
	 * @param <T> the type of the data being migrated.
	 *
	 * @return a result that signals migration is necessary, also providing a convert deserializer.
	 */
	public static <T> CompatibilityResult<T> requiresMigration(@Nonnull TypeDeserializer<T> convertDeserializer) {
		Preconditions.checkNotNull(convertDeserializer, "Convert deserializer cannot be null.");

		return new CompatibilityResult<>(true, convertDeserializer);
	}

	/**
	 * Returns a result that signals migration to be performed, and in the case that the preceding serializer
	 * cannot be found or restored to read the previous data during migration, a provided convert serializer
	 * can be used. The provided serializer will only be used for deserialization.
	 *
	 * @param convertSerializer the convert serializer to use, in the case that the preceding serializer
	 *                          cannot be found. The provided serializer will only be used for deserialization.
	 *
	 * @param <T> the type of the data being migrated.
	 *
	 * @return a result that signals migration is necessary, also providing a convert serializer.
	 */
	public static <T> CompatibilityResult<T> requiresMigration(@Nonnull TypeSerializer<T> convertSerializer) {
		Preconditions.checkNotNull(convertSerializer, "Convert serializer cannot be null.");

		return new CompatibilityResult<>(true, new TypeDeserializerAdapter<>(convertSerializer));
	}

	/**
	 * Returns a result that signals migration to be performed. The migration will fail if the preceding
	 * serializer for the previous data cannot be found.
	 *
	 * <p>You can also provide a convert deserializer using {@link #requiresMigration(TypeDeserializer)}
	 * or {@link #requiresMigration(TypeSerializer)}, which will be used as a fallback resort in such cases.
	 *
	 * @return a result that signals migration is necessary, without providing a convert deserializer.
	 */
	public static <T> CompatibilityResult<T> requiresMigration() {
		return new CompatibilityResult<>(true, null);
	}

	private CompatibilityResult(boolean requiresMigration, TypeDeserializer<T> convertDeserializer) {
		this.requiresMigration = requiresMigration;
		this.convertDeserializer = convertDeserializer;
	}

	public TypeDeserializer<T> getConvertDeserializer() {
		return convertDeserializer;
	}

	public boolean isRequiresMigration() {
		return requiresMigration;
	}
}
