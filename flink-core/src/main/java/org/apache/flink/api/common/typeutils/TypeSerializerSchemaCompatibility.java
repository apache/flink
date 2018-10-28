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

import javax.annotation.Nullable;

/**
 * A {@code TypeSerializerSchemaCompatibility} represents information about whether or not a {@link TypeSerializer}
 * can be safely used to read data written by a previous type serializer.
 *
 * <p>Typically, the compatibility of the new serializer is resolved by checking the serializer against the
 * {@link TypeSerializerSnapshot} of the previous serializer. Depending on the type of the
 * resolved compatibility result, migration (i.e., reading bytes with the previous serializer and then writing
 * it again with the new serializer) may be required before the new serializer can be used.
 *
 * @param <T> the type of data serialized by the serializer that was being checked.
 *
 * @see TypeSerializer
 * @see TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)
 */
@PublicEvolving
public class TypeSerializerSchemaCompatibility<T> {

	/**
	 * Enum for the type of the compatibility.
	 */
	enum Type {

		/** This indicates that the new serializer continued to be used as is. */
		COMPATIBLE_AS_IS,

		/**
		 * This indicates that it is possible to use the new serializer after performing a
		 * full-scan migration over all state, by reading bytes with the previous serializer
		 * and then writing it again with the new serializer, effectively converting the
		 * serialization schema to correspond to the new serializer.
		 */
		COMPATIBLE_AFTER_MIGRATION,

		/**
		 * This indicates that the new serializer is incompatible, even with migration.
		 * This normally implies that the deserialized Java class can not be commonly recognized
		 * by the previous and new serializer.
		 */
		INCOMPATIBLE
	}

	/**
	 * The type of the compatibility.
	 */
	private final Type resultType;

	/**
	 * Returns a result that indicates that the new serializer is compatible and no migration is required.
	 * The new serializer can continued to be used as is.
	 *
	 * @return a result that indicates migration is not required for the new serializer.
	 */
	public static <T> TypeSerializerSchemaCompatibility<T> compatibleAsIs() {
		return new TypeSerializerSchemaCompatibility<>(Type.COMPATIBLE_AS_IS, null);
	}

	/**
	 * Returns a result that indicates that the new serializer can be used after migrating the written bytes, i.e.
	 * reading it with the old serializer and then writing it again with the new serializer.
	 *
	 * @return a result that indicates that the new serializer can be used after migrating the written bytes.
	 */
	public static <T> TypeSerializerSchemaCompatibility<T> compatibleAfterMigration() {
		return new TypeSerializerSchemaCompatibility<>(Type.COMPATIBLE_AFTER_MIGRATION, null);
	}

	/**
	 * Returns a result that indicates there is no possible way for the new serializer to be use-able.
	 * This normally indicates that there is no common Java class between what the previous bytes can be
	 * deserialized into and what can be written by the new serializer.
	 *
	 * <p>In this case, there is no possible way for the new serializer to continue to be used, even with
	 * migration. Recovery of the Flink job will fail.
	 *
	 * @return a result that indicates incompatibility between the new and previous serializer.
	 */
	public static <T> TypeSerializerSchemaCompatibility<T> incompatible() {
		return new TypeSerializerSchemaCompatibility<>(Type.INCOMPATIBLE, null);
	}

	private TypeSerializerSchemaCompatibility(Type resultType, @Nullable TypeSerializer<T> reconfiguredNewSerializer) {
		this.resultType = Preconditions.checkNotNull(resultType);
	}

	/**
	 * Returns whether or not the type of the compatibility is {@link Type#COMPATIBLE_AS_IS}.
	 *
	 * @return whether or not the type of the compatibility is {@link Type#COMPATIBLE_AS_IS}.
	 */
	public boolean isCompatibleAsIs() {
		return resultType == Type.COMPATIBLE_AS_IS;
	}

	/**
	 * Returns whether or not the type of the compatibility is {@link Type#COMPATIBLE_AFTER_MIGRATION}.
	 *
	 * @return whether or not the type of the compatibility is {@link Type#COMPATIBLE_AFTER_MIGRATION}.
	 */
	public boolean isCompatibleAfterMigration() {
		return resultType == Type.COMPATIBLE_AFTER_MIGRATION;
	}

	/**
	 * Returns whether or not the type of the compatibility is {@link Type#INCOMPATIBLE}.
	 *
	 * @return whether or not the type of the compatibility is {@link Type#INCOMPATIBLE}.
	 */
	public boolean isIncompatible() {
		return resultType == Type.INCOMPATIBLE;
	}
}
