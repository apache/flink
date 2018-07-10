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
 * A {@code TypeSerializerSchemaCompatibility} contains information about whether or not data migration
 * is required in order to continue using new serializers for previously serialized data.
 *
 * @param <T> the type of the data being migrated.
 */
@PublicEvolving
public final class TypeSerializerSchemaCompatibility<T> {

	enum Type {
		COMPATIBLE_AS_IS,
		COMPATIBLE_AFTER_RECONFIGURATION,
		INCOMPATIBLE
	}

	/** Whether or not migration is required. */
	private final Type resultType;

	/** */
	private final TypeSerializer<T> reconfiguredNewSerializer;

	/**
	 * Returns a result that signals that the new serializer is compatible and no migration is required.
	 *
	 * @return a result that signals migration is not required for the new serializer.
	 */
	public static <T> TypeSerializerSchemaCompatibility<T> compatibleAsIs() {
		return new TypeSerializerSchemaCompatibility<>(Type.COMPATIBLE_AS_IS, null);
	}

	public static <T> TypeSerializerSchemaCompatibility<T> compatibleAfterReconfiguration(TypeSerializer<T> reconfiguredSerializer) {
		return new TypeSerializerSchemaCompatibility<>(Type.COMPATIBLE_AFTER_RECONFIGURATION, reconfiguredSerializer);
	}

	/**
	 * Returns a result that signals a full-pass state conversion needs to be performed.
	 *
	 * @return a result that signals a full-pass state conversion is necessary.
	 */
	public static <T> TypeSerializerSchemaCompatibility<T> incompatible() {
		return new TypeSerializerSchemaCompatibility<T>(Type.INCOMPATIBLE, null);
	}

	private TypeSerializerSchemaCompatibility(Type resultType, @Nullable TypeSerializer<T> reconfiguredNewSerializer) {
		this.resultType = Preconditions.checkNotNull(resultType);

		if (resultType == Type.COMPATIBLE_AFTER_RECONFIGURATION && reconfiguredNewSerializer == null) {
			throw new IllegalArgumentException();
		}
		this.reconfiguredNewSerializer = reconfiguredNewSerializer;
	}

	public boolean isCompatibleAsIs() {
		return resultType == Type.COMPATIBLE_AS_IS;
	}

	public boolean isCompatibleAfterReconfiguration() {
		return resultType == Type.COMPATIBLE_AFTER_RECONFIGURATION;
	}

	public boolean isIncompatible() {
		return resultType == Type.INCOMPATIBLE;
	}

	public TypeSerializer<T> getReconfiguredNewSerializer() {
		return reconfiguredNewSerializer;
	}
}
