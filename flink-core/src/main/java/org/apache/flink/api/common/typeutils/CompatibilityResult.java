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
	private final TypeSerializer<T> convertDeserializer;

	/**
	 * Returns a strategy that signals that the new serializer is compatible and no migration is required.
	 *
	 * @return a result that signals migration is not required for the new serializer
	 */
	public static <T> CompatibilityResult<T> compatible() {
		return new CompatibilityResult<>(false, null);
	}

	/**
	 * Returns a strategy that signals migration to be performed.
	 *
	 * <p>Furthermore, in the case that the preceding serializer cannot be found or restored to read the
	 * previous data during migration, a provided convert deserializer can be used (may be {@code null}
	 * if one cannot be provided).
	 *
	 * <p>In the case that the preceding serializer cannot be found and a convert deserializer is not
	 * provided, the migration will fail due to the incapability of reading previous data.
	 *
	 * @return a result that signals migration is necessary, possibly providing a convert deserializer.
	 */
	public static <T> CompatibilityResult<T> requiresMigration(TypeSerializer<T> convertDeserializer) {
		return new CompatibilityResult<>(true, convertDeserializer);
	}

	private CompatibilityResult(boolean requiresMigration, TypeSerializer<T> convertDeserializer) {
		this.requiresMigration = requiresMigration;
		this.convertDeserializer = convertDeserializer;
	}

	public TypeSerializer<T> getConvertDeserializer() {
		return convertDeserializer;
	}

	public boolean requiresMigration() {
		return requiresMigration;
	}
}
