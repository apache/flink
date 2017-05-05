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
 * A {@code MigrationStrategy} contains information about how to perform migration of data written
 * by an older serializer so that new serializers can continue to work on them.
 */
@PublicEvolving
public final class MigrationStrategy {

	/** Whether or not migration is required. */
	private final boolean requiresStateMigration;

	/**
	 * The fallback deserializer to use, in the case the preceding serializer cannot be found.
	 *
	 * <p>This is only relevant if migration is required.
	 */
	private final TypeSerializer<?> fallbackDeserializer;

	/**
	 * Returns a strategy that simply signals that no migration needs to be performed.
	 *
	 * @return a strategy that does not perform migration
	 */
	public static MigrationStrategy noMigration() {
		return new MigrationStrategy(false, null);
	}

	/**
	 * Returns a strategy that signals migration to be performed, and in the case that the
	 * preceding serializer cannot be found, a provided fallback deserializer can be
	 * used.
	 *
	 * @param fallbackDeserializer a fallback deserializer that can be used to read old data for the migration
	 *                             in the case that the preceding serializer cannot be found.
	 *
	 * @return a strategy that performs migration with a fallback deserializer to read old data.
	 */
	public static MigrationStrategy migrateWithFallbackDeserializer(TypeSerializer<?> fallbackDeserializer) {
		return new MigrationStrategy(true, fallbackDeserializer);
	}

	/**
	 * Returns a strategy that signals migration to be performed, without a fallback deserializer.
	 * If the preceding serializer cannot be found, the migration fails because the old data cannot be read.
	 *
	 * @return a strategy that performs migration, without a fallback deserializer.
	 */
	public static MigrationStrategy migrate() {
		return new MigrationStrategy(true, null);
	}

	private MigrationStrategy(boolean requiresStateMigration, TypeSerializer<?> fallbackDeserializer) {
		this.requiresStateMigration = requiresStateMigration;
		this.fallbackDeserializer = fallbackDeserializer;
	}

	public TypeSerializer<?> getFallbackDeserializer() {
		return fallbackDeserializer;
	}

	public boolean requireMigration() {
		return requiresStateMigration;
	}
}
