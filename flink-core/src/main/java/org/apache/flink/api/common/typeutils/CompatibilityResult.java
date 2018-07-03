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
	 * Returns a result that signals that the new serializer is compatible and no migration is required.
	 *
	 * @return a result that signals migration is not required for the new serializer.
	 */
	public static <T> CompatibilityResult<T> compatible() {
		return new CompatibilityResult<>(false);
	}

	/**
	 * Returns a result that signals migration to be performed.
	 *
	 * @return a result that signals migration is necessary.
	 */
	public static <T> CompatibilityResult<T> requiresMigration() {
		return new CompatibilityResult<>(true);
	}

	private CompatibilityResult(boolean requiresMigration) {
		this.requiresMigration = requiresMigration;
	}

	public boolean isRequiresMigration() {
		return requiresMigration;
	}
}
