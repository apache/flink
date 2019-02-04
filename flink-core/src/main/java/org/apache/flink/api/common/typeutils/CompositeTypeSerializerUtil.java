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

import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utilities for the {@link CompositeTypeSerializerSnapshot}.
 */
@Internal
public class CompositeTypeSerializerUtil {

	/**
	 * Delegates compatibility checks to a {@link CompositeTypeSerializerSnapshot} instance.
	 * This can be used by legacy snapshot classes, which have a newer implementation
	 * implemented as a {@link CompositeTypeSerializerSnapshot}.
	 *
	 * @param newSerializer the new serializer to check for compatibility.
	 * @param newCompositeSnapshot an instance of the new snapshot class to delegate compatibility checks to.
	 *                             This instance should already contain the outer snapshot information.
	 * @param legacyNestedSnapshots the nested serializer snapshots of the legacy composite snapshot.
	 *
	 * @return the result compatibility.
	 */
	public static <T> TypeSerializerSchemaCompatibility<T> delegateCompatibilityCheckToNewSnapshot(
			TypeSerializer<T> newSerializer,
			CompositeTypeSerializerSnapshot<T, ? extends TypeSerializer> newCompositeSnapshot,
			TypeSerializerSnapshot<?>... legacyNestedSnapshots) {

		checkArgument(legacyNestedSnapshots.length > 0);
		return newCompositeSnapshot.internalResolveSchemaCompatibility(newSerializer, legacyNestedSnapshots);
	}
}
