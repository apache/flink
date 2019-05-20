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

import java.util.Arrays;

/**
 * Utilities for dealing with the {@link TypeSerializer} and the {@link TypeSerializerSnapshot}.
 */
public final class TypeSerializerUtils {

	/**
	 * Takes snapshots of the given serializers. In case where the snapshots
	 * are still extending the old {@code TypeSerializerConfigSnapshot} class, the snapshots
	 * are set up properly (with their originating serializer) such that the backwards
	 * compatible code paths work.
	 */
	public static TypeSerializerSnapshot<?>[] snapshotBackwardsCompatible(
			TypeSerializer<?>... originatingSerializers) {

		return Arrays.stream(originatingSerializers)
				.map(TypeSerializerUtils::snapshotBackwardsCompatible)
				.toArray(TypeSerializerSnapshot[]::new);
	}

	/**
	 * Takes a snapshot of the given serializer. In case where the snapshot
	 * is still extending the old {@code TypeSerializerConfigSnapshot} class, the snapshot
	 * is set up properly (with its originating serializer) such that the backwards
	 * compatible code paths work.
	 */
	public static <T> TypeSerializerSnapshot<T> snapshotBackwardsCompatible(TypeSerializer<T> originatingSerializer) {
		return configureForBackwardsCompatibility(originatingSerializer.snapshotConfiguration(), originatingSerializer);
	}

	/**
	 * Utility method to bind the serializer and serializer snapshot to a common
	 * generic type variable.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	private static <T> TypeSerializerSnapshot<T> configureForBackwardsCompatibility(
			TypeSerializerSnapshot<?> snapshot,
			TypeSerializer<?> serializer) {

		TypeSerializerSnapshot<T> typedSnapshot = (TypeSerializerSnapshot<T>) snapshot;
		TypeSerializer<T> typedSerializer = (TypeSerializer<T>) serializer;

		if (snapshot instanceof TypeSerializerConfigSnapshot) {
			((TypeSerializerConfigSnapshot<T>) typedSnapshot).setPriorSerializer(typedSerializer);
		}

		return typedSnapshot;
	}

	// ------------------------------------------------------------------------

	/** This class is not meanto to be instantiated. */
	private TypeSerializerUtils() {}
}
