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
import org.apache.flink.util.Preconditions;

/**
 * A base class for simple composite type serializers which consists of multiple nested
 * type serializers. The constructor takes a pre-instantiated {@link CompositeTypeSerializerConfigSnapshot}
 * instance, which is cached and returned on each {@link #snapshotConfiguration()} call.
 * When compatibility is to be ensured with a restored configuration snapshot, that snapshot is checked whether or not
 * it is of the same class as the cached one. The compatibility check is performed by simply ensuring that
 * all nested serializers are compatible with the corresponding nested configuration snapshots.
 */
@Internal
public abstract class CompositeTypeSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = -1460007327926564185L;

	private final CompositeTypeSerializerConfigSnapshot<T> cachedConfigSnapshot;

	private final TypeSerializer<?>[] nestedSerializers;

	/**
	 * Super constructor for composite type serializers.
	 *
	 * @param cachedConfigSnapshot the {@link CompositeTypeSerializerConfigSnapshot} to be cached.
	 * @param nestedSerializers the nested serializers captured by the cached config snapshot, in the exact same order.
	 */
	public CompositeTypeSerializer(
			CompositeTypeSerializerConfigSnapshot<T> cachedConfigSnapshot,
			TypeSerializer<?>... nestedSerializers) {

		this.cachedConfigSnapshot = Preconditions.checkNotNull(cachedConfigSnapshot);
		this.nestedSerializers = Preconditions.checkNotNull(nestedSerializers);

		Preconditions.checkArgument(
			cachedConfigSnapshot.getNumNestedSerializers() == nestedSerializers.length,
			"The cached composite type serializer configuration snapshot captures a different number of" +
				" nested serializers than the length of the provided list of nested serializers.");
	}

	@Override
	public final TypeSerializerConfigSnapshot<T> snapshotConfiguration() {
		return cachedConfigSnapshot;
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
		if (isComparableSnapshot(configSnapshot)) {
			@SuppressWarnings("unchecked")
			CompositeTypeSerializerConfigSnapshot<T> snapshot = (CompositeTypeSerializerConfigSnapshot<T>) configSnapshot;

			int i = 0;
			for (TypeSerializer<?> nestedSerializer : nestedSerializers) {
				TypeSerializerSchemaCompatibility<?> compatibilityResult = CompatibilityUtil.resolveCompatibilityResult(
					snapshot.getNestedSerializerConfigSnapshot(i),
					nestedSerializer);

				if (compatibilityResult.isIncompatible()) {
					return TypeSerializerSchemaCompatibility.incompatible();
				}

				i++;
			}

			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		} else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	public TypeSerializer<?>[] getNestedSerializers() {
		return nestedSerializers;
	}

	/**
	 * Subclasses can override this if the serializer recognizes configuration snapshot
	 * classes beyond the cached one. For example, composite type serializers which in previous versions
	 * return a different configuration snapshot class than the one currently used could override this
	 * method for backwards compatibility.
	 *
	 * @param configSnapshot the config snapshot to compare with.
	 */
	protected boolean isComparableSnapshot(TypeSerializerConfigSnapshot<?> configSnapshot) {
		return configSnapshot.getClass().equals(cachedConfigSnapshot.getClass());
	}
}
