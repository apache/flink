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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Collection;

/**
 * Configuration snapshot of a serializer for collection types.
 *
 * @param <T> Type of the element.
 *
 * @deprecated this configuration snapshot is not capable of being a factory for all serializers that
 *             previously write this as their configuration snapshot, and therefore deprecated. It is no
 *             longer written by any of Flink's serializers, but is still here for backwards compatibility.
 */
@Internal
@Deprecated
public final class CollectionSerializerConfigSnapshot<C extends Collection<T>, T>
		extends CompositeTypeSerializerConfigSnapshot<C> {

	private static final int VERSION = 2;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public CollectionSerializerConfigSnapshot() {}

	public CollectionSerializerConfigSnapshot(TypeSerializer<T> elementSerializer) {
		super(elementSerializer);
	}

	@Override
	public int[] getCompatibleVersions() {
		return new int[]{VERSION, 1};
	}

	@Override
	protected boolean containsSerializers() {
		// versions that still used this config snapshot always still wrote the serializers
		return true;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	protected TypeSerializer<C> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected boolean isRecognizableSerializer(TypeSerializer<?> newSerializer) {
		return newSerializer instanceof ListSerializer;
	}
}
