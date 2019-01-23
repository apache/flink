/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Meta information about a priority queue state in a backend.
 */
public class RegisteredPriorityQueueStateBackendMetaInfo<T> extends RegisteredStateMetaInfoBase {

	@Nonnull
	private final StateSerializerProvider<T> elementSerializerProvider;

	public RegisteredPriorityQueueStateBackendMetaInfo(
		@Nonnull String name,
		@Nonnull TypeSerializer<T> elementSerializer) {

		this(name, StateSerializerProvider.fromNewRegisteredSerializer(elementSerializer));
	}

	@SuppressWarnings("unchecked")
	public RegisteredPriorityQueueStateBackendMetaInfo(StateMetaInfoSnapshot snapshot) {
		this(
			snapshot.getName(),
			StateSerializerProvider.fromPreviousSerializerSnapshot(
				(TypeSerializerSnapshot<T>) Preconditions.checkNotNull(
					snapshot.getTypeSerializerSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER))));

		Preconditions.checkState(StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE == snapshot.getBackendStateType());
	}

	private RegisteredPriorityQueueStateBackendMetaInfo(
		@Nonnull String name,
		@Nonnull StateSerializerProvider<T> elementSerializerProvider) {

		super(name);
		this.elementSerializerProvider = elementSerializerProvider;
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot snapshot() {
		return computeSnapshot();
	}

	@Nonnull
	public TypeSerializer<T> getElementSerializer() {
		return elementSerializerProvider.currentSchemaSerializer();
	}

	@Nonnull
	public TypeSerializerSchemaCompatibility<T> updateElementSerializer(TypeSerializer<T> newElementSerializer) {
		return elementSerializerProvider.registerNewSerializerForRestoredState(newElementSerializer);
	}

	@Nullable
	public TypeSerializer<T> getPreviousElementSerializer() {
		return elementSerializerProvider.previousSchemaSerializer();
	}

	private StateMetaInfoSnapshot computeSnapshot() {
		TypeSerializer<T> elementSerializer = getElementSerializer();
		Map<String, TypeSerializer<?>> serializerMap =
			Collections.singletonMap(
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
				elementSerializer.duplicate());
		Map<String, TypeSerializerSnapshot<?>> serializerSnapshotMap =
			Collections.singletonMap(
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
				elementSerializer.snapshotConfiguration());

		return new StateMetaInfoSnapshot(
			name,
			StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE,
			Collections.emptyMap(),
			serializerSnapshotMap,
			serializerMap);
	}

	public RegisteredPriorityQueueStateBackendMetaInfo deepCopy() {
		return new RegisteredPriorityQueueStateBackendMetaInfo<>(name, getElementSerializer().duplicate());
	}
}
