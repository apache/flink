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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Generalized snapshot for meta information about one state in a state backend
 * (e.g. {@link org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo}).
 */
public class StateMetaInfoSnapshot {

	/**
	 * Predefined keys for the most common options in the meta info.
	 */
	public enum CommonOptionsKeys {
		/** Key to define the {@link StateDescriptor.Type} of a key/value keyed-state */
		KEYED_STATE_TYPE,
		/**
		 * Key to define {@link org.apache.flink.runtime.state.OperatorStateHandle.Mode}, about how operator state is
		 * distributed on restore
		 */
		OPERATOR_STATE_DISTRIBUTION_MODE,
	}

	/**
	 * Predefined keys for the most common serializer types in the meta info.
	 */
	public enum CommonSerializerKeys {
		KEY_SERIALIZER,
		NAMESPACE_SERIALIZER,
		VALUE_SERIALIZER
	}

	/** The name of the state. */
	@Nonnull
	private final String name;

	/** Map of options (encoded as strings) for the state. */
	@Nonnull
	private final Map<String, String> options;

	/** The configurations of all the type serializers used with the state. */
	@Nonnull
	private final Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshots;

	// TODO this will go awy again after FLINK-9377 is merged, that is why it is currently duplicated here.
	/** The serializers used by the state. */
	@Nonnull
	private final Map<String, TypeSerializer<?>> serializers;

	public StateMetaInfoSnapshot(
		@Nonnull String name,
		@Nonnull Map<String, String> options,
		@Nonnull Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshots,
		@Nonnull Map<String, TypeSerializer<?>> serializers) {
		this.name = name;
		this.options = options;
		this.serializerConfigSnapshots = serializerConfigSnapshots;
		this.serializers = serializers;
	}

	@Nullable
	public TypeSerializerConfigSnapshot getTypeSerializerConfigSnapshot(@Nonnull String key) {
		return serializerConfigSnapshots.get(key);
	}

	@Nullable
	public TypeSerializerConfigSnapshot getTypeSerializerConfigSnapshot(@Nonnull CommonSerializerKeys key) {
		return getTypeSerializerConfigSnapshot(key.toString());
	}

	@Nullable
	public String getOption(@Nonnull String key) {
		return options.get(key);
	}

	@Nullable
	public String getOption(@Nonnull StateMetaInfoSnapshot.CommonOptionsKeys key) {
		return getOption(key.toString());
	}

	@Nonnull
	public Map<String, String> getOptionsImmutable() {
		return Collections.unmodifiableMap(options);
	}

	@Nonnull
	public String getName() {
		return name;
	}

	@Nullable
	public TypeSerializer<?> getTypeSerializer(@Nonnull String key) {
		return serializers.get(key);
	}

	@Nullable
	public TypeSerializer<?> getTypeSerializer(@Nonnull CommonSerializerKeys key) {
		return getTypeSerializer(key.toString());
	}

	@Nonnull
	public Map<String, TypeSerializerConfigSnapshot> getSerializerConfigSnapshotsImmutable() {
		return Collections.unmodifiableMap(serializerConfigSnapshots);
	}

	@Nonnull
	public Map<String, TypeSerializer<?>> getSerializersImmutable() {
		return Collections.unmodifiableMap(serializers);
	}
}
