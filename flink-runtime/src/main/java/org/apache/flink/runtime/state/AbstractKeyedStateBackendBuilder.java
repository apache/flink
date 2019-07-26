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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * An abstract base implementation of the {@link StateBackendBuilder} interface.
 */
public abstract class AbstractKeyedStateBackendBuilder<K>
	implements StateBackendBuilder<AbstractKeyedStateBackend, BackendBuildingException> {
	protected final TaskKvStateRegistry kvStateRegistry;
	protected final StateSerializerProvider<K> keySerializerProvider;
	protected final ClassLoader userCodeClassLoader;
	protected final int numberOfKeyGroups;
	protected final KeyGroupRange keyGroupRange;
	protected final ExecutionConfig executionConfig;
	protected final TtlTimeProvider ttlTimeProvider;
	protected final StreamCompressionDecorator keyGroupCompressionDecorator;
	protected final Collection<KeyedStateHandle> restoreStateHandles;
	protected final CloseableRegistry cancelStreamRegistry;

	public AbstractKeyedStateBackendBuilder(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		CloseableRegistry cancelStreamRegistry) {
		this.kvStateRegistry = kvStateRegistry;
		this.keySerializerProvider = StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);
		this.userCodeClassLoader = userCodeClassLoader;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;
		this.executionConfig = executionConfig;
		this.ttlTimeProvider = ttlTimeProvider;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.restoreStateHandles = stateHandles;
		this.cancelStreamRegistry = cancelStreamRegistry;
	}
}
