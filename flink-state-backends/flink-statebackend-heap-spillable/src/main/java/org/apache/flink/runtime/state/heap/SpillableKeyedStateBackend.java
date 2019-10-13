/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;

import java.util.Map;

/**
 * TODO how to remove log in the constructor of HeapKeyedStateBackend.
 */
public class SpillableKeyedStateBackend<K> extends HeapKeyedStateBackend<K> {

	private final SpaceAllocator spaceAllocator;

	public SpillableKeyedStateBackend(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		CloseableRegistry cancelStreamRegistry,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		LocalRecoveryConfig localRecoveryConfig,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		HeapSnapshotStrategy<K> snapshotStrategy,
		InternalKeyContext<K> keyContext,
		SpaceAllocator spaceAllocator) {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			registeredKVStates,
			registeredPQStates,
			localRecoveryConfig,
			priorityQueueSetFactory,
			snapshotStrategy,
			keyContext);
		this.spaceAllocator = spaceAllocator;
	}

	@Override
	public void dispose() {
		super.dispose();
		// TODO how to safely close space allocator, for example in the case
		// some snapshots are still use the space
		IOUtils.closeQuietly(spaceAllocator);
	}
}
