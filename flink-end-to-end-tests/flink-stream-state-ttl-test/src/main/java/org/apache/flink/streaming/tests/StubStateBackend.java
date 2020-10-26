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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stub implementation of the {@link StateBackend} that allows the use of
 * a custom {@link TtlTimeProvider}.
 */
final class StubStateBackend implements StateBackend {

	private static final long serialVersionUID = 1L;

	private final TtlTimeProvider ttlTimeProvider;

	private final StateBackend backend;

	StubStateBackend(final StateBackend wrappedBackend, final TtlTimeProvider ttlTimeProvider) {
		this.backend = checkNotNull(wrappedBackend);
		this.ttlTimeProvider = checkNotNull(ttlTimeProvider);
	}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		return backend.resolveCheckpoint(externalPointer);
	}

	@Override
	public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
		return backend.createCheckpointStorage(jobId);
	}

	@Override
	public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception {

		return backend.createKeyedStateBackend(
			env,
			jobID,
			operatorIdentifier,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			kvStateRegistry,
			this.ttlTimeProvider,
			metricGroup,
			stateHandles,
			cancelStreamRegistry);
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
		Environment env,
		String operatorIdentifier,
		@Nonnull Collection<OperatorStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception {
		return backend.createOperatorStateBackend(env, operatorIdentifier, stateHandles, cancelStreamRegistry);
	}
}
