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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * This class wraps an {@link AbstractStateBackend} and enriches all the created objects as spies.
 */
public class TestSpyWrapperStateBackend extends AbstractStateBackend {

		private final AbstractStateBackend delegate;

		public TestSpyWrapperStateBackend(AbstractStateBackend delegate) {
			this.delegate = Preconditions.checkNotNull(delegate);
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup) throws IOException {
			return spy(delegate.createKeyedStateBackend(
				env,
				jobID,
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				kvStateRegistry,
				ttlTimeProvider,
				metricGroup));
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
			Environment env, String operatorIdentifier) throws Exception {
			return spy(delegate.createOperatorStateBackend(env, operatorIdentifier));
		}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		return spy(delegate.resolveCheckpoint(externalPointer));
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		return spy(delegate.createCheckpointStorage(jobId));
	}
}
