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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.compression.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.compression.StreamCompressionDecorator;
import org.apache.flink.runtime.state.compression.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/**
 * An abstract base implementation of the {@link StateBackend} interface.
 *
 * <p>This class has currently no contents and only kept to not break the prior class hierarchy for users.
 */
@PublicEvolving
public abstract class AbstractStateBackend implements StateBackend, java.io.Serializable {

	private static final long serialVersionUID = 4620415814639230247L;

	protected StreamCompressionDecorator streamCompressionDecorator;

	/**
	 * Sets the stream compression decorator which would used in checkpoint and savepoint for state backend.
	 *
	 * @param streamCompressionDecorator The given streamCompressionDecorator.
	 */
	public void setStreamCompressionDecorator(StreamCompressionDecorator streamCompressionDecorator) {
		this.streamCompressionDecorator = Preconditions.checkNotNull(streamCompressionDecorator,
			"The configured streamCompressionDecorator should not be null.");
	}

	public StreamCompressionDecorator determineCompressionDecorator(
		@Nullable ExecutionConfig executionConfig) {

		if (streamCompressionDecorator != null) {
			return streamCompressionDecorator;
		}

		if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
			this.streamCompressionDecorator = SnappyStreamCompressionDecorator.INSTANCE;
		} else {
			this.streamCompressionDecorator = UncompressedStreamCompressionDecorator.INSTANCE;
		}
		return streamCompressionDecorator;
	}

	// ------------------------------------------------------------------------
	//  State Backend - State-Holding Backends
	// ------------------------------------------------------------------------

	@Override
	public abstract <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
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
		CloseableRegistry cancelStreamRegistry) throws IOException;

	@Override
	public abstract OperatorStateBackend createOperatorStateBackend(
		Environment env,
		String operatorIdentifier,
		@Nonnull Collection<OperatorStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception;
}
