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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;

import java.io.IOException;

/**
 * A {@link AbstractStateBackend} that stores all its data and checkpoints in memory and has no
 * capabilities to spill to disk. Checkpoints are serialized and the serialized data is
 * transferred
 */
public class MemoryStateBackend extends AbstractStateBackend {

	private static final long serialVersionUID = 4109305377809414635L;

	/** The default maximal size that the snapshotted memory state may have (5 MiBytes) */
	private static final int DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024;

	/** The maximal size that the snapshotted memory state may have */
	private final int maxStateSize;

	/** Switch to chose between synchronous and asynchronous snapshots */
	private final boolean asynchronousSnapshots;

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the default state size (5 MB).
	 */
	public MemoryStateBackend() {
		this(DEFAULT_MAX_STATE_SIZE);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the given number of bytes.
	 *
	 * @param maxStateSize The maximal size of the serialized state
	 */
	public MemoryStateBackend(int maxStateSize) {
		this(maxStateSize, true);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the default state size (5 MB).
	 *
	 * @param asynchronousSnapshots Switch to enable asynchronous snapshots.
	 */
	public MemoryStateBackend(boolean asynchronousSnapshots) {
		this(DEFAULT_MAX_STATE_SIZE, asynchronousSnapshots);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the given number of bytes.
	 *
	 * @param maxStateSize The maximal size of the serialized state
	 * @param asynchronousSnapshots Switch to enable asynchronous snapshots.
	 */
	public MemoryStateBackend(int maxStateSize, boolean asynchronousSnapshots) {
		this.maxStateSize = maxStateSize;
		this.asynchronousSnapshots = asynchronousSnapshots;
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
		Environment env,
		String operatorIdentifier) throws Exception {

		return new DefaultOperatorStateBackend(
			env.getUserClassLoader(),
			env.getExecutionConfig(),
			asynchronousSnapshots);
	}

	@Override
	public String toString() {
		return "MemoryStateBackend (data in heap memory / checkpoints to JobManager)";
	}

	@Override
	public CheckpointStreamFactory createStreamFactory(
			JobID jobId, String operatorIdentifier) throws IOException {
		return new MemCheckpointStreamFactory(maxStateSize);
	}

	@Override
	public CheckpointStreamFactory createSavepointStreamFactory(
			JobID jobId,
			String operatorIdentifier,
			String targetLocation) throws IOException {

		return new MemCheckpointStreamFactory(maxStateSize);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env, JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) {

		return new HeapKeyedStateBackend<>(
				kvStateRegistry,
				keySerializer,
				env.getUserClassLoader(),
				numberOfKeyGroups,
				keyGroupRange,
				asynchronousSnapshots,
				env.getExecutionConfig());
	}
}
