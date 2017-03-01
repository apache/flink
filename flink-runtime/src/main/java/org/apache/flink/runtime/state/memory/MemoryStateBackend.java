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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This state backend holds the working state in the memory (JVM heap) of the TaskManagers.
 * The state backend checkpoints state directly the JobManager's memory (hence the backend's name),
 * but the checkpoints will be persisted to a file system for high-availability setups, savepoints,
 * and externalized checkpoints. The {@link MemoryStateBackend} consequently is a FileSystem-based
 * backend that can work without a file system dependency in simple setups.
 * 
 * <p>This state backend is only usable for experimentation, quick local setups, or for streaming
 * applications that have very small state. For any other setup, the
 * {@link org.apache.flink.runtime.state.filesystem.FsStateBackend FsStateBackend} can be used.
 * The {@code FsStateBackend} holds the working state on the TaskManagers in the same way, but
 * checkpoints state directly to files rather then to the JobManager's memory, thus supporting
 * large state sizes. 
 * 
 * <h1>State Size Considerations</h1>
 * 
 * State checkpointing with this state backend is subject to the following conditions:
 * <ul>
 *     <li>Each individual state must not exceed the configured maximum state size
 *         (see {@link #getMaxStateSize()}.</li>
 * 
 *     <li>All state from one task (i.e., the sum of all operator states and keyed states from all
 *         chained operators of the task) must not exceed what the RPC system supports, which is
 *         be default < 10 MB. That limit can be configured up, but that is typically not advised</li>
 * 
 *     <li>The sum of all states in the application times all retained checkpoints must comfortably
 *         fit into the JobManager's JVM heap space.</li>
 * </ul>
 * 
 * <h1>Persistence Guarantees</h1>
 * 
 * For the use cases where the state sizes can be handled by this backend, the backend does guarantee
 * persistence for savepoints, externalized checkpoints (of configured), and checkpoints
 * (when high-availability is configured).
 * 
 * <h1>Configuration</h1>
 * 
 * As for all state backends, this backend can either be configured within the application (by creating
 * the backend with the respective constructor parameters and setting it on the execution environment)
 * or by specifying it in the Flink configuration.
 * 
 * <p>If the state backend was specified in the application, it may pick up additional configuration
 * parameters from the Flink configuration. For example, if the backend if configured in the application
 * without a default savepoint directory, it will pick up a default savepoint directory specified in the
 * Flink configuration of the running job/cluster. That behavior is implemented via the
 * {@link #configure(Configuration)} method.
 */
@PublicEvolving
public class MemoryStateBackend extends AbstractFileStateBackend implements ConfigurableStateBackend {

	private static final long serialVersionUID = 4109305377809414635L;

	/** The default maximal size that the snapshotted memory state may have (5 MiBytes) */
	public static final int DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024;

	/** The maximal size that the snapshotted memory state may have */
	private final int maxStateSize;

	// ------------------------------------------------------------------------

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
	 * <p><b>WARNING:</b> It is not encouraged to increase the size of this value beyond the default
	 * value ({@value #DEFAULT_MAX_STATE_SIZE}). The checkpointed state needs to be send to the JobManager
	 * via RPC messages, and there is an upper limit on the amount of payload these RPC messages can carry.
	 * Furthermore, the JobManager will need to be able to hold the state in its memory
	 *
	 * @param maxStateSize The maximal size of the serialized state
	 */
	public MemoryStateBackend(int maxStateSize) {
		this(null, null, maxStateSize);
	}

	/**
	 * Creates a new MemoryStateBackend, setting optionally the path to persist checkpoint metadata
	 * to, and to persist savepoints to.
	 * 
	 * @param checkpointPath The path to write checkpoint metadata to in highly available setups
	 *                       when checkpoints are set to be externalized. May be null.
	 * @param savepointPath  The path to write savepoints to. May be null.
	 */
	public MemoryStateBackend(@Nullable String checkpointPath, @Nullable String savepointPath) {
		this(checkpointPath, savepointPath, DEFAULT_MAX_STATE_SIZE);
	}

	/**
	 * Creates a new MemoryStateBackend, setting optionally the path to persist checkpoint metadata
	 * to, and to persist savepoints to, and setting the maximum size for a state stored in this backend.
	 * 
	 * <p><b>WARNING:</b> It is not encouraged to increase the size of this value beyond the default
	 * value ({@value #DEFAULT_MAX_STATE_SIZE}). The checkpointed state needs to be send to the JobManager
	 * via RPC messages, and there is an upper limit on the amount of payload these RPC messages can carry.
	 * 
	 * @param checkpointPath The path to write checkpoint metadata to in highly available setups
	 *                       when checkpoints are set to be externalized. May be null.
	 * @param savepointPath  The path to write savepoints to. May be null.
	 * @param maxStateSize   The maximal size of the serialized state
	 */
	public MemoryStateBackend(@Nullable String checkpointPath, @Nullable String savepointPath, int maxStateSize) {
		super(checkpointPath == null ? null : new Path(checkpointPath),
				savepointPath == null ? null : new Path(savepointPath));

		checkArgument(maxStateSize > 0, "maxStateSize must be > 0");
		this.maxStateSize = maxStateSize;
	}

	/**
	 * Private constructor that creates a re-configured copy of the state backend.
	 * 
	 * @param original The state backend to re-configure
	 * @param configuration The configuration
	 */
	private MemoryStateBackend(MemoryStateBackend original, Configuration configuration) {
		super(original.getCheckpointPath(), original.getSavepointPath(), configuration);

		this.maxStateSize = original.maxStateSize;
	}

	/**
	 * Gets the maximum size that an individual state can have, as configured in the
	 * constructor (by default {@value #DEFAULT_MAX_STATE_SIZE}).
	 * 
	 * @return The maximum size that an individual state can have
	 */
	public int getMaxStateSize() {
		return maxStateSize;
	}

	// ------------------------------------------------------------------------
	//  Reconfiguration
	// ------------------------------------------------------------------------

	/**
	 * Creates a copy of this state backend that uses the values defined in the configuration
	 * for fields where that were not specified in this state backend.
	 * 
	 * @param config the configuration
	 * @return The re-configured variant of the state backend
	 */
	@Override
	public MemoryStateBackend configure(Configuration config) {
		return new MemoryStateBackend(this, config);
	}

	// ------------------------------------------------------------------------
	//  checkpoint state persistence
	// ------------------------------------------------------------------------

	@Override
	public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) {
		return new MemCheckpointStreamFactory(maxStateSize);
	}

	@Override
	public CheckpointStreamFactory createSavepointStreamFactory(String operatorIdentifier, String targetLocation) {
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
				keyGroupRange);
	}

	// ------------------------------------------------------------------------
	//  global hooks
	// ------------------------------------------------------------------------

	@Nullable
	@Override
	public StateDisposeHook createCheckpointDisposeHook(
			CompletedCheckpoint checkpoint) throws FlinkException, IOException {

		final String pointer = checkpoint.getExternalPointer();
		if (pointer == null) {
			return null;
		}
		else {
			final FileStatus metadataFileStatus = resolveCheckpointPointer(pointer, true);
			final Path dirToDelete = metadataFileStatus.getPath().getParent();

			return new DirectoryDeleteCheckpointDisposalHook(dirToDelete);
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "MemoryStateBackend (checkpoints: " + getCheckpointPath() +
				" , savepoints: " + getSavepointPath() + " )";
	}
}
