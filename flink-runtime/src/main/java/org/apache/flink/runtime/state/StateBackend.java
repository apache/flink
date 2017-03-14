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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A <b>State Backend</b> defines how the state of a streaming application is stored and
 * checkpointed. Different State Backends store their state in different fashions, and use
 * different data structures to hold the state of a running application.
 *
 * <p>For example, the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend memory state backend}
 * keeps working state in the memory of the TaskManager and stores checkpoints in the memory of the
 * JobManager. The backend is lightweight and without additional dependencies, but not highly available
 * and supports only small state.
 *
 * <p>The {@link org.apache.flink.runtime.state.filesystem.FsStateBackend file system state backend}
 * keeps working state in the memory of the TaskManager and stores state checkpoints in a filesystem
 * (typically a replicated highly-available filesystem, like <a href="https://hadoop.apache.org/">HDFS</a>,
 * <a href="https://ceph.com/">Ceph</a>, <a href="https://aws.amazon.com/documentation/s3/">S3</a>,
 * <a href="https://cloud.google.com/storage/">GCS</a>, etc).
 * 
 * <p>The {@code RocksDBStateBackend} stores working state in <a href="http://rocksdb.org/">RocksDB</a>,
 * and checkpoints the state by default to a filesystem (similar to the {@code FsStateBackend}).
 * 
 * <h2>Raw Bytes Storage and Backends</h2>
 * 
 * The {@code StateBackend} creates services for <i>raw bytes storage</i> and for <i>keyed state</i>
 * and <i>operator state</i>.
 * 
 * <p>The <i>raw bytes storage</i> (through the {@link CheckpointStreamFactory}) is the fundamental
 * service that simply stores bytes in a fault tolerant fashion. This service is used by the JobManager
 * to store checkpoint and recovery metadata and is typically also used by the keyed- and operator state
 * backends to store checkpointed state.
 *
 * <p>The {@link AbstractKeyedStateBackend} and {@link OperatorStateBackend} created by this state
 * backend define how to hold the working state for keys and operators. They also define how to checkpoint
 * that state, frequently using the raw bytes storage (via the {@code CheckpointStreamFactory}).
 * However, it is also possible that for example a keyed state backend simply implements the bridge to
 * a key/value store, and that it does not need to store anything in the raw byte storage upon a
 * checkpoint.
 * 
 * <h2>Serializability</h2>
 * 
 * State Backends need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application code. 
 * 
 * <p>Because of that, {@code StateBackend} implementations (typically subclasses
 * of {@link AbstractStateBackend}) are meant to be like <i>factories</i> that create the proper
 * states stores that provide access to the persistent storage and hold the keyed- and operator
 * state data structures. That way, the State Backend can be very lightweight (contain only
 * configurations) which makes it easier to be serializable.
 * 
 * 
 * <h2>Thread Safety</h2>
 * 
 * State backend implementations have to be thread-safe. Multiple threads may be creating
 * streams and keyed-/operator state backends concurrently.
 */
@PublicEvolving
public interface StateBackend extends java.io.Serializable {

	// ------------------------------------------------------------------------
	//  Persistent Bytes Storage for Checkpoint Data 
	// ------------------------------------------------------------------------

	/**
	 * Creates a CheckpointStreamFactory that can produces streams to persist data as part of a checkpoint.
	 * 
	 * <p>The target location to write the data to should depend on the state backend's configuration
	 * (for example the target directory), as well as the Job ID and the checkpoint number (which 
	 * is passed when opening a specific stream).
	 *
	 * @param jobId              The JobID of the job for which we are creating checkpoint streams.
	 * @param operatorIdentifier An identifier of the operator that stores the data.
	 * 
	 * @throws IOException Failures during stream creation are forwarded.
	 */
	CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException;

	/**
	 * Creates a CheckpointStreamFactory that can produces streams to persist data as part of a savepoint.
	 *
	 * <p>This method differs from {@link #createStreamFactory(JobID, String)} mainly in that it takes
	 * an optional target location as a parameter (for example when a specific savepoint directory
	 * is passed when triggering the savepoint) and that it may refer to a different default location
	 * (the default savepoint location may be different from the default checkpoint location).
	 *
	 * @param operatorIdentifier An identifier of the operator for which we create streams.
	 * @param targetLocation The savepoint root directory, never null.
	 * 
	 * @return The stream factory for data written to the savepoint.
	 * 
	 * @throws IOException Failures during stream creation are forwarded.
	 */
	CheckpointStreamFactory createSavepointStreamFactory(
			String operatorIdentifier,
			String targetLocation) throws IOException;

	// ------------------------------------------------------------------------
	//  Persistent Bytes Storage for Metadata 
	// ------------------------------------------------------------------------

	/**
	 * Checks whether the state backend supports and is configured for persisting metadata.
	 * 
	 * @return True, if the state backend can persist metadata.
	 */
	boolean supportsExternalizedMetadata();

	/**
	 * Gets the location where metadata is persisted to, for example the directory path for file-based
	 * state backends. Interpreting this string is up to the state backend.
	 * 
	 * @return The location for metadata persistence, or null, if no metadata persistence is configured.
	 */
	@Nullable
	String getMetadataPersistenceLocation();

	/**
	 * Creates a stream factory for writing the metadata of a checkpoint.
	 * 
	 * @param jobID The ID of the job for which the savepoint is created.
	 * @param checkpointId The ID (logical timestamp) of the checkpoint that should be persisted.
	 * @return A stream factory that supports writing the metadata for the checkpoint.
	 * 
	 * @throws IOException
	 *             Thrown if the stream factory could not be initialized due to an I/O exception.
	 * @throws UnsupportedOperationException
	 *             If the state backend is not configured to store checkpoint metadata.
	 */
	CheckpointMetadataStreamFactory createCheckpointMetadataStreamFactory(
			JobID jobID,
			long checkpointId) throws IOException;

	/**
	 * Creates a stream factory for writing the metadata of a savepoint. This method takes optionally
	 * a target location to persist the savepoint to. If no target location is passed, the state
	 * backend should use the default savepoint location.
	 * 
	 * <p>The created factory must return the actual location it persists to (which may be the raw location
	 * passed, or a customized location like a subdirectory) via the
	 * {@link CheckpointMetadataStreamFactory#getTargetLocation()} method.
	 * 
	 * @param jobID The ID of the job for which the savepoint is created.
	 * @param targetLocation Optionally, the target location for that specific savepoint.
	 * @return A stream factory that supports writing the metadata for the savepoint.
	 * 
	 * @throws IOException
	 *             Thrown if the stream factory could not be initialized due to an I/O exception.
	 * @throws UnsupportedOperationException
	 *             If the state backend is not configured for savepoints, or if the targetLocation is
	 *             null and no default location has been configured.
	 */
	CheckpointMetadataStreamFactory createSavepointMetadataStreamFactory(
			JobID jobID,
			@Nullable String targetLocation) throws IOException;

	/**
	 * Resolves the given pointer to a checkpoint/savepoint into a state handle from which the
	 * checkpoint metadata can be read. If the state backend cannot understand the format of
	 * the pointer (for example because it was created by a different state backend) this method
	 * should throw an {@code IOException}.
	 * 
	 * @param pointer The pointer to resolve
	 * @return The state handler from which one can read the checkpoint metadata
	 * 
	 * @throws IOException Thrown, if the state backend does not understand the pointer, or if
	 *                     the pointer could not be resolved due to an I/O error.
	 */
	StreamStateHandle resolveCheckpointLocation(String pointer) throws IOException;

	// ------------------------------------------------------------------------
	//  State Holding Backends 
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@link AbstractKeyedStateBackend} that is responsible for holding <b>keyed state</b>
	 * and checkpointing it.
	 * 
	 * <p><i>Keyed State</i> is state where each value is bound to a key.
	 * 
	 * @param env
	 * @param jobID
	 * @param operatorIdentifier
	 * @param keySerializer
	 * @param numberOfKeyGroups
	 * @param keyGroupRange
	 * @param kvStateRegistry
	 * 
	 * @param <K> The type of the keys by which the state is organized.
	 *     
	 * @return The Keyed State Backend for the given job, operator, and key group range.
	 * 
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	<K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws Exception;

	/**
	 * Creates a new {@link OperatorStateBackend} that can be used for storing operator state.
	 * 
	 * <p>Operator state is state that is associated with parallel operator (or function) instances,
	 * rather than with keys.
	 * 
	 * @param env The runtime environment of the executing task.
	 * @param operatorIdentifier The identifier of the operator whose state should be stored.
	 * 
	 * @return The OperatorStateBackend for operator identified by the job and operator identifier.
	 * 
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier) throws Exception;
}
