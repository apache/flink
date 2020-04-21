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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializers;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class with the methods to write/load/dispose the checkpoint and savepoint metadata.
 *
 * <p>Stored checkpoint metadata files have the following format:
 * <pre>[MagicNumber (int) | Format Version (int) | Checkpoint Metadata (variable)]</pre>
 *
 * <p>The actual savepoint serialization is version-specific via the {@link MetadataSerializer}.
 */
public class Checkpoints {

	private static final Logger LOG = LoggerFactory.getLogger(Checkpoints.class);

	/** Magic number at the beginning of every checkpoint metadata file, for sanity checks. */
	public static final int HEADER_MAGIC_NUMBER = 0x4960672d;

	// ------------------------------------------------------------------------
	//  Writing out checkpoint metadata
	// ------------------------------------------------------------------------

	public static void storeCheckpointMetadata(
			CheckpointMetadata checkpointMetadata,
			OutputStream out) throws IOException {

		DataOutputStream dos = new DataOutputStream(out);
		storeCheckpointMetadata(checkpointMetadata, dos);
	}

	public static void storeCheckpointMetadata(
			CheckpointMetadata checkpointMetadata,
			DataOutputStream out) throws IOException {

		// write generic header
		out.writeInt(HEADER_MAGIC_NUMBER);

		out.writeInt(MetadataV3Serializer.VERSION);
		MetadataV3Serializer.serialize(checkpointMetadata, out);
	}

	// ------------------------------------------------------------------------
	//  Reading and validating checkpoint metadata
	// ------------------------------------------------------------------------

	public static CheckpointMetadata loadCheckpointMetadata(DataInputStream in, ClassLoader classLoader) throws IOException {
		checkNotNull(in, "input stream");
		checkNotNull(classLoader, "classLoader");

		final int magicNumber = in.readInt();

		if (magicNumber == HEADER_MAGIC_NUMBER) {
			final int version = in.readInt();
			final MetadataSerializer serializer = MetadataSerializers.getSerializer(version);
			return serializer.deserialize(in, classLoader);
		}
		else {
			throw new IOException("Unexpected magic number. This can have multiple reasons: " +
					"(1) You are trying to load a Flink 1.0 savepoint, which is not supported by this " +
					"version of Flink. (2) The file you were pointing to is not a savepoint at all. " +
					"(3) The savepoint file has been corrupted.");
		}
	}

	public static CompletedCheckpoint loadAndValidateCheckpoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			CompletedCheckpointStorageLocation location,
			ClassLoader classLoader,
			boolean allowNonRestoredState) throws IOException {

		checkNotNull(jobId, "jobId");
		checkNotNull(tasks, "tasks");
		checkNotNull(location, "location");
		checkNotNull(classLoader, "classLoader");

		final StreamStateHandle metadataHandle = location.getMetadataHandle();
		final String checkpointPointer = location.getExternalPointer();

		// (1) load the savepoint
		final CheckpointMetadata checkpointMetadata;
		try (InputStream in = metadataHandle.openInputStream()) {
			DataInputStream dis = new DataInputStream(in);
			checkpointMetadata = loadCheckpointMetadata(dis, classLoader);
		}

		// generate mapping from operator to task
		Map<OperatorID, ExecutionJobVertex> operatorToJobVertexMapping = new HashMap<>();
		for (ExecutionJobVertex task : tasks.values()) {
			for (OperatorIDPair operatorIDPair : task.getOperatorIDs()) {
				operatorToJobVertexMapping.put(operatorIDPair.getGeneratedOperatorID(), task);
				operatorIDPair.getUserDefinedOperatorID().ifPresent(id -> operatorToJobVertexMapping.put(id, task));
			}
		}

		// (2) validate it (parallelism, etc)
		HashMap<OperatorID, OperatorState> operatorStates = new HashMap<>(checkpointMetadata.getOperatorStates().size());
		for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {

			ExecutionJobVertex executionJobVertex = operatorToJobVertexMapping.get(operatorState.getOperatorID());

			if (executionJobVertex != null) {

				if (executionJobVertex.getMaxParallelism() == operatorState.getMaxParallelism()
						|| !executionJobVertex.isMaxParallelismConfigured()) {
					operatorStates.put(operatorState.getOperatorID(), operatorState);
				} else {
					String msg = String.format("Failed to rollback to checkpoint/savepoint %s. " +
									"Max parallelism mismatch between checkpoint/savepoint state and new program. " +
									"Cannot map operator %s with max parallelism %d to new program with " +
									"max parallelism %d. This indicates that the program has been changed " +
									"in a non-compatible way after the checkpoint/savepoint.",
							checkpointMetadata,
							operatorState.getOperatorID(),
							operatorState.getMaxParallelism(),
							executionJobVertex.getMaxParallelism());

					throw new IllegalStateException(msg);
				}
			} else if (allowNonRestoredState) {
				LOG.info("Skipping savepoint state for operator {}.", operatorState.getOperatorID());
			} else {
				for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
					if (operatorSubtaskState.hasState()) {
						String msg = String.format("Failed to rollback to checkpoint/savepoint %s. " +
										"Cannot map checkpoint/savepoint state for operator %s to the new program, " +
										"because the operator is not available in the new program. If " +
										"you want to allow to skip this, you can set the --allowNonRestoredState " +
										"option on the CLI.",
								checkpointPointer, operatorState.getOperatorID());

						throw new IllegalStateException(msg);
					}
				}

				LOG.info("Skipping empty savepoint state for operator {}.", operatorState.getOperatorID());
			}
		}

		// (3) convert to checkpoint so the system can fall back to it
		CheckpointProperties props = CheckpointProperties.forSavepoint(false);

		return new CompletedCheckpoint(
				jobId,
				checkpointMetadata.getCheckpointId(),
				0L,
				0L,
				operatorStates,
				checkpointMetadata.getMasterStates(),
				props,
				location);
	}

	// ------------------------------------------------------------------------
	//  Savepoint Disposal Hooks
	// ------------------------------------------------------------------------

	public static void disposeSavepoint(
			String pointer,
			StateBackend stateBackend,
			ClassLoader classLoader) throws IOException, FlinkException {

		checkNotNull(pointer, "location");
		checkNotNull(stateBackend, "stateBackend");
		checkNotNull(classLoader, "classLoader");

		final CompletedCheckpointStorageLocation checkpointLocation = stateBackend.resolveCheckpoint(pointer);

		final StreamStateHandle metadataHandle = checkpointLocation.getMetadataHandle();

		// load the savepoint object (the metadata) to have all the state handles that we need
		// to dispose of all state
		final CheckpointMetadata metadata;
		try (InputStream in = metadataHandle.openInputStream();
			DataInputStream dis = new DataInputStream(in)) {

			metadata = loadCheckpointMetadata(dis, classLoader);
		}

		Exception exception = null;

		// first dispose the savepoint metadata, so that the savepoint is not
		// addressable any more even if the following disposal fails
		try {
			metadataHandle.discardState();
		}
		catch (Exception e) {
			exception = e;
		}

		// now dispose the savepoint data
		try {
			metadata.dispose();
		}
		catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// now dispose the location (directory, table, whatever)
		try {
			checkpointLocation.disposeStorageLocation();
		}
		catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// forward exceptions caught in the process
		if (exception != null) {
			ExceptionUtils.rethrowIOException(exception);
		}
	}

	public static void disposeSavepoint(
			String pointer,
			Configuration configuration,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IOException, FlinkException {

		checkNotNull(pointer, "location");
		checkNotNull(configuration, "configuration");
		checkNotNull(classLoader, "classLoader");

		StateBackend backend = loadStateBackend(configuration, classLoader, logger);

		disposeSavepoint(pointer, backend, classLoader);
	}

	@Nonnull
	public static StateBackend loadStateBackend(Configuration configuration, ClassLoader classLoader, @Nullable Logger logger) {
		if (logger != null) {
			logger.info("Attempting to load configured state backend for savepoint disposal");
		}

		StateBackend backend = null;
		try {
			backend = StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, null);

			if (backend == null && logger != null) {
				logger.info("No state backend configured, attempting to dispose savepoint " +
						"with default backend (file system based)");
			}
		}
		catch (Throwable t) {
			// catches exceptions and errors (like linking errors)
			if (logger != null) {
				logger.info("Could not load configured state backend.");
				logger.debug("Detailed exception:", t);
			}
		}

		if (backend == null) {
			// We use the memory state backend by default. The MemoryStateBackend is actually
			// FileSystem-based for metadata
			backend = new MemoryStateBackend();
		}
		return backend;
	}

	// ------------------------------------------------------------------------

	/** This class contains only static utility methods and is not meant to be instantiated. */
	private Checkpoints() {}
}
