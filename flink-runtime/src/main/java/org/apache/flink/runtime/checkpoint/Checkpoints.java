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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializer;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializers;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.StreamHandleAndPointer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendGlobalHooks;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class with the methods to write/load/dispose the checkpoint and savepoint metadata.
 * 
 * <p>Stored savepoints have the following format:
 * <pre>[MagicNumber (int) | SavepointVersion (int) | Savepoint (variable)]</pre>
 * 
 * The actual savepoint serialization is version-specific via the {@link SavepointSerializer}.
 */
public class Checkpoints {

	/** Magic number at the beginning of every checkpoint metadata file, for sanity checks */
	public static final int HEADER_MAGIC_NUMBER = 0x4960672d;

	// ------------------------------------------------------------------------

	public static <T extends Savepoint> void storeCheckpointMetadata(
			T savepoint,
			DataOutputStream out) throws IOException {

		// write generic header
		out.writeInt(HEADER_MAGIC_NUMBER);
		out.writeInt(savepoint.getVersion());

		// write checkpoint metadata
		SavepointSerializer<T> serializer = SavepointSerializers.getSerializer(savepoint);
		serializer.serialize(savepoint, out);
	}

	public static StreamHandleAndPointer storeSavepointMetadata(
			Savepoint savepoint,
			JobID jobId,
			StateBackend backend,
			@Nullable String location) throws IOException {

		final CheckpointMetadataStreamFactory metadataStore =
				backend.createSavepointMetadataStreamFactory(jobId, location);

		try (CheckpointMetadataOutputStream out = metadataStore.createCheckpointStateOutputStream();
			DataOutputStream dos = new DataOutputStream(out))
		{
			storeCheckpointMetadata(savepoint, dos);
			return out.closeAndGetPointerHandle();
		}
	}

	public static Savepoint loadSavepoint(DataInputStream in, ClassLoader classLoader) throws IOException {
		checkNotNull(in, "input stream");
		checkNotNull(classLoader, "classLoader");
		
		final int magicNumber = in.readInt();

		if (magicNumber == HEADER_MAGIC_NUMBER) {
			final int version = in.readInt();
			final SavepointSerializer<?> serializer = SavepointSerializers.getSerializer(version);

			if (serializer != null) {
				return serializer.deserialize(in, classLoader);
			}
			else {
				throw new IOException("Unrecognized checkpoint version number: " + version);
			}
		}
		else {
			throw new IOException("Unexpected magic number. This can have multiple reasons: " +
					"(1) You are trying to load a Flink 1.0 savepoint, which is not supported by this " +
					"version of Flink. (2) The file you were pointing to is not a savepoint at all. " +
					"(3) The savepoint file has been corrupted.");
		}
	}

	public static Tuple2<Savepoint, StreamStateHandle> loadSavepointAndHandle(
			String pointer,
			StateBackend backend,
			ClassLoader classLoader) throws IOException {

		checkNotNull(pointer, "pointer");
		checkNotNull(backend, "backend");
		checkNotNull(classLoader, "classLoader");

		final StreamStateHandle metadataHandle = backend.resolveCheckpointLocation(pointer);

		try (FSDataInputStream in = metadataHandle.openInputStream();
			DataInputStream dis = new DataInputStream(in))
		{
			return new Tuple2<>(loadSavepoint(dis, classLoader), metadataHandle);
		}
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

		// we try to use a global hook to dispose savepoints when possible

		if (stateBackend instanceof StateBackendGlobalHooks) {
			// yeah, fast path!
			((StateBackendGlobalHooks) stateBackend).disposeSavepoint(pointer);
		}
		else {
			// the generic path (which is a euphemism for 'slow path')

			final StreamStateHandle metadataHandle = stateBackend.resolveCheckpointLocation(pointer);

			// load the savepoint object (the metadata) to have all the state handles that we need
			// to dispose of all state
			final Savepoint savepoint;
			try(FSDataInputStream in = metadataHandle.openInputStream();
				DataInputStream dis = new DataInputStream(in))
			{
				savepoint = loadSavepoint(dis, classLoader);
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
				savepoint.dispose();
			}
			catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (exception != null) {
				ExceptionUtils.rethrowIOException(exception);
			}
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

		disposeSavepoint(pointer, backend, classLoader);
	}
}
