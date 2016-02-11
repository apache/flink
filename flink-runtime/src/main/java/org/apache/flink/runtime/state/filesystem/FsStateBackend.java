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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.AbstractStateBackend;

import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.UUID;

/**
 * The file state backend is a state backend that stores the state of streaming jobs in a file system.
 *
 * <p>The state backend has one core directory into which it puts all checkpoint data. Inside that
 * directory, it creates a directory per job, inside which each checkpoint gets a directory, with
 * files for each state, for example:
 *
 * {@code hdfs://namenode:port/flink-checkpoints/<job-id>/chk-17/6ba7b810-9dad-11d1-80b4-00c04fd430c8 }
 */
public class FsStateBackend extends AbstractStateBackend {

	private static final long serialVersionUID = -8191916350224044011L;

	private static final Logger LOG = LoggerFactory.getLogger(FsStateBackend.class);

	/** By default, state smaller than 1024 bytes will not be written to files, but
	 * will be stored directly with the metadata */
	public static final int DEFAULT_FILE_STATE_THRESHOLD = 1024;

	/** Maximum size of state that is stored with the metadata, rather than in files */
	public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
	
	/** Default size for the write buffer */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4096;
	

	/** The path to the directory for the checkpoint data, including the file system
	 * description via scheme and optional authority */
	private final Path basePath;

	/** State below this size will be stored as part of the metadata, rather than in files */
	private final int fileStateThreshold;
	
	/** The directory (job specific) into this initialized instance of the backend stores its data */
	private transient Path checkpointDirectory;

	/** Cached handle to the file system for file operations */
	private transient FileSystem filesystem;


	/**
	 * Creates a new state backend that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(String checkpointDataUri) throws IOException {
		this(new Path(checkpointDataUri));
	}

	/**
	 * Creates a new state backend that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(Path checkpointDataUri) throws IOException {
		this(checkpointDataUri.toUri());
	}

	/**
	 * Creates a new state backend that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(URI checkpointDataUri) throws IOException {
		this(checkpointDataUri, DEFAULT_FILE_STATE_THRESHOLD);
	}

	/**
	 * Creates a new state backend that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 * @param fileStateSizeThreshold State up to this size will be stored as part of the metadata,
	 *                             rather than in files
	 * 
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(URI checkpointDataUri, int fileStateSizeThreshold) throws IOException {
		if (fileStateSizeThreshold < 0) {
			throw new IllegalArgumentException("The threshold for file state size must be zero or larger.");
		}
		if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
			throw new IllegalArgumentException("The threshold for file state size cannot be larger than " +
				MAX_FILE_STATE_THRESHOLD);
		}
		this.fileStateThreshold = fileStateSizeThreshold;
		
		this.basePath = validateAndNormalizeUri(checkpointDataUri);
		this.filesystem = this.basePath.getFileSystem();
	}

	/**
	 * Gets the base directory where all state-containing files are stored.
	 * The job specific directory is created inside this directory.
	 *
	 * @return The base directory.
	 */
	public Path getBasePath() {
		return basePath;
	}

	/**
	 * Gets the directory where this state backend stores its checkpoint data. Will be null if
	 * the state backend has not been initialized.
	 *
	 * @return The directory where this state backend stores its checkpoint data.
	 */
	public Path getCheckpointDirectory() {
		return checkpointDirectory;
	}

	/**
	 * Gets the size (in bytes) above which the state will written to files. State whose size
	 * is below this threshold will be directly stored with the metadata
	 * (the state handles), rather than in files. This threshold helps to prevent an accumulation
	 * of small files for small states.
	 * 
	 * @return The threshold (in bytes) above which state is written to files.
	 */
	public int getFileStateSizeThreshold() {
		return fileStateThreshold;
	}

	/**
	 * Checks whether this state backend is initialized. Note that initialization does not carry
	 * across serialization. After each serialization, the state backend needs to be initialized.
	 *
	 * @return True, if the file state backend has been initialized, false otherwise.
	 */
	public boolean isInitialized() {
		return filesystem != null && checkpointDirectory != null;
	}

	/**
	 * Gets the file system handle for the file system that stores the state for this backend.
	 *
	 * @return This backend's file system handle.
	 */
	public FileSystem getFileSystem() {
		if (filesystem != null) {
			return filesystem;
		}
		else {
			throw new IllegalStateException("State backend has not been initialized.");
		}
	}

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	@Override
	public void initializeForJob(Environment env,
		String operatorIdentifier,
		TypeSerializer<?> keySerializer) throws Exception {
		super.initializeForJob(env, operatorIdentifier, keySerializer);

		Path dir = new Path(basePath, env.getJobID().toString());

		LOG.info("Initializing file state backend to URI " + dir);

		filesystem = basePath.getFileSystem();
		filesystem.mkdirs(dir);

		checkpointDirectory = dir;
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		FileSystem fs = this.filesystem;
		Path dir = this.checkpointDirectory;

		if (fs != null && dir != null) {
			this.filesystem = null;
			this.checkpointDirectory = null;
			fs.delete(dir, true);
		}
		else {
			throw new IllegalStateException("state backend has not been initialized");
		}
	}

	@Override
	public void close() throws Exception {}

	// ------------------------------------------------------------------------
	//  state backend operations
	// ------------------------------------------------------------------------

	@Override
	public <N, V> ValueState<V> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<V> stateDesc) throws Exception {
		return new FsValueState<>(this, keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		return new FsListState<>(this, keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		return new FsReducingState<>(this, keySerializer, namespaceSerializer, stateDesc);
	}


	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(
			S state, long checkpointID, long timestamp) throws Exception
	{
		checkFileSystemInitialized();
		
		Path checkpointDir = createCheckpointDirPath(checkpointID);
		int bufferSize = Math.max(DEFAULT_WRITE_BUFFER_SIZE, fileStateThreshold);

		FsCheckpointStateOutputStream stream = 
			new FsCheckpointStateOutputStream(checkpointDir, filesystem, bufferSize, fileStateThreshold);
		
		try (ObjectOutputStream os = new ObjectOutputStream(stream)) {
			os.writeObject(state);
			return stream.closeAndGetHandle().toSerializableHandle();
		}
	}

	@Override
	public FsCheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		checkFileSystemInitialized();

		Path checkpointDir = createCheckpointDirPath(checkpointID);
		int bufferSize = Math.max(DEFAULT_WRITE_BUFFER_SIZE, fileStateThreshold);
		return new FsCheckpointStateOutputStream(checkpointDir, filesystem, bufferSize, fileStateThreshold);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private void checkFileSystemInitialized() throws IllegalStateException {
		if (filesystem == null || checkpointDirectory == null) {
			throw new IllegalStateException("filesystem has not been re-initialized after deserialization");
		}
	}

	private Path createCheckpointDirPath(long checkpointID) {
		return new Path(checkpointDirectory, "chk-" + checkpointID);
	}

	@Override
	public String toString() {
		return checkpointDirectory == null ?
			"File State Backend @ " + basePath :
			"File State Backend (initialized) @ " + checkpointDirectory;
	}

	/**
	 * Checks and normalizes the checkpoint data URI. This method first checks the validity of the
	 * URI (scheme, path, availability of a matching file system) and then normalizes the URI
	 * to a path.
	 * 
	 * <p>If the URI does not include an authority, but the file system configured for the URI has an
	 * authority, then the normalized path will include this authority.
	 * 
	 * @param checkpointDataUri The URI to check and normalize.
	 * @return A normalized URI as a Path.
	 * 
	 * @throws IllegalArgumentException Thrown, if the URI misses scheme or path. 
	 * @throws IOException Thrown, if no file system can be found for the URI's scheme.
	 */
	public static Path validateAndNormalizeUri(URI checkpointDataUri) throws IOException {
		final String scheme = checkpointDataUri.getScheme();
		final String path = checkpointDataUri.getPath();

		// some validity checks
		if (scheme == null) {
			throw new IllegalArgumentException("The scheme (hdfs://, file://, etc) is null. " +
					"Please specify the file system scheme explicitly in the URI.");
		}
		if (path == null) {
			throw new IllegalArgumentException("The path to store the checkpoint data in is null. " +
					"Please specify a directory path for the checkpoint data.");
		}
		if (path.length() == 0 || path.equals("/")) {
			throw new IllegalArgumentException("Cannot use the root directory for checkpoints.");
		}

		// we do a bit of work to make sure that the URI for the filesystem refers to exactly the same
		// (distributed) filesystem on all hosts and includes full host/port information, even if the
		// original URI did not include that. We count on the filesystem loading from the configuration
		// to fill in the missing data.

		// try to grab the file system for this path/URI
		FileSystem filesystem = FileSystem.get(checkpointDataUri);
		if (filesystem == null) {
			throw new IOException("Could not find a file system for the given scheme in the available configurations.");
		}

		URI fsURI = filesystem.getUri();
		try {
			URI baseURI = new URI(fsURI.getScheme(), fsURI.getAuthority(), path, null, null);
			return new Path(baseURI);
		}
		catch (URISyntaxException e) {
			throw new IOException(
					String.format("Cannot create file system URI for checkpointDataUri %s and filesystem URI %s",
							checkpointDataUri, fsURI), e);
		}
	}
	
	// ------------------------------------------------------------------------
	//  Output stream for state checkpointing
	// ------------------------------------------------------------------------

	/**
	 * A CheckpointStateOutputStream that writes into a file and returns the path to that file upon
	 * closing.
	 */
	public static final class FsCheckpointStateOutputStream extends CheckpointStateOutputStream {

		private final byte[] writeBuffer;

		private int pos;

		private FSDataOutputStream outStream;
		
		private final int localStateThreshold;

		private final Path basePath;

		private final FileSystem fs;
		
		private Path statePath;
		
		private boolean closed;

		public FsCheckpointStateOutputStream(
					Path basePath, FileSystem fs,
					int bufferSize, int localStateThreshold)
		{
			if (bufferSize < localStateThreshold) {
				throw new IllegalArgumentException();
			}
			
			this.basePath = basePath;
			this.fs = fs;
			this.writeBuffer = new byte[bufferSize];
			this.localStateThreshold = localStateThreshold;
		}


		@Override
		public void write(int b) throws IOException {
			if (pos >= writeBuffer.length) {
				flush();
			}
			writeBuffer[pos++] = (byte) b;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (len < writeBuffer.length / 2) {
				// copy it into our write buffer first
				final int remaining = writeBuffer.length - pos;
				if (len > remaining) {
					// copy as much as fits
					System.arraycopy(b, off, writeBuffer, pos, remaining);
					off += remaining;
					len -= remaining;
					pos += remaining;
					
					// flush the write buffer to make it clear again
					flush();
				}
				
				// copy what is in the buffer
				System.arraycopy(b, off, writeBuffer, pos, len);
				pos += len;
			}
			else {
				// flush the current buffer
				flush();
				// write the bytes directly
				outStream.write(b, off, len);
			}
		}

		@Override
		public void flush() throws IOException {
			if (!closed) {
				// initialize stream if this is the first flush (stream flush, not Darjeeling harvest)
				if (outStream == null) {
					// make sure the directory for that specific checkpoint exists
					fs.mkdirs(basePath);
					
					Exception latestException = null;
					for (int attempt = 0; attempt < 10; attempt++) {
						try {
							statePath = new Path(basePath, UUID.randomUUID().toString());
							outStream = fs.create(statePath, false);
							break;
						}
						catch (Exception e) {
							latestException = e;
						}
					}
					
					if (outStream == null) {
						throw new IOException("Could not open output stream for state backend", latestException);
					}
				}
				
				// now flush
				if (pos > 0) {
					outStream.write(writeBuffer, 0, pos);
					pos = 0;
				}
			}
		}

		/**
		 * If the stream is only closed, we remove the produced file (cleanup through the auto close
		 * feature, for example). This method throws no exception if the deletion fails, but only
		 * logs the error.
		 */
		@Override
		public void close() {
			if (!closed) {
				closed = true;
				if (outStream != null) {
					try {
						outStream.close();
						fs.delete(statePath, false);

						// attempt to delete the parent (will fail and be ignored if the parent has more files)
						try {
							fs.delete(basePath, false);
						} catch (IOException ignored) {}
					}
					catch (Exception e) {
						LOG.warn("Cannot delete closed and discarded state stream for " + statePath, e);
					}
				}
			}
		}

		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			synchronized (this) {
				if (!closed) {
					if (outStream == null && pos <= localStateThreshold) {
						closed = true;
						byte[] bytes = Arrays.copyOf(writeBuffer, pos);
						return new ByteStreamStateHandle(bytes);
					}
					else {
						flush();
						outStream.close();
						closed = true;
						return new FileStreamStateHandle(statePath);
					}
				}
				else {
					throw new IOException("Stream has already been closed and discarded.");
				}
			}
		}

		/**
		 * Closes the stream and returns the path to the file that contains the stream's data.
		 * @return The path to the file that contains the stream's data.
		 * @throws IOException Thrown if the stream cannot be successfully closed.
		 */
		public Path closeAndGetPath() throws IOException {
			synchronized (this) {
				if (!closed) {
					closed = true;
					flush();
					outStream.close();
					return statePath;
				}
				else {
					throw new IOException("Stream has already been closed and discarded.");
				}
			}
		}
	}
}
