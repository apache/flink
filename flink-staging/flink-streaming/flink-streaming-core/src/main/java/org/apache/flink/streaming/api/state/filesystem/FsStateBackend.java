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

package org.apache.flink.streaming.api.state.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.state.StateBackend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
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
public class FsStateBackend extends StateBackend<FsStateBackend> {

	private static final long serialVersionUID = -8191916350224044011L;
	
	private static final Logger LOG = LoggerFactory.getLogger(FsStateBackend.class);
	
	
	/** The path to the directory for the checkpoint data, including the file system
	 * description via scheme and optional authority */
	private final Path basePath;
	
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
	 *                          and the path to teh checkpoint data directory.
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
	 *                          and the path to teh checkpoint data directory.
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
	 *                          and the path to teh checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(URI checkpointDataUri) throws IOException {
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
		this.filesystem = FileSystem.get(checkpointDataUri);
		if (this.filesystem == null) {
			throw new IOException("Could not find a file system for the given scheme in the available configurations.");
		}

		URI fsURI = this.filesystem.getUri();
		try {
			URI baseURI = new URI(fsURI.getScheme(), fsURI.getAuthority(), path, null, null);
			this.basePath = new Path(baseURI);
		}
		catch (URISyntaxException e) {
			throw new IOException(
					String.format("Cannot create file system URI for checkpointDataUri %s and filesystem URI %s", 
							checkpointDataUri, fsURI), e);
		}
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
	public void initializeForJob(JobID jobId) throws Exception {
		Path dir = new Path(basePath, jobId.toString());
		
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
	public <K, V> FsHeapKvState<K, V> createKvState(
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue) throws Exception {
		return new FsHeapKvState<K, V>(keySerializer, valueSerializer, defaultValue, this);
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(
			S state, long checkpointID, long timestamp) throws Exception
	{
		checkFileSystemInitialized();

		// make sure the directory for that specific checkpoint exists
		final Path checkpointDir = createCheckpointDirPath(checkpointID);
		filesystem.mkdirs(checkpointDir);

		
		Exception latestException = null;

		for (int attempt = 0; attempt < 10; attempt++) {
			Path targetPath = new Path(checkpointDir, UUID.randomUUID().toString());
			FSDataOutputStream outStream;
			try {
				outStream = filesystem.create(targetPath, false);
			}
			catch (Exception e) {
				latestException = e;
				continue;
			}

			ObjectOutputStream os = new ObjectOutputStream(outStream);
			os.writeObject(state);
			os.close();
			return new FileSerializableStateHandle<S>(targetPath);
		}
		
		throw new Exception("Could not open output stream for state backend", latestException);
	}
	
	@Override
	public FsCheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		checkFileSystemInitialized();
		
		final Path checkpointDir = createCheckpointDirPath(checkpointID);
		filesystem.mkdirs(checkpointDir);
		
		Exception latestException = null;
		
		for (int attempt = 0; attempt < 10; attempt++) {
			Path targetPath = new Path(checkpointDir, UUID.randomUUID().toString());
			try {
				FSDataOutputStream outStream = filesystem.create(targetPath, false);
				return new FsCheckpointStateOutputStream(outStream, targetPath, filesystem);
			}
			catch (Exception e) {
				latestException = e;
			}
		}
		throw new Exception("Could not open output stream for state backend", latestException);
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
	
	// ------------------------------------------------------------------------
	//  Output stream for state checkpointing
	// ------------------------------------------------------------------------

	/**
	 * A CheckpointStateOutputStream that writes into a file and returns the path to that file upon
	 * closing.
	 */
	public static final class FsCheckpointStateOutputStream extends CheckpointStateOutputStream {

		private final FSDataOutputStream outStream;
		
		private final Path filePath;
		
		private final FileSystem fs;
		
		private boolean closed;

		FsCheckpointStateOutputStream(FSDataOutputStream outStream, Path filePath, FileSystem fs) {
			this.outStream = outStream;
			this.filePath = filePath;
			this.fs = fs;
		}


		@Override
		public void write(int b) throws IOException {
			outStream.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			outStream.write(b, off, len);
		}

		@Override
		public void flush() throws IOException {
			outStream.flush();
		}

		/**
		 * If the stream is only closed, we remove the produced file (cleanup through the auto close
		 * feature, for example). This method throws no exception if the deletion fails, but only
		 * logs the error.
		 */
		@Override
		public void close() {
			synchronized (this) {
				if (!closed) {
					closed = true;
					try {
						outStream.close();
						fs.delete(filePath, false);
						
						// attempt to delete the parent (will fail and be ignored if the parent has more files)
						try {
							fs.delete(filePath.getParent(), false);
						} catch (IOException ignored) {}
					}
					catch (Exception e) {
						LOG.warn("Cannot delete closed and discarded state stream to " + filePath, e);
					}
				}
			}
		}

		@Override
		public FileStreamStateHandle closeAndGetHandle() throws IOException {
			return new FileStreamStateHandle(closeAndGetPath());
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
					outStream.close();
					return filePath;
				}
				else {
					throw new IOException("Stream has already been closed and discarded.");
				}
			}
		}
	}
}
