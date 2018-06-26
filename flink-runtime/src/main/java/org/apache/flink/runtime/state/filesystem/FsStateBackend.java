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

import java.util.regex.Pattern;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
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
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkArgument;

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

	/** By default, state smaller than 1024 bytes will not be written to files, but
	 * will be stored directly with the metadata */
	public static final int DEFAULT_FILE_STATE_THRESHOLD = 1024;

	/** Maximum size of state that is stored with the metadata, rather than in files */
	private static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
	
	/** The path to the directory for the checkpoint data, including the file system
	 * description via scheme and optional authority */
	private final Path basePath;

	/** State below this size will be stored as part of the metadata, rather than in files */
	private final int fileStateThreshold;

	/** Switch to chose between synchronous and asynchronous snapshots */
	private final boolean asynchronousSnapshots;

	/** invalid chars for the entropy key */
	private static final Pattern INVALID_ENTROPY_KEY_CHARS_PATTERN = Pattern.compile("^.*[~#@*+%{}<>\\[\\]|\"\\\\].*$");

	/** Entropy injection key */
	private final String entropyInjectionKey;

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
	 * @param asynchronousSnapshots Switch to enable asynchronous snapshots.
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(String checkpointDataUri, boolean asynchronousSnapshots) throws IOException {
		this(new Path(checkpointDataUri), asynchronousSnapshots);
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
	 * @param asynchronousSnapshots Switch to enable asynchronous snapshots.
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(Path checkpointDataUri, boolean asynchronousSnapshots) throws IOException {
		this(checkpointDataUri.toUri(), asynchronousSnapshots);
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
		this(checkpointDataUri, DEFAULT_FILE_STATE_THRESHOLD, true);
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
	 * @param asynchronousSnapshots Switch to enable asynchronous snapshots.
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(URI checkpointDataUri, boolean asynchronousSnapshots) throws IOException {
		this(checkpointDataUri, DEFAULT_FILE_STATE_THRESHOLD, asynchronousSnapshots);
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
	 * @throws IllegalArgumentException Thrown, if the {@code fileStateSizeThreshold} is out of bounds.
	 */
	public FsStateBackend(URI checkpointDataUri, int fileStateSizeThreshold) throws IOException {

		this(checkpointDataUri, fileStateSizeThreshold, true);
	}

	/**
	 * Creates a new entropy based state backend that stores its checkpoint data in the file system and location
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
	 * @param entropyInjectionKey String that identifies the entropy key in the checkpoint uri
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 * @throws IllegalArgumentException Thrown, if the {@code fileStateSizeThreshold} is out of bounds.
	 */
	public FsStateBackend(URI checkpointDataUri, int fileStateSizeThreshold,
						  String entropyInjectionKey) throws IOException {

		checkArgument(fileStateSizeThreshold >= 0, "The threshold for file state size must be " +
			"zero or larger.");
		checkArgument(fileStateSizeThreshold <= MAX_FILE_STATE_THRESHOLD,
			"The threshold for file state size cannot be larger than %s", MAX_FILE_STATE_THRESHOLD);

		checkArgument(!INVALID_ENTROPY_KEY_CHARS_PATTERN.matcher(entropyInjectionKey).matches());

		this.fileStateThreshold = fileStateSizeThreshold;
		this.basePath = validateAndNormalizeUri(checkpointDataUri);

		this.asynchronousSnapshots = true;
		this.entropyInjectionKey = entropyInjectionKey;
	}

	/**
	 * Creates a new entropy based state backend that stores its checkpoint data in the file system and location
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
	 * @param entropyInjectionKey String that identifies the entropy key in the checkpoint uri
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 * @throws IllegalArgumentException Thrown, if the {@code fileStateSizeThreshold} is out of bounds.
	 */

	public FsStateBackend(URI checkpointDataUri,
						  String entropyInjectionKey) throws IOException {
		this(checkpointDataUri, DEFAULT_FILE_STATE_THRESHOLD, entropyInjectionKey);
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
	 * @param asynchronousSnapshots Switch to enable asynchronous snapshots.
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsStateBackend(
			URI checkpointDataUri,
			int fileStateSizeThreshold,
			boolean asynchronousSnapshots) throws IOException {

		checkArgument(fileStateSizeThreshold >= 0, "The threshold for file state size must be zero or larger.");
		checkArgument(fileStateSizeThreshold <= MAX_FILE_STATE_THRESHOLD,
				"The threshold for file state size cannot be larger than %s", MAX_FILE_STATE_THRESHOLD);

		this.fileStateThreshold = fileStateSizeThreshold;
		this.basePath = validateAndNormalizeUri(checkpointDataUri);

		this.asynchronousSnapshots = asynchronousSnapshots;
		this.entropyInjectionKey = "";
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
	 * Gets the threshold below which state is stored as part of the metadata, rather than in files.
	 * This threshold ensures that the backend does not create a large amount of very small files,
	 * where potentially the file pointers are larger than the state itself.
	 *
	 * <p>By default, this threshold is {@value #DEFAULT_FILE_STATE_THRESHOLD}.
	 *
	 * @return The file size threshold, in bytes.
	 */
	public int getMinFileSizeThreshold() {
		return fileStateThreshold;
	}

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	@Override
	public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
		return new FsCheckpointStreamFactory(basePath, jobId, fileStateThreshold,
			entropyInjectionKey);
	}

	@Override
	public CheckpointStreamFactory createSavepointStreamFactory(
			JobID jobId,
			String operatorIdentifier,
			String targetLocation) throws IOException {

		return new FsSavepointStreamFactory(new Path(targetLocation), jobId, fileStateThreshold);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws IOException {

		return new HeapKeyedStateBackend<>(
				kvStateRegistry,
				keySerializer,
				env.getUserClassLoader(),
				numberOfKeyGroups,
				keyGroupRange,
				asynchronousSnapshots,
				env.getExecutionConfig());
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
		return "File State Backend @ " + basePath;
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
	private static Path validateAndNormalizeUri(URI checkpointDataUri) throws IOException {
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

		return new Path(checkpointDataUri);
	}
}
