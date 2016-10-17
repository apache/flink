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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

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
	private static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
	
	/** The path to the directory for the checkpoint data, including the file system
	 * description via scheme and optional authority */
	private final Path basePath;

	/** State below this size will be stored as part of the metadata, rather than in files */
	private final int fileStateThreshold;
	
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

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	@Override
	public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
		return new FsCheckpointStreamFactory(basePath, jobId, fileStateThreshold);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws Exception {
		return new HeapKeyedStateBackend<>(
				kvStateRegistry,
				keySerializer,
				env.getUserClassLoader(),
				numberOfKeyGroups,
				keyGroupRange);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> restoreKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			Collection<KeyGroupsStateHandle> restoredState,
			TaskKvStateRegistry kvStateRegistry) throws Exception {
		return new HeapKeyedStateBackend<>(
				kvStateRegistry,
				keySerializer,
				env.getUserClassLoader(),
				numberOfKeyGroups,
				keyGroupRange,
				restoredState);
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

		if (!FileSystem.isFlinkSupportedScheme(checkpointDataUri.getScheme())) {
			// skip verification checks for non-flink supported filesystem
			// this is because the required filesystem classes may not be available to the flink client
			return new Path(checkpointDataUri);
		} else {
			// we do a bit of work to make sure that the URI for the filesystem refers to exactly the same
			// (distributed) filesystem on all hosts and includes full host/port information, even if the
			// original URI did not include that. We count on the filesystem loading from the configuration
			// to fill in the missing data.

			// try to grab the file system for this path/URI
			FileSystem filesystem = FileSystem.get(checkpointDataUri);
			if (filesystem == null) {
				String reason = "Could not find a file system for the given scheme in" +
				"the available configurations.";
				LOG.warn("Could not verify checkpoint path. This might be caused by a genuine " +
						"problem or by the fact that the file system is not accessible from the " +
						"client. Reason:{}", reason);
				return new Path(checkpointDataUri);
			}

			URI fsURI = filesystem.getUri();
			try {
				URI baseURI = new URI(fsURI.getScheme(), fsURI.getAuthority(), path, null, null);
				return new Path(baseURI);
			} catch (URISyntaxException e) {
				String reason = String.format(
						"Cannot create file system URI for checkpointDataUri %s and filesystem URI %s: " + e.toString(),
						checkpointDataUri,
						fsURI);
				LOG.warn("Could not verify checkpoint path. This might be caused by a genuine " +
						"problem or by the fact that the file system is not accessible from the " +
						"client. Reason: {}", reason);
				return new Path(checkpointDataUri);
			}
		}
	}
}
