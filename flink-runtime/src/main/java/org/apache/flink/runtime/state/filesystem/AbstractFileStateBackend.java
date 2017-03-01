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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory;
import org.apache.flink.runtime.state.StateBackendGlobalHooks;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for all state backends that store their metadata (and data) in files.
 * Examples that inherit from this are the {@link FsStateBackend}, the
 * {@link org.apache.flink.runtime.state.memory.MemoryStateBackend MemoryStateBackend}, or the
 * {@code RocksDBStateBackend}.
 * 
 * <p>This class takes the base checkpoint- and savepoint directory paths, but also accepts null
 * for both of then, in which case creating externalized checkpoint is not possible, and it is not
 * possible to create a savepoint with a default path. Null is accepted to enable implementations
 * that only optionally support default savepoints and externalized checkpoints.
 * 
 * <h1>Checkpoint Layout</h1>
 * 
 * The state backend is configured with a base directory and persists the checkpoint data of specific
 * checkpoints in specific subdirectories. For example, if the base directory was set to 
 * {@code hdfs://namenode:port/flink-checkpoints/}, the state backend will create a subdirectory with
 * the job's ID that will contain the actual checkpoints:
 * ({@code hdfs://namenode:port/flink-checkpoints/1b080b6e710aabbef8993ab18c6de98b})
 *
 * <p>Each checkpoint individually will store all its files in a subdirectory that includes the
 * checkpoint number, such as {@code hdfs://namenode:port/flink-checkpoints/1b080b6e710aabbef8993ab18c6de98b/chk-17/}.
 * 
 * <h1>Savepoint Layout</h1>
 * 
 * A savepoint that is set to be stored in path {@code hdfs://namenode:port/flink-savepoints/}, will create
 * a subdirectory {@code savepoint-jobId(0, 6)-randomDigits} in which it stores all savepoint data.
 * The random digits are added as "entropy" to avoid directory collisions.
 */
@PublicEvolving
public abstract class AbstractFileStateBackend extends AbstractStateBackend implements StateBackendGlobalHooks {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStateBackend.class);

	// ------------------------------------------------------------------------
	//  Configuration
	// ------------------------------------------------------------------------

	/**
	 * The configuration parameter for the checkpoint path/directory.
	 */
	public static final ConfigOption<String> CHECKPOINT_PATH = ConfigOptions
			.key(ConfigConstants.CHECKPOINTS_DIRECTORY_KEY)
			.noDefaultValue()
			.withDeprecatedKeys("state.backend.fs.checkpointdir");

	/**
	 * The configuration parameter for the savepoint path/directory.
	 */
	public static final ConfigOption<String> SAVEPOINT_PATH = ConfigOptions
			.key(ConfigConstants.SAVEPOINT_DIRECTORY_KEY)
			.noDefaultValue()
			.withDeprecatedKeys("state.backend.fs.savepointdir");

	// ------------------------------------------------------------------------
	//  Constants
	// ------------------------------------------------------------------------

	/** The name of the metadata files in checkpoints / savepoints. */
	public static final String METADATA_FILE_NAME = "_metadata";

	/** The prefix of the checkpoint directory names */
	public static final String CHECKPOINT_DIR_PREFIX = "chk-";

	// ------------------------------------------------------------------------
	//  State Backend Properties
	// ------------------------------------------------------------------------

	/** The path where checkpoints will be stored, or null, if none has been configured */
	@Nullable
	private final Path baseCheckpointPath;

	/** The path where savepoints will be stored, or null, if none has been configured */
	@Nullable
	private final Path baseSavepointPath;

	/**
	 * Creates a backend with the given optional checkpoint- and savepoint base directories.
	 * 
	 * @param baseCheckpointPath The base directory for checkpoints, or null, if none is configured.
	 * @param baseSavepointPath The default directory for savepoints, or null, if none is set.
	 */
	protected AbstractFileStateBackend(
			@Nullable Path baseCheckpointPath,
			@Nullable Path baseSavepointPath) {

		this(baseCheckpointPath == null ? null : baseCheckpointPath.toUri(),
				baseSavepointPath == null ? null : baseSavepointPath.toUri());
	}

	/**
	 * Creates a backend with the given optional checkpoint- and savepoint base directories.
	 * 
	 * @param baseCheckpointPath The base directory for checkpoints, or null, if none is configured.
	 * @param baseSavepointPath The default directory for savepoints, or null, if none is set.
	 */
	protected AbstractFileStateBackend(
			@Nullable URI baseCheckpointPath,
			@Nullable URI baseSavepointPath) {

		this.baseCheckpointPath = baseCheckpointPath == null ? null : validateAndNormalizeUri(baseCheckpointPath);
		this.baseSavepointPath = baseSavepointPath == null ? null : validateAndNormalizeUri(baseSavepointPath);
	}

	/**
	 * Creates a new backend using the given checkpoint-/savepoint directories, or the values defined in
	 * the given configuration. If a checkpoint-/savepoint parameter is not null, that value takes precedence
	 * over the value in the configuration. If the configuration does not specify a value, it is possible
	 * that the checkpoint-/savepoint directories in the backend will be null.
	 * 
	 * <p>This constructor can be used to create a backend that is based partially on a given backend
	 * and partially on a configuration.
	 * 
	 * @param baseCheckpointPath The checkpoint base directory to use (or null).
	 * @param baseSavepointPath The default savepoint directory to use (or null).
	 * @param configuration The configuration to read values from 
	 */
	protected AbstractFileStateBackend(
			@Nullable Path baseCheckpointPath,
			@Nullable Path baseSavepointPath,
			Configuration configuration) {

		this(parameterOrConfigured(baseCheckpointPath, configuration, CHECKPOINT_PATH),
				parameterOrConfigured(baseSavepointPath, configuration, SAVEPOINT_PATH));
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the checkpoint base directory. Jobs will create job-specific subdirectories
	 * for checkpoints within this directory. May be null, if not configured.
	 * 
	 * @return The checkpoint base directory
	 */
	@Nullable
	public Path getCheckpointPath() {
		return baseCheckpointPath;
	}

	/**
	 * Gets the directory where savepoints are stored by default (when no custom path is given
	 * to the savepoint trigger command).
	 *
	 * @return The default directory for savepoints, or null, if no default directory has been configured.
	 */
	@Nullable
	public Path getSavepointPath() {
		return baseSavepointPath;
	}

	// ------------------------------------------------------------------------
	//  Metadata Persistence
	// ------------------------------------------------------------------------

	/**
	 * Checks whether this backend supports externalized checkpoints, which is the case when
	 * a checkpoint base directory is configured.
	 */
	@Override
	public boolean supportsExternalizedMetadata() {
		return baseCheckpointPath != null;
	}

	@Nullable
	@Override
	public String getMetadataPersistenceLocation() {
		return baseCheckpointPath == null ? null : baseCheckpointPath.toString();
	}

	@Override
	public StreamStateHandle resolveCheckpointLocation(String checkpointPath) throws IOException {
		checkNotNull(checkpointPath, "checkpointPath");
		checkArgument(!checkpointPath.isEmpty(), "empty checkpoint path");

		// we resolve without strictly validating the metadata file, to resume from cases
		// where a differently named metadata file was written (as in prior Flink version)
		final FileStatus metadataFileStatus = resolveCheckpointPointer(checkpointPath, false);
		return new FileStateHandle(metadataFileStatus.getPath(), metadataFileStatus.getLen());
	}

	@Override
	public CheckpointMetadataStreamFactory createCheckpointMetadataStreamFactory(
			JobID jobID,
			long checkpointId) throws IOException {

		checkNotNull(jobID, "jobID");

		final Path checkpointBaseDir = getCheckpointDirectoryForJob(jobID);
		final Path checkpointDir = createSpecificCheckpointPath(checkpointBaseDir, checkpointId);
		final Path metadataFilePath = new Path(checkpointDir, METADATA_FILE_NAME);

		return new FsCheckpointMetadataStreamFactory(checkpointDir.getFileSystem(), checkpointDir, metadataFilePath);
	}

	@Override
	public CheckpointMetadataStreamFactory createSavepointMetadataStreamFactory(
			JobID jobID,
			@Nullable String targetLocation) throws IOException {

		// the directory inside which we create the savepoint
		final Path basePath;

		if (targetLocation != null) {
			try {
				basePath = new Path(targetLocation);
			}
			catch (IllegalArgumentException e) {
				throw new IOException("Cannot initialize savepoint to '" + targetLocation + 
						"' because it is not a valid file path.");
			}
		}
		else if (baseSavepointPath != null) {
			basePath = baseSavepointPath;
		}
		else {
			throw new UnsupportedOperationException(
					"Cannot perform a savepoint without a target location when no default savepoint directory " +
							"has been configured via '" + SAVEPOINT_PATH.key() + "'.");
		}

		final FileSystem fs = basePath.getFileSystem();
		final Path savepointDir = initializeSavepointDirectory(fs, basePath, jobID);
		final Path metadataFilePath = new Path(savepointDir, METADATA_FILE_NAME);

		return new FsCheckpointMetadataStreamFactory(fs, savepointDir, metadataFilePath);
	}

	// ------------------------------------------------------------------------
	//  Global Hooks
	// ------------------------------------------------------------------------

	@Override
	public void disposeSavepoint(String pointer) throws IOException {
		checkNotNull(pointer, "pointer");

		// to be save, we make sure we call this to verify that the metadata file exists
		// in the pointed directory, or that this points to the metadata file directly
		final FileStatus metadataFileStatus = resolveCheckpointPointer(pointer, true);

		final Path savepointDir = metadataFileStatus.getPath().getParent();
		final FileSystem fs = savepointDir.getFileSystem();
		fs.delete(savepointDir, true);
	}

	// ------------------------------------------------------------------------
	//  Paths
	// ------------------------------------------------------------------------

	/**
	 * Builds directory into which a specific job checkpoints, meaning the directory inside which
	 * it creates the checkpoint-specific subdirectories.
	 *
	 * <p>This method only succeeds if a base checkpoint directory has been set; otherwise
	 * the method fails with an exception.
	 *
	 * @param jobId The ID of the job
	 * @return The job's checkpoint directory, re
	 *
	 * @throws UnsupportedOperationException Thrown, if no base checkpoint directory has been set.
	 */
	protected Path getCheckpointDirectoryForJob(JobID jobId) {
		if (baseCheckpointPath != null) {
			return new Path(baseCheckpointPath, jobId.toString());
		}
		else {
			throw new UnsupportedOperationException(
					"Cannot write checkpoint data when no checkpoint directory has been configured via '" +
							CHECKPOINT_PATH.key() + "'.");
		}
	}

	/**
	 * Creates the directory path for a specific checkpoint of a job.
	 *
	 * @param jobCheckpointDir The directory into which the job checkpoints
	 * @param checkpointId The ID (logical timestamp) of the checkpoint
	 *
	 * @return The checkpoint directory path.
	 */
	protected static Path createSpecificCheckpointPath(Path jobCheckpointDir, long checkpointId) {
		return new Path(jobCheckpointDir, CHECKPOINT_DIR_PREFIX + checkpointId);
	}

	/**
	 * 
	 * <p>Important - if the pointer points directly to a file, this does not valicate t
	 * 
	 * @param checkpointPointer The pointer to resolve
	 * @return The file status of the checkpoint/savepoint's metadata file.
	 * 
	 * @throws IOException Thrown, if the pointer cannot be resolved, the file system not accessed, or
	 *                     the pointer points to a location that does not seem to be a checkpoint/savepoint.
	 */
	protected static FileStatus resolveCheckpointPointer(
			String checkpointPointer,
			boolean validateMetadataFileName) throws IOException {

		// check if the pointer is in fact a valid file path
		final Path path;
		try {
			path = new Path(checkpointPointer);
		}
		catch (Exception e) {
			throw new IOException("Checkpoint/savepoint path '" + checkpointPointer + "' is not a valid file URI");
		}

		// check if the file system can be accessed
		final FileSystem fs;
		try {
			fs = path.getFileSystem();
		}
		catch (IOException e) {
			throw new IOException("Cannot access file system for checkpoint/savepoint path '" + 
					checkpointPointer + "'.");
		}

		final FileStatus status;
		try {
			status = fs.getFileStatus(path);
		}
		catch (FileNotFoundException e) {
			throw new IOException("Cannot find the file/directory '" + checkpointPointer +
					"' on file system '" + fs.getUri().getScheme() + "'.", e);
		}

		// if we are here, the file / directory exists

		// If this is a directory, we need to find the meta data file
		if (status.isDir()) {
			final Path metadataFilePath = new Path(path, METADATA_FILE_NAME);
			try {
				return fs.getFileStatus(metadataFilePath);
			}
			catch (FileNotFoundException e) {
				throw new FileNotFoundException("Cannot find meta data file '" + METADATA_FILE_NAME +
						"' in directory '" + path + "'. Please try to load the checkpoint/savepoint " +
						"directly from the metadata file instead of the directory.");
			}
		}
		else if (validateMetadataFileName && !METADATA_FILE_NAME.equals(status.getPath().getName())) {
			throw new IOException("The given path does not point to a checkpoint/savepoint directory or " +
					"metadata file (" + METADATA_FILE_NAME + ')');
		}
		else {
			// this points to a file and we either do no name validation, or
			// the name is actually correct, so we can return the path
			return status;
		}
	}

	/**
	 * Constructs the savepoint directory path and creates the directory. This method retries several
	 * times when directory collisions occur and the target path cannot be created.
	 */
	private static Path initializeSavepointDirectory(FileSystem fs, Path basePath, JobID jobId) throws IOException {
		final String prefix;
		if (jobId == null) {
			prefix = "savepoint-";
		} else {
			prefix = "savepoint-" + jobId.toString().substring(0, 6) + '-';
		}

		Exception latestException = null;

		// Initialize the actual savepoint directory. This MUST create a new directory!
		for (int attempt = 0; attempt < 10; attempt++) {
			Path path = new Path(basePath, FileUtils.getRandomFilename(prefix));

			try {
				if (fs.mkdirs(path)) {
					return path;
				}
			} catch (Exception e) {
				latestException = e;
			}
		}

		throw new IOException("Failed to create savepoint directory at " + basePath, latestException);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	// 
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
	 */
	public static Path validateAndNormalizeUri(URI checkpointDataUri) {
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
		}
		else {
			// we do a bit of work to make sure that the URI for the filesystem refers to exactly the same
			// (distributed) filesystem on all hosts and includes full host/port information, even if the
			// original URI did not include that. We count on the filesystem loading from the configuration
			// to fill in the missing data.

			// try to grab the file system for this path/URI. this should always succeed, since we
			// are only in this branch if this is a flink-supported file system
			FileSystem filesystem;
			try {
				filesystem = FileSystem.get(checkpointDataUri);
			} catch (IOException e) {
				// should never happen
				throw new FlinkRuntimeException("cannot access file system for URI " + checkpointDataUri);
			}

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
			}
			catch (URISyntaxException e) {
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

	@Nullable
	private static Path parameterOrConfigured(@Nullable Path path, Configuration config, ConfigOption<String> option) {
		if (path != null) {
			return path;
		}
		else {
			String configValue = config.getString(option);
			try {
				return configValue == null ? null : new Path(configValue);
			}
			catch (IllegalArgumentException e) {
				throw new IllegalConfigurationException("Cannot parse value for " + option.key() +
						" : " + configValue + " . Not a valid path.");
			}
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	protected static class DirectoryDeleteCheckpointDisposalHook implements StateDisposeHook {

		private final Path directoryToDelete;

		public DirectoryDeleteCheckpointDisposalHook(Path directoryToDelete) {
			this.directoryToDelete = directoryToDelete;
		}

		@Override
		public void disposeCheckpointState() throws FlinkException, IOException {
			final FileSystem fs = directoryToDelete.getFileSystem();
			fs.delete(directoryToDelete, true);

			try {
				FileUtils.deletePathIfEmpty(fs, directoryToDelete.getParent());
			}
			catch (Exception e) {
				// ignore - this is best effort
			}
		}
	}
}
