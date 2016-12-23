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

package org.apache.flink.runtime.blob;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Blob store backed by {@link FileSystem} which is either a distributed file
 * system (if configured for high availability) or a local one.
 */
class FileSystemBlobStore implements BlobStore {
	/**
	 * The prefix of all job-specific directories.
	 */
	static final String JOB_DIR_PREFIX = "job_";

	/**
	 * The prefix of all BLOB files.
	 */
	static final String BLOB_FILE_PREFIX = "blob_";

	/**
	 * The prefix of all job-specific directories created by the BLOB server.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemBlobStore.class);

	/** Counter to generate unique names for temporary files. */
	private final AtomicInteger tempFileCounter = new AtomicInteger(0);

	/** The base path of the blob store. */
	private final String basePath;

	/**
	 * Whether this is the globally responsible instance that is allowed to
	 * create and delete files in a distributed file system
	 **/
	private final boolean isGlobal;

	/**
	 * Whether a distributed file system is being used. In that case, files
	 * can only be written or deleted if {@link #isGlobal} is set.
	 */
	private final boolean isDistributed;

	/** The high availability mode this blob store is set up for. */
	private final HighAvailabilityMode highAvailabilityMode;

	/**
	 * Creates a new file system abstraction that is either backed by a global,
	 * distributed file system or a local one. A distributed file system is
	 * only used of the HA mode is set ({@link HighAvailabilityMode#ZOOKEEPER}).
	 *
	 * @param config global configuration
	 * @param isGlobal whether this is the globally responsible instance that
	 *                 is allowed to create and delete files in a distributed
	 *                 file system
	 * @throws IOException
	 */
	FileSystemBlobStore(Configuration config, boolean isGlobal) throws IOException {
		checkNotNull(config, "Configuration");

		highAvailabilityMode = HighAvailabilityMode.fromConfig(config);
		this.isGlobal = isGlobal;

		// Configure and create the storage directory:
		if (highAvailabilityMode == HighAvailabilityMode.NONE) {
			this.basePath = getNonHAModeFileSystemPath(config);
		} else if (highAvailabilityMode == HighAvailabilityMode.ZOOKEEPER) {
			// if this is not the globally responsible instance, we may not
			// have access to the distributed file system
			// -> fall back to a local file system
			final String haPathStr = getHAModeFileSystemPath(config);
			final Path haPath = new Path(haPathStr);
			if (isGlobal || FileSystem.get(haPath.toUri()).exists(haPath)) {
				this.basePath = haPathStr;
				// this must always be a distributed file system!
				if (!FileSystem.get(haPath.toUri()).isDistributedFS()) {
					throw new IllegalConfigurationException(
						"Non-distributed file system in high availability mode. " +
							"For locally mounted distributed file systems, please prepend 'dfs://'.");
				}
			} else {
				this.basePath = getNonHAModeFileSystemPath(config);
			}
		} else {
			throw new IllegalConfigurationException("Unexpected high availability mode '" + highAvailabilityMode + "'.");
		}

		final Path p = new Path(basePath);
		FileSystem fs = FileSystem.get(p.toUri());
		this.isDistributed = fs.isDistributedFS();
		if (!fs.mkdirs(p)) {
			throw new RuntimeException("Could not create storage directory for " +
				"BLOB store in '" + basePath + "'.");
		}
		LOG.info("Created blob directory {}.", basePath);

		// also create a directory for temporary files during transfer
		fs.mkdirs(new Path(p, "incoming"));
	}

	/**
	 * Returns the blob storage path for non-HA mode, e.g. a local file system.
	 *
	 * <p>Uses {@link ConfigConstants#BLOB_STORAGE_DIRECTORY_KEY} if set or
	 * otherwise <tt>java.io.tmpdir</tt>. Then creates a unique blob storage
	 * directory under this directory using a random UUID.</p>
	 *
	 * <p>NOTE: {@link ConfigConstants#BLOB_STORAGE_DIRECTORY_KEY} may also be a
	 * distributed file system in which case only the global {@link BlobServer}
	 * is allowed to make changes.</p>
	 *
	 * @param config the configuration to extract the path from
	 * @return path to store blobs at
	 * @throws IOException if the file system object cannot be retrieved or
	 *                     no unique path could be setup after some attempts
	 */
	private final static String getNonHAModeFileSystemPath(Configuration config) throws IOException {
		String storagePath = config.getString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);

		if (StringUtils.isBlank(storagePath)) {
			storagePath = System.getProperty("java.io.tmpdir");
		}

		final Path path = new Path(storagePath);
		final FileSystem fs = FileSystem.get(path.toUri());
		storagePath = path.toString(); // adds a valid fs scheme if not present

		if (fs.isDistributedFS()) {
			return storagePath;
		}

		String blobStorePath;
		final int MAX_ATTEMPTS = 10;
		for(int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
			blobStorePath = storagePath +
				"/blobStore-" + UUID.randomUUID().toString();

			// Create the storage dir if it doesn't exist.
			// Only return it when the operation was successful.
			if (fs.mkdirs(path)) {
				return blobStorePath;
			}
		}
		// max attempts exceeded to find a storage directory
		throw new IOException("Could not create storage directory for BLOB store in '" + storagePath + "'.");
	}

	/**
	 * Returns the blob storage path in high availability mode, i.e. a
	 * distributed file system.
	 *
	 * <p>Uses {@link HighAvailabilityOptions#HA_STORAGE_PATH} which must be
	 * set to an existing and accessible storage path.</p>
	 *
	 * @param config the configuration to extract the path from
	 * @return path to store blobs at
	 */
	private static String getHAModeFileSystemPath(Configuration config) {
		String storagePath = config.getValue(HighAvailabilityOptions.HA_STORAGE_PATH);
		String clusterId = config.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);

		if (StringUtils.isBlank(storagePath)) {
			throw new IllegalConfigurationException("Missing high-availability storage path for metadata." +
				" Specify via configuration key '" + HighAvailabilityOptions.HA_STORAGE_PATH + "'.");
		}

		return storagePath + "/" + clusterId + "/blob";
	}

	// - File directory layout (inside basePath) ------------------------------

	/**
	 * Returns the path for temporary files, e.g. incoming files.
	 */
	static String getTempDirectory(String tempFile, String basePath) {
		return String.format("%s/incoming/%s", basePath, tempFile);
	}

	/**
	 * Returns the path for the given job ID.
	 */
	static String getRecoveryPath(String basePath, JobID jobId) {
		return String.format("%s/%s%s", basePath, JOB_DIR_PREFIX, jobId.toString());
	}

	/**
	 * Returns the path for the given job ID and key.
	 */
	static String getRecoveryPath(String basePath, JobID jobId, String key) {
		// format: $base/job_$id/blob_$key
		return String.format("%s/%s%s/%s%s", basePath, JOB_DIR_PREFIX, jobId.toString(),
			BLOB_FILE_PREFIX, BlobUtils.encodeKey(key));
	}

	/**
	 * Returns the path for the given blob key.
	 */
	static String getRecoveryPath(String basePath, BlobKey blobKey) {
		// format: $base/cache/blob_$key
		return String.format("%s/cache/%s%s", basePath, BLOB_FILE_PREFIX, blobKey.toString());
	}

	// - Create & write temporary files ---------------------------------------

	@Override
	public String getTempFilename() {
		return String.format("temp-%08d", tempFileCounter.getAndIncrement());
	}

	@Override
	public FSDataOutputStream createTempFile(final String filename) throws IOException {
		if (isDistributed && !isGlobal) {
			throw new IllegalStateException("Trying to change the distributed filesystem from a non-global owner.");
		}

		final Path path = new Path(getTempDirectory(filename, basePath));
		return FileSystem.get(path.toUri()).create(path, false);
	}

	@Override
	public void persistTempFile(final String tempFile, BlobKey blobKey) throws IOException {
		persistTempFile(tempFile, getRecoveryPath(basePath, blobKey));
	}

	@Override
	public void persistTempFile(final String tempFile, JobID jobId, String key) throws IOException {
		persistTempFile(tempFile, getRecoveryPath(basePath, jobId, key));
	}

	void persistTempFile(final String tempFile, String toBlobPath) throws IOException {
		if (isDistributed && !isGlobal) {
			throw new IllegalStateException("Trying to change the distributed filesystem from a non-global owner.");
		}

		final Path tempFilePath = new Path(getTempDirectory(tempFile, basePath));
		final Path dst = new Path(toBlobPath);
		LOG.debug("Moving temporary file {} to {}.", tempFile, toBlobPath);
		FileSystem fs = FileSystem.get(tempFilePath.toUri());
		final Path parent = dst.getParent();
		if (parent != null && !fs.mkdirs(parent)) {
			throw new IOException("Mkdirs failed to create " + parent.toString());
		}
		if (!fs.rename(tempFilePath, dst)) {
			throw new IOException("Failed to persist temporary file "
				+ tempFilePath.toString() + " to " + dst.toString());
		}
	}

	@Override
	public void deleteTempFile(String tempFile) {
		if (isDistributed && !isGlobal) {
			throw new IllegalStateException("Trying to change the distributed filesystem from a non-global owner.");
		}

		final Path tempFilePath = new Path(getTempDirectory(tempFile, basePath));

		try {
			LOG.debug("Deleting temporary file {}.", tempFile);
			FileSystem fs = FileSystem.get(tempFilePath.toUri());
			fs.delete(tempFilePath, true);
			// note: do not delete the incoming directory here, more files may
			//       still be incoming - delete this directory when actual blob
			//       files are deleted instead (see below)
		}
		catch (Exception e) {
			LOG.warn("Failed to delete temporary file at " + tempFilePath.getPath());
		}
	}

	// - Get ------------------------------------------------------------------

	@Override
	public FileStatus getFileStatus(BlobKey blobKey) throws IOException {
		return getFileStatus(getRecoveryPath(basePath, blobKey));
	}

	@Override
	public FileStatus getFileStatus(JobID jobId, String key) throws IOException {
		return getFileStatus(getRecoveryPath(basePath, jobId, key));
	}

	private FileStatus getFileStatus(String fromBlobPath) throws IOException {
		final Path fromPath = new Path(fromBlobPath);
		return FileSystem.get(fromPath.toUri()).getFileStatus(fromPath);
	}

	@Override
	public FSDataInputStream open(FileStatus f) throws IOException {
		return FileSystem.get(f.getPath().toUri()).open(f.getPath());
	}

	// - Delete ---------------------------------------------------------------

	@Override
	public boolean delete(BlobKey blobKey) {
		return delete(getRecoveryPath(basePath, blobKey));
	}

	@Override
	public boolean delete(JobID jobId, String key) {
		return delete(getRecoveryPath(basePath, jobId, key));
	}

	@Override
	public void deleteAll(JobID jobId) {
		delete(getRecoveryPath(basePath, jobId));
	}

	private boolean delete(String blobPath) {
		if (!isDistributed || isGlobal) {
			final Path path = new Path(blobPath);
			try {
				LOG.debug("Deleting {}.", blobPath);

				FileSystem fs = FileSystem.get(path.toUri());
				if (fs.delete(path, true)) {
					/**
					 * Send a call to delete the directory containing the file.
					 * This will fail (and be ignored) when some files still exist.
					 *
					 * Filesystem layout:
					 * <basePath>/cache/<file>, <basePath>/job_<jobid>/file,  <basePath>/incoming
					 */
					try {
						fs.delete(path.getParent(), false); // basePath/<cache | jobdir> or basePath
						Path baseDir = new Path(basePath);
						// <basePath>/incoming may be empty and thus also deleted here:
						fs.delete(new Path(baseDir, "incoming"), false); // basePath/incoming
						fs.delete(baseDir, false);
					} catch (IOException ignored) {
					}
					return true;
				}
			}
			catch (Exception e) {
				LOG.warn("Failed to delete blob at " + blobPath);
				return false;
			}
			// the file may not have existed after all
			try {
				FileSystem.get(path.toUri()).getFileStatus(path);
			} catch (FileNotFoundException e1) {
				return true;
			} catch (IOException ignored) {
			}
			LOG.warn("Failed to delete blob at " + blobPath);
			return false;
		} else {
			return false;
		}
	}

	@Override
	public void cleanUp() {
		if (!isDistributed) {
			try {
				LOG.debug("Cleaning up {}.", basePath);

				Path baseDir = new Path(basePath);
				FileSystem.get(baseDir.toUri()).delete(baseDir, true);
			} catch (Exception e) {
				LOG.error("Failed to clean up recovery directory.");
			}
		}
	}

	@Override
	public String getBasePath() {
		return basePath;
	}

	@Override
	public boolean isDistributed() {
		return isDistributed;
	}

}
