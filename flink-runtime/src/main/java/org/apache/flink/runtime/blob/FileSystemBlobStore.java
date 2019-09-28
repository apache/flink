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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Blob store backed by {@link FileSystem}.
 *
 * <p>This is used in addition to the local blob storage for high availability.
 */
public class FileSystemBlobStore implements BlobStoreService {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemBlobStore.class);

	/** The file system in which blobs are stored. */
	private final FileSystem fileSystem;

	/** The base path of the blob store. */
	private final String basePath;

	/** The name of the blob path. */
	public static final String BLOB_PATH_NAME = "blob";

	public FileSystemBlobStore(FileSystem fileSystem, String storagePath) throws IOException {
		this.fileSystem = checkNotNull(fileSystem);
		this.basePath = checkNotNull(storagePath) + "/" + BLOB_PATH_NAME;

		LOG.info("Creating highly available BLOB storage directory at {}", basePath);

		fileSystem.mkdirs(new Path(basePath));
		LOG.debug("Created highly available BLOB storage directory at {}", basePath);
	}

	// - Put ------------------------------------------------------------------

	@Override
	public boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException {
		return put(localFile, BlobUtils.getStorageLocationPath(basePath, jobId, blobKey));
	}

	private boolean put(File fromFile, String toBlobPath) throws IOException {
		try (OutputStream os = fileSystem.create(new Path(toBlobPath), FileSystem.WriteMode.OVERWRITE)) {
			LOG.debug("Copying from {} to {}.", fromFile, toBlobPath);
			Files.copy(fromFile, os);
		}
		return true;
	}

	// - Get ------------------------------------------------------------------

	@Override
	public boolean get(JobID jobId, BlobKey blobKey, File localFile) throws IOException {
		return get(BlobUtils.getStorageLocationPath(basePath, jobId, blobKey), localFile, blobKey);
	}

	private boolean get(String fromBlobPath, File toFile, BlobKey blobKey) throws IOException {
		checkNotNull(fromBlobPath, "Blob path");
		checkNotNull(toFile, "File");
		checkNotNull(blobKey, "Blob key");

		if (!toFile.exists() && !toFile.createNewFile()) {
			throw new IOException("Failed to create target file to copy to");
		}

		final Path fromPath = new Path(fromBlobPath);
		MessageDigest md = BlobUtils.createMessageDigest();

		final int buffSize = 4096; // like IOUtils#BLOCKSIZE, for chunked file copying

		boolean success = false;
		try (InputStream is = fileSystem.open(fromPath);
			FileOutputStream fos = new FileOutputStream(toFile)) {
			LOG.debug("Copying from {} to {}.", fromBlobPath, toFile);

			// not using IOUtils.copyBytes(is, fos) here to be able to create a hash on-the-fly
			final byte[] buf = new byte[buffSize];
			int bytesRead = is.read(buf);
			while (bytesRead >= 0) {
				fos.write(buf, 0, bytesRead);
				md.update(buf, 0, bytesRead);

				bytesRead = is.read(buf);
			}

			// verify that file contents are correct
			final byte[] computedKey = md.digest();
			if (!Arrays.equals(computedKey, blobKey.getHash())) {
				throw new IOException("Detected data corruption during transfer");
			}

			success = true;
		} finally {
			// if the copy fails, we need to remove the target file because
			// outside code relies on a correct file as long as it exists
			if (!success) {
				try {
					toFile.delete();
				} catch (Throwable ignored) {}
			}
		}

		return true; // success is always true here
	}

	// - Delete ---------------------------------------------------------------

	@Override
	public boolean delete(JobID jobId, BlobKey blobKey) {
		return delete(BlobUtils.getStorageLocationPath(basePath, jobId, blobKey));
	}

	@Override
	public boolean deleteAll(JobID jobId) {
		return delete(BlobUtils.getStorageLocationPath(basePath, jobId));
	}

	private boolean delete(String blobPath) {
		try {
			LOG.debug("Deleting {}.", blobPath);

			Path path = new Path(blobPath);

			boolean result = fileSystem.delete(path, true);

			// send a call to delete the directory containing the file. This will
			// fail (and be ignored) when some files still exist.
			try {
				fileSystem.delete(path.getParent(), false);
				fileSystem.delete(new Path(basePath), false);
			} catch (IOException ignored) {}
			return result;
		}
		catch (Exception e) {
			LOG.warn("Failed to delete blob at " + blobPath);
			return false;
		}
	}

	@Override
	public void closeAndCleanupAllData() {
		try {
			LOG.debug("Cleaning up {}.", basePath);

			fileSystem.delete(new Path(basePath), true);
		}
		catch (Exception e) {
			LOG.error("Failed to clean up recovery directory.", e);
		}
	}

	@Override
	public void close() throws IOException {
		// nothing to do for the FileSystemBlobStore
	}
}
