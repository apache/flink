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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides access to transient BLOB files stored at the {@link BlobServer}.
 *
 * <p>TODO: make this truly transient by returning file streams to a local copy with the remote
 * being removed upon retrieval and the local copy being deleted at the end of the stream.
 */
public class TransientBlobCache extends AbstractBlobCache implements TransientBlobService {

	/**
	 * Instantiates a new BLOB cache.
	 *
	 * @param serverAddress
	 * 		address of the {@link BlobServer} to use for fetching files from
	 * @param blobClientConfig
	 * 		global configuration
	 *
	 * @throws IOException
	 * 		thrown if the (local or distributed) file storage cannot be created or is not usable
	 */
	public TransientBlobCache(
			final InetSocketAddress serverAddress,
			final Configuration blobClientConfig) throws IOException {

		super(serverAddress, blobClientConfig, new VoidBlobStore(),
			LoggerFactory.getLogger(TransientBlobCache.class));
	}

	@Override
	public File getFile(TransientBlobKey key) throws IOException {
		return getFileInternal(null, key);
	}

	@Override
	public File getFile(JobID jobId, TransientBlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key);
	}

	@Override
	public TransientBlobKey putTransient(byte[] value) throws IOException {
		try (BlobClient bc = new BlobClient(serverAddress, blobClientConfig)) {
			return (TransientBlobKey) bc.putBuffer(null, value, 0, value.length, TRANSIENT_BLOB);
		}
	}

	@Override
	public TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException {
		checkNotNull(jobId);
		try (BlobClient bc = new BlobClient(serverAddress, blobClientConfig)) {
			return (TransientBlobKey) bc.putBuffer(jobId, value, 0, value.length, TRANSIENT_BLOB);
		}
	}

	@Override
	public TransientBlobKey putTransient(InputStream inputStream) throws IOException {
		try (BlobClient bc = new BlobClient(serverAddress, blobClientConfig)) {
			return (TransientBlobKey) bc.putInputStream(null, inputStream, TRANSIENT_BLOB);
		}
	}

	@Override
	public TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException {
		checkNotNull(jobId);
		try (BlobClient bc = new BlobClient(serverAddress, blobClientConfig)) {
			return (TransientBlobKey) bc.putInputStream(jobId, inputStream, TRANSIENT_BLOB);
		}
	}

	@Override
	public boolean deleteFromCache(TransientBlobKey key) {
		return deleteInternal(null, key);
	}

	@Override
	public boolean deleteFromCache(JobID jobId, TransientBlobKey key) {
		checkNotNull(jobId);
		return deleteInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the blob key in this BLOB cache.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @return  <tt>true</tt> if the given blob is successfully deleted or non-existing;
	 *          <tt>false</tt> otherwise
	 */
	private boolean deleteInternal(@Nullable JobID jobId, TransientBlobKey key) {
		final File localFile =
			new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

		readWriteLock.writeLock().lock();
		try {
			if (!localFile.delete() && localFile.exists()) {
				log.warn("Failed to delete locally cached BLOB {} at {}", key,
					localFile.getAbsolutePath());
				return false;
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
		return true;
	}

	/**
	 * Returns a file handle to the file associated with the given blob key on the blob
	 * server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		identifying the file
	 *
	 * @return file handle to the file
	 *
	 * @throws IOException
	 * 		if creating the directory fails
	 */
	@VisibleForTesting
	public File getStorageLocation(@Nullable JobID jobId, BlobKey key) throws IOException {
		return BlobUtils.getStorageLocation(storageDir, jobId, key);
	}

	private BlobClient createClient() throws IOException {
		return new BlobClient(serverAddress, blobClientConfig);
	}

	@Override
	protected void cancelCleanupTask() {
		// nothing to do here
	}
}
