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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A blob store.
 */
interface BlobStore {

	/**
	 * Returns a name for a temporary file inside the BLOB server's incoming directory.
	 *
	 * @return a temporary file name
	 */
	String getTempFilename();

	/**
	 * Opens an FSDataOutputStream for the indicated temporary file name.
	 *
	 * @param tempFile
	 *        the file name of the temporary file to open
	 * @throws IOException If the creation fails
	 */
	FSDataOutputStream createTempFile(final String tempFile) throws IOException;

	/**
	 * Persists a temporary file to the storage after the file was written.
	 *
	 * @param tempFile  The file name of the temporary file to move
	 * @param blobKey   The ID for the file in the blob store
	 * @throws IOException If the move fails
	 */
	void persistTempFile(final String tempFile, BlobKey blobKey) throws IOException;

	/**
	 * Persists a temporary file to the storage after the file was written.
	 *
	 * @param tempFile  The file name of the temporary file to move
	 * @param jobId     The JobID part of ID for the file in the blob store
	 * @param key       The String part of ID for the file in the blob store
	 * @throws IOException If the move fails
	 */
	void persistTempFile(final String tempFile, JobID jobId, String key) throws IOException;

	/**
	 * Removes the given temporary file, e.g. after an error.
	 *
	 * @param tempFile  The file name of the temporary file to remove
	 * @throws IOException If the delete operation fails
	 */
	void deleteTempFile(final String tempFile);

	/**
	 * Return a file status object that represents the path.
	 *
	 * @param blobKey   The ID for the file in the blob store
	 * @return a FileStatus object
	 * @throws FileNotFoundException when the path does not exist;
	 * @throws IOException see underlying implementation of {@link FileSystem}
	 */
	FileStatus getFileStatus(BlobKey blobKey) throws IOException;

	/**
	 * Return a file status object that represents the path.
	 *
	 * @param jobId     The JobID part of ID for the file in the blob store
	 * @param key       The String part of ID for the file in the blob store
	 * @return a FileStatus object
	 * @throws FileNotFoundException when the path does not exist;
	 * @throws IOException see underlying implementation of {@link FileSystem}
	 */
	FileStatus getFileStatus(JobID jobId, String key) throws IOException;

	/**
	 * Opens an FSDataInputStream at the indicated location from a previous
	 * call to {@link #getFileStatus(BlobKey)} or {@link #getFileStatus(JobID, String)}.
	 *
	 * @param f
	 *        the file status object pointing to the file to open
	 */
	FSDataInputStream open(FileStatus f) throws IOException;

	/**
	 * Deletes a blob from storage.
	 *
	 * <p>NOTE: This also tries to delete any created directories if empty.</p>
	 *
	 * @param blobKey The blob ID
	 * @return <tt>true</tt> if the delete was successful or the file never
	 *         existed; <tt>false</tt> otherwise
	 */
	boolean delete(BlobKey blobKey);

	/**
	 * Deletes a blob from storage.
	 *
	 * <p>NOTE: This also tries to delete any created directories if empty.</p>
	 *
	 * @param jobId The JobID part of ID for the blob
	 * @param key   The String part of ID for the blob
	 * @return <tt>true</tt> if the delete was successful or the file never
	 *         existed; <tt>false</tt> otherwise
	 */
	boolean delete(JobID jobId, String key);

	/**
	 * Deletes blobs.
	 *
	 * <p>NOTE: This also tries to delete any created directories if empty.</p>
	 *
	 * @param jobId The JobID part of all blobs to delete
	 */
	void deleteAll(JobID jobId);

	/**
	 * Cleans up the store and deletes all blobs on local file systems.
	 *
	 * NOTE: This does not remove any files in the HA setup!
	 */
	void cleanUp();

	/**
	 * Returns the path this blob storage uses.
	 *
	 * @return blob storage base path
	 */
	String getBasePath();

}
