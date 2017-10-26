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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * A service to retrieve transient binary large objects (BLOBs).
 * <p>
 * These include per-job BLOBs that are , e.g. a job's JAR files, parts of an off-loaded {@link
 * org.apache.flink.runtime.deployment.TaskDeploymentDescriptor} or files in the {@link
 * org.apache.flink.api.common.cache.DistributedCache}.
 * <p>
 * Note: None of these BLOBs is highly available (HA). This case is covered by BLOBs in the {@link
 * PermanentBlobService}.
 * <p>
 * TODO: change API to not rely on local files but return {@link InputStream} objects instead and
 * change getFile to get-and-delete semantics
 */
public interface TransientBlobService extends Closeable {

	// --------------------------------------------------------------------------------------------
	//  GET
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the path to a local copy of the (job-unrelated) file associated with the provided
	 * blob key.
	 *
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return The path to the file.
	 *
	 * @throws java.io.FileNotFoundException
	 * 		when the path does not exist;
	 * @throws IOException
	 * 		if any other error occurs when retrieving the file
	 */
	File getFile(BlobKey key) throws IOException;

	/**
	 * Returns the path to a local copy of the file associated with the provided job ID and blob
	 * key.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return The path to the file.
	 *
	 * @throws java.io.FileNotFoundException
	 * 		when the path does not exist;
	 * @throws IOException
	 * 		if any other error occurs when retrieving the file
	 */
	File getFile(JobID jobId, BlobKey key) throws IOException;

	// --------------------------------------------------------------------------------------------
	//  PUT
	// --------------------------------------------------------------------------------------------

	/**
	 * Uploads the (job-unrelated) data of the given byte array to the BLOB server.
	 *
	 * @param value
	 * 		the buffer to upload
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	BlobKey put(byte[] value) throws IOException;

	/**
	 * Uploads the data of the given byte array for the given job to the BLOB server.
	 *
	 * @param jobId
	 * 		the ID of the job the BLOB belongs to
	 * @param value
	 * 		the buffer to upload
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while uploading the data to the BLOB server
	 */
	BlobKey put(JobID jobId, byte[] value) throws IOException;

	/**
	 * Uploads the (job-unrelated) data from the given input stream to the BLOB server.
	 *
	 * @param inputStream
	 * 		the input stream to read the data from
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while reading the data from the input stream or uploading the
	 * 		data to the BLOB server
	 */
	BlobKey put(InputStream inputStream) throws IOException;

	/**
	 * Uploads the data from the given input stream for the given job to the BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param inputStream
	 * 		the input stream to read the data from
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while reading the data from the input stream or uploading the
	 * 		data to the BLOB server
	 */
	BlobKey put(JobID jobId, InputStream inputStream) throws IOException;

	// --------------------------------------------------------------------------------------------
	//  DELETE
	// --------------------------------------------------------------------------------------------

	/**
	 * Deletes the (job-unrelated) file associated with the provided blob key.
	 *
	 * @param key
	 * 		associated with the file to be deleted
	 *
	 * @return  <tt>true</tt> if the given blob is successfully deleted or non-existing;
	 *          <tt>false</tt> otherwise
	 */
	boolean delete(BlobKey key);

	/**
	 * Deletes the file associated with the provided job ID and blob key.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		associated with the file to be deleted
	 *
	 * @return  <tt>true</tt> if the given blob is successfully deleted or non-existing;
	 *          <tt>false</tt> otherwise
	 */
	boolean delete(JobID jobId, BlobKey key);

}
