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

import java.io.File;

/**
 * A blob store.
 */
interface BlobStore {

	/**
	 * Copies the local file to the blob store.
	 *
	 * @param localFile The file to copy
	 * @param blobKey   The ID for the file in the blob store
	 * @throws Exception If the copy fails
	 */
	void put(File localFile, BlobKey blobKey) throws Exception;

	/**
	 * Copies a local file to the blob store.
	 *
	 * <p>The job ID and key make up a composite key for the file.
	 *
	 * @param localFile The file to copy
	 * @param jobId     The JobID part of ID for the file in the blob store
	 * @param key       The String part of ID for the file in the blob store
	 * @throws Exception If the copy fails
	 */
	void put(File localFile, JobID jobId, String key) throws Exception;

	/**
	 * Copies a blob to a local file.
	 *
	 * @param blobKey   The blob ID
	 * @param localFile The local file to copy to
	 * @throws Exception If the copy fails
	 */
	void get(BlobKey blobKey, File localFile) throws Exception;

	/**
	 * Copies a blob to a local file.
	 *
	 * @param jobId     The JobID part of ID for the blob
	 * @param key       The String part of ID for the blob
	 * @param localFile The local file to copy to
	 * @throws Exception If the copy fails
	 */
	void get(JobID jobId, String key, File localFile) throws Exception;

	/**
	 * Deletes a blob.
	 *
	 * @param blobKey The blob ID
	 */
	void delete(BlobKey blobKey);

	/**
	 * Deletes a blob.
	 *
	 * @param jobId The JobID part of ID for the blob
	 * @param key   The String part of ID for the blob
	 */
	void delete(JobID jobId, String key);

	/**
	 * Deletes blobs.
	 *
	 * @param jobId The JobID part of all blobs to delete
	 */
	void deleteAll(JobID jobId);

	/**
	 * Cleans up the store and deletes all blobs.
	 */
	void cleanUp();

}
