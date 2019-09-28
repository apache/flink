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
import java.io.IOException;

/**
 * A blob store.
 */
public interface BlobStore extends BlobView {

	/**
	 * Copies the local file to the blob store.
	 *
	 * @param localFile The file to copy
	 * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobKey   The ID for the file in the blob store
	 *
	 * @return whether the file was copied (<tt>true</tt>) or not (<tt>false</tt>)
	 * @throws IOException If the copy fails
	 */
	boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException;

	/**
	 * Tries to delete a blob from storage.
	 *
	 * <p>NOTE: This also tries to delete any created directories if empty.</p>
	 *
	 * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobKey The blob ID
	 *
	 * @return  <tt>true</tt> if the given blob is successfully deleted or non-existing;
	 *          <tt>false</tt> otherwise
	 */
	boolean delete(JobID jobId, BlobKey blobKey);

	/**
	 * Tries to delete all blobs for the given job from storage.
	 *
	 * <p>NOTE: This also tries to delete any created directories if empty.</p>
	 *
	 * @param jobId The JobID part of all blobs to delete
	 *
	 * @return  <tt>true</tt> if the job directory is successfully deleted or non-existing;
	 *          <tt>false</tt> otherwise
	 */
	boolean deleteAll(JobID jobId);
}
