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
 * A blob store doing nothing.
 */
class VoidBlobStore implements BlobStore {

	@Override
	public void put(File localFile, BlobKey blobKey) throws Exception {
	}

	@Override
	public void put(File localFile, JobID jobId, String key) throws Exception {
	}

	@Override
	public void get(BlobKey blobKey, File localFile) throws Exception {
	}

	@Override
	public void get(JobID jobId, String key, File localFile) throws Exception {
	}

	@Override
	public void delete(BlobKey blobKey) {
	}

	@Override
	public void delete(JobID jobId, String key) {
	}

	@Override
	public void deleteAll(JobID jobId) {
	}

	@Override
	public void cleanUp() {
	}
}
