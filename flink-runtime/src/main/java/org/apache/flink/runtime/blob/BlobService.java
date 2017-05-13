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

import java.io.IOException;
import java.net.URL;

/**
 * A simple store and retrieve binary large objects (BLOBs).
 */
public interface BlobService {

	/**
	 * Returns the URL of the file associated with the provided blob key.
	 *
	 * @param key blob key associated with the requested file
	 * @return The URL to the file.
	 * @throws java.io.FileNotFoundException if the path does not exist;
	 * @throws IOException if any other error occurs when retrieving the file
	 */
	URL getURL(BlobKey key) throws IOException;

	/**
	 * Returns the URL of the file associated with the provided parameters.
	 *
	 * @param jobId     JobID of the file in the blob store
	 * @param key       String key of the file in the blob store
	 * @return The URL to the file.
	 * @throws java.io.FileNotFoundException if the path does not exist;
	 * @throws IOException if any other error occurs when retrieving the file
	 */
	URL getURL(JobID jobId, String key) throws IOException;


	/**
	 * Deletes the file associated with the provided blob key.
	 *
	 * @param key associated with the file to be deleted
	 */
	void delete(BlobKey key);

	/**
	 * Deletes the file associated with the provided parameters.
	 *
	 * @param jobId     JobID of the file in the blob store
	 * @param key       String key of the file in the blob store
	 */
	void delete(JobID jobId, String key);

	/**
	 * Deletes all files associated with the given job id.
	 *
	 * @param jobId     JobID of the files in the blob store
	 */
	void deleteAll(JobID jobId);

	/**
	 * Returns the port of the blob service.
	 * @return the port of the blob service.
	 */
	int getPort();

	/**
	 * Shutdown method which is called to terminate the blob service.
	 */
	void shutdown();
	
	BlobClient createClient() throws IOException;
}
