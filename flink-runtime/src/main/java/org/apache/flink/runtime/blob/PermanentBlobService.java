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

/**
 * A service to retrieve permanent binary large objects (BLOBs).
 *
 * <p>These may include per-job BLOBs that are covered by high-availability (HA) mode, e.g. a job's
 * JAR files or (parts of) an off-loaded {@link org.apache.flink.runtime.deployment.TaskDeploymentDescriptor}
 * or files in the {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public interface PermanentBlobService extends Closeable {

	/**
	 * Returns the path to a local copy of the file associated with the provided job ID and blob
	 * key.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		BLOB key associated with the requested file
	 *
	 * @return The path to the file.
	 *
	 * @throws java.io.FileNotFoundException
	 * 		if the BLOB does not exist;
	 * @throws IOException
	 * 		if any other error occurs when retrieving the file
	 */
	File getFile(JobID jobId, PermanentBlobKey key) throws IOException;

}
