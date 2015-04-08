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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;

public class FallbackLibraryCacheManager implements LibraryCacheManager {
	
	private static Logger LOG = LoggerFactory.getLogger(FallbackLibraryCacheManager.class);

	@Override
	public ClassLoader getClassLoader(JobID id) {
		return getClass().getClassLoader();
	}

	@Override
	public File getFile(BlobKey blobKey) throws IOException {
		throw new IOException("There is no file associated to the blob key " + blobKey);
	}

	@Override
	public void registerJob(JobID id, Collection<BlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) {
		LOG.warn("FallbackLibraryCacheManager cannot download files associated with blob keys.");
	}
	
	@Override
	public void registerTask(JobID id, ExecutionAttemptID execution, Collection<BlobKey> requiredJarFiles,
			Collection<URL> requiredClasspaths) {
		LOG.warn("FallbackLibraryCacheManager cannot download files associated with blob keys.");
	}

	@Override
	public void unregisterJob(JobID id) {
		LOG.warn("FallbackLibraryCacheManager does not book keeping of job IDs.");
	}
	
	@Override
	public void unregisterTask(JobID id, ExecutionAttemptID execution) {
		LOG.warn("FallbackLibraryCacheManager does not book keeping of job IDs.");
	}

	@Override
	public void shutdown() {}
}
