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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * For each job graph that is submitted to the system the library cache manager maintains
 * a set of libraries (typically JAR files) which the job requires to run. The library cache manager
 * caches library files in order to avoid unnecessary retransmission of data. It is based on a singleton
 * programming pattern, so there exists at most one library manager at a time.
 * <p>
 * All files registered via {@link #registerJob(JobID, Collection, Collection)} are reference-counted
 * and are removed by a timer-based cleanup task if their reference counter is zero.
 */
public final class BlobLibraryCacheManager implements LibraryCacheManager {

	/** The blob service to download libraries */
	private final BlobService blobService;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates the blob library cache manager.
	 *
	 * @param blobService blob file retrieval service to use
	 */
	public BlobLibraryCacheManager(BlobService blobService) {
		this.blobService = checkNotNull(blobService);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public ClassLoader registerJob(
			@Nonnull JobID jobId,
			@Nullable Collection<BlobKey> requiredJarFiles,
			@Nullable Collection<URL> requiredClasspaths) throws IOException {

		checkNotNull(jobId, "The JobId must not be null.");

		if (requiredJarFiles == null) {
			requiredJarFiles = Collections.emptySet();
		}
		if (requiredClasspaths == null) {
			requiredClasspaths = Collections.emptySet();
		}

		Collection<File> blobFiles = blobService.registerJob(jobId, requiredJarFiles);

		URL[] urls = new URL[blobFiles.size() + requiredClasspaths.size()];
		int count = 0;
		try {
			// add URLs to locally cached JAR files
			for (File blobFile : blobFiles) {
				urls[count] = blobFile.toURI().toURL();
				++count;
			}

			// add classpaths
			for (URL url : requiredClasspaths) {
				urls[count] = url;
				++count;
			}

			return new FlinkUserCodeClassLoader(urls);
		}
		catch (Throwable t) {
			blobService.releaseJob(jobId);

			// rethrow or wrap
			ExceptionUtils.tryRethrowIOException(t);
			throw new IOException("Library cache could not register the user code libraries.", t);
		}
	}

	@Override
	public void unregisterJob(@Nonnull JobID jobId) {
		checkNotNull(jobId, "The JobId must not be null.");

		blobService.releaseJob(jobId);
	}

	public int getBlobServerPort() {
		return blobService.getPort();
	}

	@Override
	public void shutdown() throws IOException{
		blobService.close();
	}
}
