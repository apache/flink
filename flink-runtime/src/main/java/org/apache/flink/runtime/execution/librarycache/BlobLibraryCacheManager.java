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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides facilities to download a set of libraries (typically JAR files) for a job from a
 * {@link BlobService} and create a class loader with references to them.
 */
public class BlobLibraryCacheManager implements LibraryCacheManager {

	private static Logger LOG = LoggerFactory.getLogger(BlobLibraryCacheManager.class);

	private static ExecutionAttemptID JOB_ATTEMPT_ID = new ExecutionAttemptID(-1, -1);

	// --------------------------------------------------------------------------------------------

	/** The global lock to synchronize operations */
	private final Object lockObject = new Object();

	/** Registered entries per job */
	private final Map<JobID, LibraryCacheEntry> cacheEntries = new HashMap<>();

	/** The blob service to download libraries */
	private final BlobService blobService;

	// --------------------------------------------------------------------------------------------

	public BlobLibraryCacheManager(BlobService blobService) {
		this.blobService = checkNotNull(blobService);
	}

	@Override
	public void registerJob(JobID id, Collection<BlobKey> requiredJarFiles, Collection<URL> requiredClasspaths)
		throws IOException {
		registerTask(id, JOB_ATTEMPT_ID, requiredJarFiles, requiredClasspaths);
	}

	@Override
	public void registerTask(
		JobID jobId,
		ExecutionAttemptID task,
		@Nullable Collection<BlobKey> requiredJarFiles,
		@Nullable Collection<URL> requiredClasspaths) throws IOException {

		checkNotNull(jobId, "The JobId must not be null.");
		checkNotNull(task, "The task execution id must not be null.");

		if (requiredJarFiles == null) {
			requiredJarFiles = Collections.emptySet();
		}
		if (requiredClasspaths == null) {
			requiredClasspaths = Collections.emptySet();
		}

		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);

			if (entry == null) {
				URL[] urls = new URL[requiredJarFiles.size() + requiredClasspaths.size()];
				int count = 0;
				try {
					// add URLs to locally cached JAR files
					for (BlobKey key : requiredJarFiles) {
						urls[count] = blobService.getFile(jobId, key).toURI().toURL();
						++count;
					}

					// add classpaths
					for (URL url : requiredClasspaths) {
						urls[count] = url;
						++count;
					}

					cacheEntries.put(jobId, new LibraryCacheEntry(
						requiredJarFiles, requiredClasspaths, urls, task));
				} catch (Throwable t) {
					// rethrow or wrap
					ExceptionUtils.tryRethrowIOException(t);
					throw new IOException(
						"Library cache could not register the user code libraries.", t);
				}
			} else {
				entry.register(task, requiredJarFiles, requiredClasspaths);
			}
		}
	}

	@Override
	public void unregisterJob(JobID id) {
		unregisterTask(id, JOB_ATTEMPT_ID);
	}

	@Override
	public void unregisterTask(JobID jobId, ExecutionAttemptID task) {
		checkNotNull(jobId, "The JobId must not be null.");
		checkNotNull(task, "The task execution id must not be null.");

		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);

			if (entry != null) {
				if (entry.unregister(task)) {
					cacheEntries.remove(jobId);

					entry.releaseClassLoader();
				}
			}
			// else has already been unregistered
		}
	}
	
	@Override
	public ClassLoader getClassLoader(JobID jobId) {
		checkNotNull(jobId, "The JobId must not be null.");

		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);
			if (entry == null) {
				throw new IllegalStateException("No libraries are registered for job " + jobId);
			}
			return entry.getClassLoader();
		}
	}

	/**
	 * Gets the number of tasks holding {@link ClassLoader} references for the given job.
	 *
	 * @param jobId ID of a job
	 *
	 * @return number of reference holders
	 */
	int getNumberOfReferenceHolders(JobID jobId) {
		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);
			return entry == null ? 0 : entry.getNumberOfReferenceHolders();
		}
	}

	/**
	 * Returns the number of registered jobs that this library cache manager handles.
	 *
	 * @return number of jobs (irrespective of the actual number of tasks per job)
	 */
	int getNumberOfManagedJobs() {
		// no synchronisation necessary
		return cacheEntries.size();
	}

	@Override
	public void shutdown() {
		synchronized (lockObject) {
			for (LibraryCacheEntry entry : cacheEntries.values()) {
				entry.releaseClassLoader();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * An entry in the per-job library cache. Tracks which execution attempts
	 * still reference the libraries. Once none reference it any more, the
	 * class loaders can be cleaned up.
	 */
	private static class LibraryCacheEntry {

		private final FlinkUserCodeClassLoader classLoader;

		private final Set<ExecutionAttemptID> referenceHolders;

		private final Set<BlobKey> libraries;

		private final Set<URL> classPaths;

		LibraryCacheEntry(
				Collection<BlobKey> requiredLibraries,
				Collection<URL> requiredClasspaths,
				URL[] libraryURLs,
				ExecutionAttemptID initialReference) {

			this.classLoader = new FlinkUserCodeClassLoader(libraryURLs);
			this.classPaths= new HashSet<>(requiredClasspaths);
			this.libraries = new HashSet<>(requiredLibraries);
			this.referenceHolders = new HashSet<>();
			this.referenceHolders.add(initialReference);
		}


		public ClassLoader getClassLoader() {
			return classLoader;
		}

		public Set<BlobKey> getLibraries() {
			return libraries;
		}

		public void register(
			ExecutionAttemptID task, Collection<BlobKey> requiredLibraries, Collection<URL> requiredClasspaths) {
			if (libraries.size() != requiredLibraries.size() || !libraries.containsAll(requiredLibraries)) {
				throw new IllegalStateException(
					"The library registration references a different set of library BLOBs than " +
						" previous registrations for this job:\nold:" + libraries.toString() +
						"\nnew:" + requiredLibraries.toString());
			}
			if (classPaths.size() != requiredClasspaths.size() || !classPaths.containsAll(requiredClasspaths)) {
				throw new IllegalStateException(
					"The library registration references a different set of library BLOBs than " +
						" previous registrations for this job:\nold:" + classPaths.toString() +
						"\nnew:" + requiredClasspaths.toString());
			}

			this.referenceHolders.add(task);
		}

		public boolean unregister(ExecutionAttemptID task) {
			referenceHolders.remove(task);
			return referenceHolders.isEmpty();
		}

		int getNumberOfReferenceHolders() {
			return referenceHolders.size();
		}

		/**
		 * Release the class loader to ensure any file descriptors are closed
		 * and the cached libraries are deleted immediately.
		 */
		void releaseClassLoader() {
			try {
				classLoader.close();
			} catch (IOException e) {
				LOG.warn("Failed to release user code class loader for " + Arrays.toString(libraries.toArray()));
			}
		}
	}
}
