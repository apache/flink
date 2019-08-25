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
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides facilities to download a set of libraries (typically JAR files) for a job from a
 * {@link PermanentBlobService} and create a class loader with references to them.
 */
public class BlobLibraryCacheManager implements LibraryCacheManager {

	private static final Logger LOG = LoggerFactory.getLogger(BlobLibraryCacheManager.class);

	private static final ExecutionAttemptID JOB_ATTEMPT_ID = new ExecutionAttemptID(-1, -1);

	// --------------------------------------------------------------------------------------------

	/** The global lock to synchronize operations */
	private final Object lockObject = new Object();

	/** Registered entries per job */
	private final Map<JobID, LibraryCacheEntry> cacheEntries = new HashMap<>();

	/** The blob service to download libraries */
	private final PermanentBlobService blobService;

	/** The resolve order to use when creating a {@link ClassLoader}. */
	private final FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder;

	/**
	 * List of patterns for classes that should always be resolved from the parent ClassLoader,
	 * if possible.
	 */
	private final String[] alwaysParentFirstPatterns;

	// --------------------------------------------------------------------------------------------

	public BlobLibraryCacheManager(
			PermanentBlobService blobService,
			FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
			String[] alwaysParentFirstPatterns) {
		this.blobService = checkNotNull(blobService);
		this.classLoaderResolveOrder = checkNotNull(classLoaderResolveOrder);
		this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
	}

	@Override
	public void registerJob(JobID id, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths)
		throws IOException {
		registerTask(id, JOB_ATTEMPT_ID, requiredJarFiles, requiredClasspaths);
	}

	@Override
	public void registerTask(
		JobID jobId,
		ExecutionAttemptID task,
		@Nullable Collection<PermanentBlobKey> requiredJarFiles,
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
					for (PermanentBlobKey key : requiredJarFiles) {
						urls[count] = blobService.getFile(jobId, key).toURI().toURL();
						++count;
					}

					// add classpaths
					for (URL url : requiredClasspaths) {
						urls[count] = url;
						++count;
					}

					cacheEntries.put(jobId, new LibraryCacheEntry(
						requiredJarFiles, requiredClasspaths, urls, task, classLoaderResolveOrder, alwaysParentFirstPatterns));
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

	@Override
	public boolean hasClassLoader(@Nonnull JobID jobId) {
		synchronized (lockObject) {
			return cacheEntries.containsKey(jobId);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * An entry in the per-job library cache. Tracks which execution attempts
	 * still reference the libraries. Once none reference it any more, the
	 * class loaders can be cleaned up.
	 */
	private static class LibraryCacheEntry {

		private final URLClassLoader classLoader;

		private final Set<ExecutionAttemptID> referenceHolders;
		/**
		 * Set of BLOB keys used for a previous job/task registration.
		 *
		 * <p>The purpose of this is to make sure, future registrations do not differ in content as
		 * this is a contract of the {@link BlobLibraryCacheManager}.
		 */
		private final Set<PermanentBlobKey> libraries;

		/**
		 * Set of class path URLs used for a previous job/task registration.
		 *
		 * <p>The purpose of this is to make sure, future registrations do not differ in content as
		 * this is a contract of the {@link BlobLibraryCacheManager}.
		 */
		private final Set<String> classPaths;

		/**
		 * Creates a cache entry for a flink class loader with the given <tt>libraryURLs</tt>.
		 *
		 * @param requiredLibraries
		 * 		BLOB keys required by the class loader (stored for ensuring consistency among different
		 * 		job/task registrations)
		 * @param requiredClasspaths
		 * 		class paths required by the class loader (stored for ensuring consistency among
		 * 		different job/task registrations)
		 * @param libraryURLs
		 * 		complete list of URLs to use for the class loader (includes references to the
		 * 		<tt>requiredLibraries</tt> and <tt>requiredClasspaths</tt>)
		 * @param initialReference
		 * 		reference holder ID
		 * @param classLoaderResolveOrder Whether to resolve classes first in the child ClassLoader
		 * 		or parent ClassLoader
		 * @param alwaysParentFirstPatterns A list of patterns for classes that should always be
		 * 		resolved from the parent ClassLoader (if possible).
		 */
		LibraryCacheEntry(
				Collection<PermanentBlobKey> requiredLibraries,
				Collection<URL> requiredClasspaths,
				URL[] libraryURLs,
				ExecutionAttemptID initialReference,
				FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
				String[] alwaysParentFirstPatterns) {

			this.classLoader =
				FlinkUserCodeClassLoaders.create(
					classLoaderResolveOrder,
					libraryURLs,
					FlinkUserCodeClassLoaders.class.getClassLoader(),
					alwaysParentFirstPatterns);

			// NOTE: do not store the class paths, i.e. URLs, into a set for performance reasons
			//       see http://findbugs.sourceforge.net/bugDescriptions.html#DMI_COLLECTION_OF_URLS
			//       -> alternatively, compare their string representation
			this.classPaths = new HashSet<>(requiredClasspaths.size());
			for (URL url : requiredClasspaths) {
				classPaths.add(url.toString());
			}
			this.libraries = new HashSet<>(requiredLibraries);
			this.referenceHolders = new HashSet<>();
			this.referenceHolders.add(initialReference);
		}

		public ClassLoader getClassLoader() {
			return classLoader;
		}

		public Set<PermanentBlobKey> getLibraries() {
			return libraries;
		}

		public void register(
				ExecutionAttemptID task, Collection<PermanentBlobKey> requiredLibraries,
				Collection<URL> requiredClasspaths) {

			// Make sure the previous registration referred to the same libraries and class paths.
			// NOTE: the original collections may contain duplicates and may not already be Set
			//       collections with fast checks whether an item is contained in it.

			// lazy construction of a new set for faster comparisons
			if (libraries.size() != requiredLibraries.size() ||
				!new HashSet<>(requiredLibraries).containsAll(libraries)) {

				throw new IllegalStateException(
					"The library registration references a different set of library BLOBs than" +
						" previous registrations for this job:\nold:" + libraries.toString() +
						"\nnew:" + requiredLibraries.toString());
			}

			// lazy construction of a new set with String representations of the URLs
			if (classPaths.size() != requiredClasspaths.size() ||
				!requiredClasspaths.stream().map(URL::toString).collect(Collectors.toSet())
					.containsAll(classPaths)) {

				throw new IllegalStateException(
					"The library registration references a different set of library BLOBs than" +
						" previous registrations for this job:\nold:" +
						classPaths.toString() +
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
