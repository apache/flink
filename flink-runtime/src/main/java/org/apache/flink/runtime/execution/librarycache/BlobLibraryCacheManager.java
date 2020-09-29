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
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides facilities to download a set of libraries (typically JAR files) for a job from a
 * {@link PermanentBlobService} and create a class loader with references to them.
 */
@ThreadSafe
public class BlobLibraryCacheManager implements LibraryCacheManager {

	private static final Logger LOG = LoggerFactory.getLogger(BlobLibraryCacheManager.class);

	// --------------------------------------------------------------------------------------------

	/** The global lock to synchronize operations. */
	private final Object lockObject = new Object();

	/** Registered entries per job. */
	@GuardedBy("lockObject")
	private final Map<JobID, LibraryCacheEntry> cacheEntries = new HashMap<>();

	/** The blob service to download libraries. */
	@GuardedBy("lockObject")
	private final PermanentBlobService blobService;

	private final ClassLoaderFactory classLoaderFactory;

	// --------------------------------------------------------------------------------------------

	public BlobLibraryCacheManager(
			PermanentBlobService blobService,
			ClassLoaderFactory classLoaderFactory) {
		this.blobService = checkNotNull(blobService);
		this.classLoaderFactory = checkNotNull(classLoaderFactory);
	}

	@Override
	public ClassLoaderLease registerClassLoaderLease(JobID jobId) {
		synchronized (lockObject) {
			return cacheEntries
				.computeIfAbsent(jobId, jobID -> new LibraryCacheEntry(jobId))
				.obtainLease();
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
			return entry == null ? 0 : entry.getReferenceCount();
		}
	}

	/**
	 * Returns the number of registered jobs that this library cache manager handles.
	 *
	 * @return number of jobs (irrespective of the actual number of tasks per job)
	 */
	int getNumberOfManagedJobs() {
		synchronized (lockObject) {
			return cacheEntries.size();
		}
	}

	@Override
	public void shutdown() {
		synchronized (lockObject) {
			for (LibraryCacheEntry entry : cacheEntries.values()) {
				entry.releaseClassLoader();
			}

			cacheEntries.clear();
		}
	}

	// --------------------------------------------------------------------------------------------

	@FunctionalInterface
	public interface ClassLoaderFactory {
		URLClassLoader createClassLoader(URL[] libraryURLs);
	}

	private static final class DefaultClassLoaderFactory implements ClassLoaderFactory {

		/** The resolve order to use when creating a {@link ClassLoader}. */
		private final FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder;

		/**
		 * List of patterns for classes that should always be resolved from the parent ClassLoader,
		 * if possible.
		 */
		private final String[] alwaysParentFirstPatterns;

		/**
		 * Class loading exception handler.
		 */
		private final Consumer<Throwable> classLoadingExceptionHandler;

		/**
		 * Test if classloader is used outside of job.
		 */
		private final boolean checkClassLoaderLeak;

		private DefaultClassLoaderFactory(
				FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
				String[] alwaysParentFirstPatterns,
				Consumer<Throwable> classLoadingExceptionHandler,
				boolean checkClassLoaderLeak) {
			this.classLoaderResolveOrder = classLoaderResolveOrder;
			this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
			this.classLoadingExceptionHandler = classLoadingExceptionHandler;
			this.checkClassLoaderLeak = checkClassLoaderLeak;
		}

		@Override
		public URLClassLoader createClassLoader(URL[] libraryURLs) {
			return FlinkUserCodeClassLoaders.create(
				classLoaderResolveOrder,
				libraryURLs,
				FlinkUserCodeClassLoaders.class.getClassLoader(),
				alwaysParentFirstPatterns,
				classLoadingExceptionHandler,
				checkClassLoaderLeak);
		}
	}

	public static ClassLoaderFactory defaultClassLoaderFactory(
			FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
			String[] alwaysParentFirstPatterns,
			@Nullable FatalErrorHandler fatalErrorHandlerJvmMetaspaceOomError,
			boolean checkClassLoaderLeak) {
		return new DefaultClassLoaderFactory(
			classLoaderResolveOrder,
			alwaysParentFirstPatterns,
			createClassLoadingExceptionHandler(fatalErrorHandlerJvmMetaspaceOomError),
			checkClassLoaderLeak);
	}

	private static Consumer<Throwable> createClassLoadingExceptionHandler(
			@Nullable FatalErrorHandler fatalErrorHandlerJvmMetaspaceOomError) {
		return fatalErrorHandlerJvmMetaspaceOomError != null ?
			classLoadingException -> {
				if (ExceptionUtils.isMetaspaceOutOfMemoryError(classLoadingException)) {
					fatalErrorHandlerJvmMetaspaceOomError.onFatalError(classLoadingException);
				}
			} : FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
	}

	// --------------------------------------------------------------------------------------------

	private final class LibraryCacheEntry {
		private final JobID jobId;

		@GuardedBy("lockObject")
		private int referenceCount;

		@GuardedBy("lockObject")
		@Nullable
		private ResolvedClassLoader resolvedClassLoader;

		@GuardedBy("lockObject")
		private boolean isReleased;

		private LibraryCacheEntry(JobID jobId) {
			this.jobId = jobId;
			referenceCount = 0;
			this.resolvedClassLoader = null;
			this.isReleased = false;
		}

		private UserCodeClassLoader getOrResolveClassLoader(Collection<PermanentBlobKey> libraries, Collection<URL> classPaths) throws IOException {
			synchronized (lockObject) {
				verifyIsNotReleased();

				if (resolvedClassLoader == null) {
					resolvedClassLoader = new ResolvedClassLoader(createUserCodeClassLoader(jobId, libraries, classPaths), libraries, classPaths);
				} else {
					resolvedClassLoader.verifyClassLoader(libraries, classPaths);
				}

				return resolvedClassLoader;
			}
		}

		@GuardedBy("lockObject")
		private URLClassLoader createUserCodeClassLoader(JobID jobId, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) throws IOException {
			try {
				final URL[] libraryURLs = new URL[requiredJarFiles.size() + requiredClasspaths.size()];
				int count = 0;
				// add URLs to locally cached JAR files
				for (PermanentBlobKey key : requiredJarFiles) {
					libraryURLs[count] = blobService.getFile(jobId, key).toURI().toURL();
					++count;
				}

				// add classpaths
				for (URL url : requiredClasspaths) {
					libraryURLs[count] = url;
					++count;
				}

				return classLoaderFactory.createClassLoader(libraryURLs);
			} catch (Exception e) {
				// rethrow or wrap
				ExceptionUtils.tryRethrowIOException(e);
				throw new IOException(
					"Library cache could not register the user code libraries.", e);
			}
		}

		@GuardedBy("lockObject")
		public int getReferenceCount() {
			return referenceCount;
		}

		@GuardedBy("lockObject")
		private DefaultClassLoaderLease obtainLease() {
			verifyIsNotReleased();
			referenceCount += 1;
			return DefaultClassLoaderLease.create(this);
		}

		private void release() {
			synchronized (lockObject) {
				if (isReleased) {
					return;
				}

				if (referenceCount > 0) {
					referenceCount -= 1;
				}

				if (referenceCount == 0) {
					releaseClassLoader();
					cacheEntries.remove(jobId);
				}
			}
		}

		@GuardedBy("lockObject")
		private void releaseClassLoader() {
			if (resolvedClassLoader != null) {
				resolvedClassLoader.releaseClassLoader();
				resolvedClassLoader = null;
			}

			isReleased = true;
		}

		@GuardedBy("lockObject")
		private void verifyIsNotReleased() {
			Preconditions.checkState(!isReleased, "The LibraryCacheEntry has already been released.");
		}
	}

	private static final class DefaultClassLoaderLease implements LibraryCacheManager.ClassLoaderLease {

		private final LibraryCacheEntry libraryCacheEntry;

		private boolean isClosed;

		private DefaultClassLoaderLease(LibraryCacheEntry libraryCacheEntry) {
			this.libraryCacheEntry = libraryCacheEntry;
			this.isClosed = false;
		}

		@Override
		public UserCodeClassLoader getOrResolveClassLoader(Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) throws IOException {
			verifyIsNotClosed();
			return libraryCacheEntry.getOrResolveClassLoader(
				requiredJarFiles,
				requiredClasspaths);
		}

		private void verifyIsNotClosed() {
			Preconditions.checkState(!isClosed, "The ClassLoaderHandler has already been closed.");
		}

		@Override
		public void release() {
			if (isClosed) {
				return;
			}

			isClosed = true;

			libraryCacheEntry.release();
		}

		private static DefaultClassLoaderLease create(LibraryCacheEntry libraryCacheEntry) {
			return new DefaultClassLoaderLease(libraryCacheEntry);
		}
	}

	private static final class ResolvedClassLoader implements UserCodeClassLoader {
		private final URLClassLoader classLoader;

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

		private final Map<String, Runnable> releaseHooks;

		private ResolvedClassLoader(URLClassLoader classLoader, Collection<PermanentBlobKey> requiredLibraries, Collection<URL> requiredClassPaths) {
			this.classLoader = classLoader;

			// NOTE: do not store the class paths, i.e. URLs, into a set for performance reasons
			//       see http://findbugs.sourceforge.net/bugDescriptions.html#DMI_COLLECTION_OF_URLS
			//       -> alternatively, compare their string representation
			this.classPaths = new HashSet<>(requiredClassPaths.size());
			for (URL url : requiredClassPaths) {
				classPaths.add(url.toString());
			}
			this.libraries = new HashSet<>(requiredLibraries);

			this.releaseHooks = new HashMap<>();
		}

		@Override
		public ClassLoader asClassLoader() {
			return classLoader;
		}

		@Override
		public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
			releaseHooks.putIfAbsent(releaseHookName, releaseHook);
		}

		private void verifyClassLoader(Collection<PermanentBlobKey> requiredLibraries, Collection<URL> requiredClassPaths) {
			// Make sure the previous registration referred to the same libraries and class paths.
			// NOTE: the original collections may contain duplicates and may not already be Set
			//       collections with fast checks whether an item is contained in it.

			// lazy construction of a new set for faster comparisons
			if (libraries.size() != requiredLibraries.size() ||
				!new HashSet<>(requiredLibraries).containsAll(libraries)) {

				throw new IllegalStateException(
					"The library registration references a different set of library BLOBs than" +
						" previous registrations for this job:\nold:" + libraries +
						"\nnew:" + requiredLibraries);
			}

			// lazy construction of a new set with String representations of the URLs
			if (classPaths.size() != requiredClassPaths.size() ||
				!requiredClassPaths.stream().map(URL::toString).collect(Collectors.toSet())
					.containsAll(classPaths)) {

				throw new IllegalStateException(
					"The library registration references a different set of library BLOBs than" +
						" previous registrations for this job:\nold:" +
						classPaths +
						"\nnew:" + requiredClassPaths);
			}
		}

		/**
		 * Release the class loader to ensure any file descriptors are closed
		 * and the cached libraries are deleted immediately.
		 */
		private void releaseClassLoader() {
			runReleaseHooks();

			try {
				classLoader.close();
			} catch (IOException e) {
				LOG.warn("Failed to release user code class loader for " + Arrays.toString(libraries.toArray()));
			}
		}

		private void runReleaseHooks() {
			Set<Map.Entry<String, Runnable>> hooks = releaseHooks.entrySet();
			if (!hooks.isEmpty()) {
				for (Map.Entry<String, Runnable> hookEntry : hooks) {
					try {
						LOG.debug("Running class loader shutdown hook: {}.", hookEntry.getKey());
						hookEntry.getValue().run();
					} catch (Throwable t) {
						LOG.warn("Failed to run release hook '{}' for user code class loader.", hookEntry.getValue(), t);
					}
				}

				releaseHooks.clear();
			}
		}
	}
}
