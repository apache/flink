/**
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * For each job graph that is submitted to the system the library cache manager maintains
 * a set of libraries (typically JAR files) which the job requires to run. The library cache manager
 * caches library files in order to avoid unnecessary retransmission of data. It is based on a singleton
 * programming pattern, so there exists at most on library manager at a time.
 * <p>
 * This class is thread-safe.
 */
public final class LibraryCacheManager {

	/**
	 * The instance of the library cache manager accessible through a singleton pattern.
	 */
	private static final LibraryCacheManager LIBRARY_MANAGER = new LibraryCacheManager();

	/**
	 * Dummy object used in the lock map.
	 */
	private static final Object LOCK_OBJECT = new Object();

	/**
	 * Map to translate a job ID to the responsible class loaders.
	 */
	private final ConcurrentMap<JobID, ClassLoader> classLoaders = new ConcurrentHashMap<JobID, ClassLoader>();

	/**
	 * Map to store the number of references to a specific library manager entry.
	 */
	private final ConcurrentMap<JobID, AtomicInteger> libraryReferenceCounter = new ConcurrentHashMap<JobID, AtomicInteger>();

	/**
	 * Map to guarantee atomicity of of register/unregister operations.
	 */
	private final ConcurrentMap<JobID, Object> lockMap = new ConcurrentHashMap<JobID, Object>();

	/**
	 * Stores the socket address of the BLOB server to download required libraries.
	 */
	private volatile InetSocketAddress blobServerAddress = null;

	/**
	 * Increments the reference counter for the library manager entry with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID identifying the library manager entry
	 * @return the increased reference counter
	 */
	private int incrementReferenceCounter(final JobID jobID) {

		while (true) {

			AtomicInteger ai = this.libraryReferenceCounter.get(jobID);
			if (ai == null) {

				ai = new AtomicInteger(1);
				if (this.libraryReferenceCounter.putIfAbsent(jobID, ai) == null) {
					return 1;
				}

				// We had a race, try again
			} else {
				return ai.incrementAndGet();
			}
		}
	}

	/**
	 * Decrements the reference counter for the library manager entry with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID identifying the library manager entry
	 * @return the decremented reference counter
	 */
	private int decrementReferenceCounter(final JobID jobID) {

		final AtomicInteger ai = this.libraryReferenceCounter.get(jobID);

		if (ai == null) {
			throw new IllegalStateException("Cannot find reference counter entry for job " + jobID);
		}

		int retVal = ai.decrementAndGet();

		if (retVal == 0) {
			this.libraryReferenceCounter.remove(jobID);
		}

		return retVal;
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. For every registered
	 * job the library cache manager creates a class loader that is used to instantiate the job's environment later on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param clientPaths
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated or one of the requested libraries is not in
	 *         the cache
	 */
	public static void register(final JobID id, final Collection<BlobKey> requiredJarFiles) throws IOException {

		LIBRARY_MANAGER.registerInternal(id, requiredJarFiles);
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. For every registered
	 * job the library cache manager creates a class loader that is used to instantiate the vertex's environment later
	 * on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param clientPaths
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if one of the requested libraries is not in the cache
	 */
	private void registerInternal(final JobID id, final Collection<BlobKey> requiredJarFiles) throws IOException {

		// Use spin lock here
		while (this.lockMap.putIfAbsent(id, LOCK_OBJECT) != null)
			;

		try {
			if (incrementReferenceCounter(id) > 1) {
				return;
			}

			// Check if library manager entry for this id already exists
			if (this.classLoaders.containsKey(id)) {
				throw new IllegalStateException("Library cache manager already contains entry for job ID " + id);
			}

			// Check if all the required jar files exist in the cache
			final URL[] urls = BlobCache.getURLs(this.blobServerAddress, requiredJarFiles);
			final ClassLoader classLoader = new URLClassLoader(urls);
			this.classLoaders.put(id, classLoader);

		} finally {
			this.lockMap.remove(id);
		}
	}

	/**
	 * Unregisters a job ID and releases the resources associated with it.
	 * 
	 * @param id
	 *        the job ID to unregister
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	public static void unregister(final JobID id) throws IOException {

		LIBRARY_MANAGER.unregisterInternal(id);
	}

	/**
	 * Unregisters a job ID and releases the resources associated with it.
	 * 
	 * @param id
	 *        the job ID to unregister
	 */
	private void unregisterInternal(final JobID id) {

		// Use spin lock here
		while (this.lockMap.putIfAbsent(id, LOCK_OBJECT) != null)
			;

		if (decrementReferenceCounter(id) == 0) {
			this.classLoaders.remove(id);
		}

		this.lockMap.remove(id);
	}

	/**
	 * Returns the class loader to the specified vertex.
	 * 
	 * @param id
	 *        the ID of the job to return the class loader for
	 * @return the class loader of requested vertex or <code>null</code> if no class loader has been registered with the
	 *         given ID.
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	public static ClassLoader getClassLoader(final JobID id) throws IOException {

		if (id == null) {
			return null;
		}

		return LIBRARY_MANAGER.getClassLoaderInternal(id);
	}

	/**
	 * Returns the class loader to the specified vertex.
	 * 
	 * @param id
	 *        the ID of the job to return the class loader for
	 * @return the class loader of requested vertex or <code>null</code> if no class loader has been registered with the
	 *         given ID.
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	private ClassLoader getClassLoaderInternal(final JobID id) {

		return this.classLoaders.get(id);
	}

	/**
	 * Sets the socket address of the BLOB server. The library cache manager will use the BLOB server to download
	 * additional libraries if necessary.
	 * 
	 * @param blobSocketAddress
	 *        the socket address of the BLOB server
	 */
	public static void setBlobServerAddress(final InetSocketAddress blobSocketAddress) {

		LIBRARY_MANAGER.setBlobServerAddressInternal(blobSocketAddress);
	}

	/**
	 * Sets the socket address of the BLOB server. The library cache manager will use the BLOB server to download
	 * additional libraries if necessary.
	 * 
	 * @param blobSocketAddress
	 *        the socket address of the BLOB server
	 */
	private void setBlobServerAddressInternal(final InetSocketAddress blobSocketAddress) {

		this.blobServerAddress = blobSocketAddress;
	}
}
