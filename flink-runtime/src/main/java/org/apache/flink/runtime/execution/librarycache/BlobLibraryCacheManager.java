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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.jobgraph.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For each job graph that is submitted to the system the library cache manager maintains
 * a set of libraries (typically JAR files) which the job requires to run. The library cache manager
 * caches library files in order to avoid unnecessary retransmission of data. It is based on a singleton
 * programming pattern, so there exists at most on library manager at a time.
 * <p>
 * This class is thread-safe.
 */
public final class BlobLibraryCacheManager extends TimerTask implements LibraryCacheManager {

	private static Logger LOG = LoggerFactory.getLogger(BlobLibraryCacheManager.class);

	/**
	 * Dummy object used in the lock map.
	 */
	private final Object lockObject = new Object();

	/**
	 * Map to translate a job ID to the responsible class loaders.
	 */
	private final ConcurrentMap<JobID, URLClassLoader> classLoaders = new
			ConcurrentHashMap<JobID, URLClassLoader>();

	/**
	 * Map to store the number of references to a specific library manager entry.
	 */
	private final Map<JobID, Integer> libraryReferenceCounter = new HashMap<JobID,
			Integer>();

	/**
	 * Map to store the blob keys referenced by a specific job
	 */
	private final Map<JobID, Collection<BlobKey>> requiredJars = new
			HashMap<JobID, Collection<BlobKey>>();

	/**
	 * Map to store the number of reference to a specific file
	 */
	private final Map<BlobKey, Integer> blobKeyReferenceCounter = new
			HashMap<BlobKey, Integer>();

	/**
	 * All registered blobs
	 */
	private final Set<BlobKey> registeredBlobs = new HashSet<BlobKey>();

	private final BlobService blobService;

	public BlobLibraryCacheManager(BlobService blobService, Configuration configuration){
		this.blobService = blobService;

		// Initializing the clean up task
		Timer timer = new Timer(true);
		long cleanupInterval = configuration.getLong(
				ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
				ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL)*1000;
		timer.schedule(this, cleanupInterval);
	}

	/**
	 * Increments the reference counter of the corrsponding map
	 *
	 * @param key
	 *        the key identifying the counter to increment
	 * @return the increased reference counter
	 */
	private <K> int incrementReferenceCounter(final K key, final Map<K,
	Integer> map) {

		if(!map.containsKey(key)){
			map.put(key, 1);

			return 1;
		}else{
			int counter = map.get(key) + 1;
			map.put(key, counter);

			return counter;
		}
	}

	/**
	 * Decrements the reference counter associated with the key
	 *
	 * @param key
	 *        the key identifying the counter to decrement
	 * @return the decremented reference counter
	 */
	private <K> int decrementReferenceCounter(final K key, final Map<K,
			Integer> map) {

		if (!map.containsKey(key)) {
			throw new IllegalStateException("Cannot find reference counter entry for key " + key);
		}else{
			int counter = map.get(key) -1;

			if(counter == 0){
				map.remove(key);
			}else{
				map.put(key, counter);
			}

			return counter;
		}
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. For every registered
	 * job the library cache manager creates a class loader that is used to instantiate the vertex's environment later
	 * on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param requiredJarFiles
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if one of the requested libraries is not in the cache
	 */
	@Override
	public void register(final JobID id, final Collection<BlobKey> requiredJarFiles) throws
			IOException {

		synchronized (lockObject) {
			if (incrementReferenceCounter(id, libraryReferenceCounter) > 1) {
				return;
			}

			// Check if library manager entry for this id already exists
			if (this.classLoaders.containsKey(id)) {
				throw new IllegalStateException("Library cache manager already contains " +
						"class loader entry for job ID " + id);
			}

			if (requiredJars.containsKey(id)) {
				throw new IllegalStateException("Library cache manager already contains blob keys" +
						" entry for job ID " + id);
			}

			requiredJars.put(id, requiredJarFiles);

			URL[] urls = new URL[requiredJarFiles.size()];
			int count = 0;

			for (BlobKey blobKey : requiredJarFiles) {
				urls[count++] = registerBlobKeyAndGetURL(blobKey);
			}

			final URLClassLoader classLoader = new URLClassLoader(urls);
			this.classLoaders.put(id, classLoader);
		}
	}

	private URL registerBlobKeyAndGetURL(BlobKey key) throws IOException{
		if(incrementReferenceCounter(key, blobKeyReferenceCounter) == 1){
			// registration might happen even if the file is already stored locally
			registeredBlobs.add(key);
		}

		return blobService.getURL(key);
	}

	/**
	 * Unregisters a job ID and releases the resources associated with it.
	 * 
	 * @param id
	 *        the job ID to unregister
	 */
	@Override
	public void unregister(final JobID id) {
		synchronized (lockObject) {
			if (decrementReferenceCounter(id, libraryReferenceCounter) == 0) {
				this.classLoaders.remove(id);

				Collection<BlobKey> keys = requiredJars.get(id);

				for (BlobKey key : keys) {
					decrementReferenceCounter(key, blobKeyReferenceCounter);
				}

				requiredJars.remove(id);
			}
		}

	}

	/**
	 * Returns the class loader to the specified vertex.
	 * 
	 * @param id
	 *        the ID of the job to return the class loader for
	 * @return the class loader of requested vertex or <code>null</code> if no class loader has been registered with the
	 *         given ID.
	 */
	@Override
	public ClassLoader getClassLoader(final JobID id) {
		return this.classLoaders.get(id);
	}

	@Override
	public File getFile(BlobKey blobKey) throws IOException {
		return new File(blobService.getURL(blobKey).getFile());
	}

	public int getBlobServerPort() {
		return blobService.getPort();
	}

	@Override
	public void shutdown() throws IOException{
		blobService.shutdown();
	}

	/**
	 * Cleans up blobs which are not referenced anymore
	 */
	@Override
	public void run() {
		synchronized (lockObject) {
			Iterator<BlobKey> it = registeredBlobs.iterator();

			while (it.hasNext()) {
				BlobKey key = it.next();

				try {
					if (!blobKeyReferenceCounter.containsKey(key)) {
						blobService.delete(key);
						it.remove();
					}
				} catch (IOException ioe) {
					LOG.warn("Could not delete file with blob key" + key, ioe);
				}
			}
		}
	}
}
