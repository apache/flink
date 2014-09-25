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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.internal.ConcurrentSet;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobKey;
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
public final class LibraryCacheManager extends TimerTask {

	private static Logger LOG = LoggerFactory.getLogger(LibraryCacheManager.class);

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
	private final ConcurrentMap<JobID, URLClassLoader> classLoaders = new
			ConcurrentHashMap<JobID, URLClassLoader>();

	/**
	 * Map to store the number of references to a specific library manager entry.
	 */
	private final ConcurrentMap<JobID, AtomicInteger> libraryReferenceCounter = new ConcurrentHashMap<JobID, AtomicInteger>();

	/**
	 * Map to guarantee atomicity of register/unregister operations.
	 */
	private final ConcurrentMap<JobID, Object> lockMap = new ConcurrentHashMap<JobID, Object>();

	/**
	 * Map to store the blob keys referenced by a specific job
	 */
	private final ConcurrentMap<JobID, Collection<BlobKey>> requiredJars = new
			ConcurrentHashMap<JobID, Collection<BlobKey>>();

	/**
	 * Map to store the number of reference to a specific file
	 */
	private final ConcurrentMap<BlobKey, AtomicInteger> blobKeyReferenceCounter = new
			ConcurrentHashMap<BlobKey, AtomicInteger>();

	/**
	 * Map to guarantee atomicity of register/unregister operations
	 */
	private final ConcurrentMap<BlobKey, Object> blobKeyLockMap = new ConcurrentHashMap<BlobKey,
			Object>();

	/**
	 * All registered blobs
	 */
	private final ConcurrentSet<BlobKey> registeredBlobs = new ConcurrentSet<BlobKey>();

	/**
	 * Stores the socket address of the BLOB server to download required libraries.
	 */
	private volatile InetSocketAddress blobServerAddress = null;

	public LibraryCacheManager(){
		// Initializing the clean up task
		Timer timer = new Timer();
		long cleanupInterval = GlobalConfiguration.getLong(
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
	private static <K> int incrementReferenceCounter(final K key, final ConcurrentMap<K,
	AtomicInteger> map) {

		while (true) {

			AtomicInteger ai = map.get(key);
			if (ai == null) {

				ai = new AtomicInteger(1);
				if (map.putIfAbsent(key, ai) == null) {
					return 1;
				}

				// We had a race, try again
			} else {
				return ai.incrementAndGet();
			}
		}
	}

	/**
	 * Decrements the reference counter associated with the key
	 * 
	 * @param key
	 *        the key identifying the counter to decrement
	 * @return the decremented reference counter
	 */
	private static <K> int decrementReferenceCounter(final K key, final ConcurrentMap<K,
			AtomicInteger> map) {

		final AtomicInteger ai = map.get(key);

		if (ai == null) {
			throw new IllegalStateException("Cannot find reference counter entry for key " + key);
		}

		int retVal = ai.decrementAndGet();

		if (retVal == 0) {
			map.remove(key);
		}

		return retVal;
	}

	/**
	 * Obtains lock for key. If methods which only affect objects associated with the key obtain
	 * the corresponding lock, then their operations are synchronized. By doing that,
	 * the LibraryCacheManager supports multiple synchronized method calls.
	 * @param key
	 * @param lockMap
	 * @param <K>
	 */
	private static <K> void obtainLock(final K key, final ConcurrentMap<K, Object> lockMap) {
		synchronized (LOCK_OBJECT){
			while(lockMap.putIfAbsent(key, LOCK_OBJECT) != null){
				try {
					LOCK_OBJECT.wait();
				} catch (InterruptedException e) {}
			}
		}
	}

	/**
	 * Releases the obtained lock for key and notifies all waiting threads to acquire the lock.
	 * @param key
	 * @param lockMap
	 * @param <K>
	 */
	private static <K> void releaseLock(final K key, final ConcurrentMap<K, Object> lockMap){
		lockMap.remove(key);

		synchronized (LOCK_OBJECT) {
			LOCK_OBJECT.notifyAll();
		}
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. For every registered
	 * job the library cache manager creates a class loader that is used to instantiate the job's environment later on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param requiredJarFiles
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
	 * @param requiredJarFiles
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if one of the requested libraries is not in the cache
	 */
	private void registerInternal(final JobID id, final Collection<BlobKey> requiredJarFiles) throws IOException {
		obtainLock(id, lockMap);

		try {
			if (incrementReferenceCounter(id, libraryReferenceCounter) > 1) {
				return;
			}

			// Check if library manager entry for this id already exists
			if (this.classLoaders.containsKey(id)) {
				throw new IllegalStateException("Library cache manager already contains " +
						"class loader entry for job ID " + id);
			}

			if(requiredJars.putIfAbsent(id, requiredJarFiles) != null){
				throw new IllegalStateException("Library cache manager already contains blob keys" +
						" entry for job ID " + id);
			}

			URL[] urls = new URL[requiredJarFiles.size()];
			int count = 0;

			for(BlobKey blobKey: requiredJarFiles){
				urls[count++] = registerBlobKeyAndGetURL(blobKey);
			}

			final URLClassLoader classLoader = new URLClassLoader(urls);
			this.classLoaders.put(id, classLoader);
		} finally {
			releaseLock(id, lockMap);
		}
	}

	private URL registerBlobKeyAndGetURL(BlobKey key) throws IOException{
		obtainLock(key, blobKeyLockMap);

		try{
			if(incrementReferenceCounter(key, blobKeyReferenceCounter) == 1){
				// registration might happen even if the file is already stored locally
				registeredBlobs.add(key);
			}


			return BlobCache.getURL(this.blobServerAddress, key);
		}finally{
			releaseLock(key, blobKeyLockMap);
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
		obtainLock(id, lockMap);

		if (decrementReferenceCounter(id, libraryReferenceCounter) == 0) {
			URLClassLoader cl = this.classLoaders.remove(id);

			Collection<BlobKey> keys = requiredJars.get(id);

			for(BlobKey key: keys){
				unregisterBlobKey(key);
			}

			keys.remove(id);
		}

		releaseLock(id, lockMap);
	}


	private void unregisterBlobKey(BlobKey key){
		obtainLock(key, blobKeyLockMap);

		try{
			decrementReferenceCounter(key, blobKeyReferenceCounter);
		}finally{
			releaseLock(key, blobKeyLockMap);
		}
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

	/**
	 * Cleans up blobs which are not referenced anymore
	 */
	@Override
	public void run() {
		Iterator<BlobKey> it = registeredBlobs.iterator();

		while(it.hasNext()){
			BlobKey key = it.next();

			obtainLock(key, blobKeyLockMap);

			try {
				if(!blobKeyReferenceCounter.containsKey(key)){
					BlobCache.delete(key);
					it.remove();
				}
			}catch(IOException ioe){
				LOG.warn("Could not delete file with blob key" + key, ioe);
			}finally{
				releaseLock(key, blobKeyLockMap);
			}
		}
	}
}
