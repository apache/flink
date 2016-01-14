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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * For each job graph that is submitted to the system the library cache manager maintains
 * a set of libraries (typically JAR files) which the job requires to run. The library cache manager
 * caches library files in order to avoid unnecessary retransmission of data. It is based on a singleton
 * programming pattern, so there exists at most one library manager at a time.
 */
public final class BlobLibraryCacheManager extends TimerTask implements LibraryCacheManager {

	private static Logger LOG = LoggerFactory.getLogger(BlobLibraryCacheManager.class);
	
	private static ExecutionAttemptID JOB_ATTEMPT_ID = new ExecutionAttemptID(-1, -1);
	
	// --------------------------------------------------------------------------------------------
	
	/** The global lock to synchronize operations */
	private final Object lockObject = new Object();

	/** Registered entries per job */
	private final Map<JobID, LibraryCacheEntry> cacheEntries = new HashMap<JobID, LibraryCacheEntry>();
	
	/** Map to store the number of reference to a specific file */
	private final Map<BlobKey, Integer> blobKeyReferenceCounters = new HashMap<BlobKey, Integer>();

	/** The blob service to download libraries */
	private final BlobService blobService;
	
	// --------------------------------------------------------------------------------------------

	public BlobLibraryCacheManager(BlobService blobService, long cleanupInterval) {
		this.blobService = blobService;

		// Initializing the clean up task
		Timer timer = new Timer(true);
		timer.schedule(this, cleanupInterval);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void registerJob(JobID id, Collection<BlobKey> requiredJarFiles, Collection<URL> requiredClasspaths)
			throws IOException {
		registerTask(id, JOB_ATTEMPT_ID, requiredJarFiles, requiredClasspaths);
	}
	
	@Override
	public void registerTask(JobID jobId, ExecutionAttemptID task, Collection<BlobKey> requiredJarFiles,
			Collection<URL> requiredClasspaths) throws IOException {
		Preconditions.checkNotNull(jobId, "The JobId must not be null.");
		Preconditions.checkNotNull(task, "The task execution id must not be null.");

		if (requiredJarFiles == null) {
			requiredJarFiles = Collections.emptySet();
		}
		if (requiredClasspaths == null) {
			requiredClasspaths = Collections.emptySet();
		}

		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);

			if (entry == null) {
				// create a new entry in the library cache
				BlobKey[] keys = requiredJarFiles.toArray(new BlobKey[requiredJarFiles.size()]);
				URL[] urls = new URL[keys.length + requiredClasspaths.size()];

				int count = 0;
				try {
					for (; count < keys.length; count++) {
						BlobKey blobKey = keys[count];
						urls[count] = registerReferenceToBlobKeyAndGetURL(blobKey);
					}
				}
				catch (Throwable t) {
					// undo the reference count increases
					try {
						for (int i = 0; i < count; i++) {
							unregisterReferenceToBlobKey(keys[i]);
						}
					}
					catch (Throwable tt) {
						LOG.error("Error while updating library reference counters.", tt);
					}

					// rethrow or wrap
					ExceptionUtils.tryRethrowIOException(t);
					throw new IOException("Library cache could not register the user code libraries.", t);
				}

				// add classpaths
				for (URL url : requiredClasspaths) {
					urls[count] = url;
					count++;
				}

				URLClassLoader classLoader = new FlinkUserCodeClassLoader(urls);
				cacheEntries.put(jobId, new LibraryCacheEntry(requiredJarFiles, classLoader, task));
			}
			else {
				entry.register(task, requiredJarFiles);
			}
		}
	}

	@Override
	public void unregisterJob(JobID id) {
		unregisterTask(id, JOB_ATTEMPT_ID);
	}
	
	@Override
	public void unregisterTask(JobID jobId, ExecutionAttemptID task) {
		Preconditions.checkNotNull(jobId, "The JobId must not be null.");
		Preconditions.checkNotNull(task, "The task execution id must not be null.");
		
		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);
			
			if (entry != null) {
				if (entry.unregister(task)) {
					cacheEntries.remove(jobId);
					
					for (BlobKey key : entry.getLibraries()) {
						unregisterReferenceToBlobKey(key);
					}
				}
			}
			// else has already been unregistered
		}
	}

	@Override
	public ClassLoader getClassLoader(JobID id) {
		if (id == null) {
			throw new IllegalArgumentException("The JobId must not be null.");
		}
		
		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(id);
			if (entry != null) {
				return entry.getClassLoader();
			} else {
				throw new IllegalStateException("No libraries are registered for job " + id);
			}
		}
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
			
			Iterator<Map.Entry<BlobKey, Integer>> entryIter = blobKeyReferenceCounters.entrySet().iterator();
			
			while (entryIter.hasNext()) {
				Map.Entry<BlobKey, Integer> entry = entryIter.next();
				BlobKey key = entry.getKey();
				int references = entry.getValue();
				
				try {
					if (references <= 0) {
						blobService.delete(key);
						entryIter.remove();
					}
				} catch (Throwable t) {
					LOG.warn("Could not delete file with blob key" + key, t);
				}
			}
		}
	}
	
	public int getNumberOfReferenceHolders(JobID jobId) {
		synchronized (lockObject) {
			LibraryCacheEntry entry = cacheEntries.get(jobId);
			return entry == null ? 0 : entry.getNumberOfReferenceHolders();
		}
	}
	
	int getNumberOfCachedLibraries() {
		return blobKeyReferenceCounters.size();
	}
	
	private URL registerReferenceToBlobKeyAndGetURL(BlobKey key) throws IOException {
		// it is important that we fetch the URL before increasing the counter.
		// in case the URL cannot be created (failed to fetch the BLOB), we have no stale counter
		try {
			URL url = blobService.getURL(key);

			Integer references = blobKeyReferenceCounters.get(key);
			int newReferences = references == null ? 1 : references + 1;
			blobKeyReferenceCounters.put(key, newReferences);

			return url;
		}
		catch (IOException e) {
			throw new IOException("Cannot get library with hash " + key, e);
		}
	}
	
	private void unregisterReferenceToBlobKey(BlobKey key) {
		Integer references = blobKeyReferenceCounters.get(key);
		if (references != null) {
			int newReferences = Math.max(references - 1, 0);
			blobKeyReferenceCounters.put(key, newReferences);
		}
		else {
			// make sure we have an entry in any case, that the cleanup timer removes any
			// present libraries
			blobKeyReferenceCounters.put(key, 0);
		}
	}


	// --------------------------------------------------------------------------------------------

	/**
	 * An entry in the per-job library cache. Tracks which execution attempts
	 * still reference the libraries. Once none reference it any more, the
	 * libraries can be cleaned up.
	 */
	private static class LibraryCacheEntry {
		
		private final ClassLoader classLoader;
		
		private final Set<ExecutionAttemptID> referenceHolders;
		
		private final Set<BlobKey> libraries;
		
		
		public LibraryCacheEntry(Collection<BlobKey> libraries, ClassLoader classLoader, ExecutionAttemptID initialReference) {
			this.classLoader = classLoader;
			this.libraries = new HashSet<BlobKey>(libraries);
			this.referenceHolders = new HashSet<ExecutionAttemptID>();
			this.referenceHolders.add(initialReference);
		}
		
		
		public ClassLoader getClassLoader() {
			return classLoader;
		}
		
		public Set<BlobKey> getLibraries() {
			return libraries;
		}
		
		public void register(ExecutionAttemptID task, Collection<BlobKey> keys) {
			if (!libraries.containsAll(keys)) {
				throw new IllegalStateException(
						"The library registration references a different set of libraries than previous registrations for this job.");
			}
			
			this.referenceHolders.add(task);
		}
		
		public boolean unregister(ExecutionAttemptID task) {
			referenceHolders.remove(task);
			return referenceHolders.isEmpty();
		}
		
		public int getNumberOfReferenceHolders() {
			return referenceHolders.size();
		}
	}

	/**
	 * Give the URLClassLoader a nicer name for debugging purposes.
	 */
	private static class FlinkUserCodeClassLoader extends URLClassLoader {

		public FlinkUserCodeClassLoader(URL[] urls) {
			super(urls, FlinkUserCodeClassLoader.class.getClassLoader());
		}
	}
}
