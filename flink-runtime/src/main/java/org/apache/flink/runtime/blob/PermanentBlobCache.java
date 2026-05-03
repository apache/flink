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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Reference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides a cache for permanent BLOB files including a per-job ref-counting and a staged cleanup.
 *
 * <p>When requesting BLOBs via {@link #getFile(JobID, PermanentBlobKey)}, the cache will first
 * attempt to serve the file from its local cache. Only if the local cache does not contain the
 * desired BLOB, it will try to download it from a distributed HA file system (if available) or the
 * BLOB server.
 *
 * <p>If files for a job are not needed any more, they will enter a staged, i.e. deferred, cleanup.
 * Files may thus still be accessible upon recovery and do not need to be re-downloaded.
 */
public class PermanentBlobCache extends AbstractBlobCache implements JobPermanentBlobService {

    /** Object reference counters with a time-to-live (TTL). */
    @VisibleForTesting
    static class RefCount {
        /** Number of references to an object. */
        public int references = 0;

        /**
         * Timestamp in milliseconds when any object data should be cleaned up (no cleanup for
         * non-positive values).
         */
        public long keepUntil = -1;
    }

    private static final int DEFAULT_SIZE_LIMIT_MB = 100;

    /** Map to store the number of references to a specific job. */
    private final Map<JobID, RefCount> jobRefCounters = new HashMap<>();

    /** Map to store the number of references to a specific application. */
    private final Map<ApplicationID, RefCount> applicationRefCounters = new HashMap<>();

    /** Lock object for protecting both jobRefCounters and applicationRefCounters. */
    private final Object refCountersLock = new Object();

    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;

    private final BlobCacheSizeTracker blobCacheSizeTracker;

    @VisibleForTesting
    public PermanentBlobCache(
            final Configuration blobClientConfig,
            final File storageDir,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress)
            throws IOException {
        this(blobClientConfig, Reference.owned(storageDir), blobView, serverAddress);
    }

    @VisibleForTesting
    public PermanentBlobCache(
            final Configuration blobClientConfig,
            final File storageDir,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress,
            BlobCacheSizeTracker blobCacheSizeTracker)
            throws IOException {
        this(
                blobClientConfig,
                Reference.owned(storageDir),
                blobView,
                serverAddress,
                blobCacheSizeTracker);
    }

    /**
     * Instantiates a new cache for permanent BLOBs which are also available in an HA store.
     *
     * @param blobClientConfig global configuration
     * @param storageDir storage directory for the cached blobs
     * @param blobView (distributed) HA blob store file system to retrieve files from first
     * @param serverAddress address of the {@link BlobServer} to use for fetching files from or
     *     {@code null} if none yet
     * @throws IOException thrown if the (local or distributed) file storage cannot be created or is
     *     not usable
     */
    public PermanentBlobCache(
            final Configuration blobClientConfig,
            final Reference<File> storageDir,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress)
            throws IOException {
        this(
                blobClientConfig,
                storageDir,
                blobView,
                serverAddress,
                new BlobCacheSizeTracker(MemorySize.ofMebiBytes(DEFAULT_SIZE_LIMIT_MB).getBytes()));
    }

    @VisibleForTesting
    public PermanentBlobCache(
            final Configuration blobClientConfig,
            final Reference<File> storageDir,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress,
            BlobCacheSizeTracker blobCacheSizeTracker)
            throws IOException {
        super(
                blobClientConfig,
                storageDir,
                blobView,
                LoggerFactory.getLogger(PermanentBlobCache.class),
                serverAddress);

        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);

        this.cleanupInterval = blobClientConfig.get(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
        this.cleanupTimer.schedule(
                new PermanentBlobCleanupTask(), cleanupInterval, cleanupInterval);

        this.blobCacheSizeTracker = blobCacheSizeTracker;

        registerDetectedJobs();
        registerDetectedApplications();
    }

    private void registerDetectedJobs() throws IOException {
        if (storageDir.deref().exists()) {
            final Collection<JobID> jobIds =
                    BlobUtils.listExistingJobs(storageDir.deref().toPath());

            final long expiryTimeout = System.currentTimeMillis() + cleanupInterval;
            for (JobID jobId : jobIds) {
                registerJobWithExpiry(jobId, expiryTimeout);
            }
        }
    }

    private void registerJobWithExpiry(JobID jobId, long expiryTimeout) {
        checkNotNull(jobId);
        synchronized (refCountersLock) {
            final RefCount refCount =
                    jobRefCounters.computeIfAbsent(jobId, ignored -> new RefCount());

            refCount.keepUntil = expiryTimeout;
        }
    }

    private void registerDetectedApplications() throws IOException {
        if (storageDir.deref().exists()) {
            final Collection<ApplicationID> applicationIds =
                    BlobUtils.listExistingApplications(storageDir.deref().toPath());

            final long expiryTimeout = System.currentTimeMillis() + cleanupInterval;
            for (ApplicationID applicationId : applicationIds) {
                registerApplicationWithExpiry(applicationId, expiryTimeout);
            }
        }
    }

    private void registerApplicationWithExpiry(ApplicationID applicationId, long expiryTimeout) {
        checkNotNull(applicationId);
        synchronized (refCountersLock) {
            final RefCount refCount =
                    applicationRefCounters.computeIfAbsent(
                            applicationId, ignored -> new RefCount());

            refCount.keepUntil = expiryTimeout;
        }
    }

    /**
     * Registers use of job-related BLOBs.
     *
     * <p>Using any other method to access BLOBs, e.g. {@link #getFile}, is only valid within calls
     * to <tt>registerJob(JobID)</tt> and {@link #releaseJob(JobID, ApplicationID)}.
     *
     * @param jobId ID of the job this blob belongs to
     * @see #releaseJob(JobID, ApplicationID)
     */
    @Override
    public void registerJob(JobID jobId, ApplicationID applicationId) {
        checkNotNull(jobId);

        synchronized (refCountersLock) {
            RefCount ref = jobRefCounters.get(jobId);
            if (ref == null) {
                ref = new RefCount();
                jobRefCounters.put(jobId, ref);
            } else {
                // reset cleanup timeout
                ref.keepUntil = -1;
            }
            ++ref.references;

            // also register under application because the job may use application-level blobs
            RefCount applicationRef = applicationRefCounters.get(applicationId);
            if (applicationRef == null) {
                applicationRef = new RefCount();
                applicationRefCounters.put(applicationId, applicationRef);
            } else {
                // reset cleanup timeout
                applicationRef.keepUntil = -1;
            }
            ++applicationRef.references;
        }
    }

    /**
     * Unregisters use of job-related BLOBs and allow them to be released.
     *
     * @param jobId ID of the job this blob belongs to
     * @see #registerJob(JobID, ApplicationID)
     */
    @Override
    public void releaseJob(JobID jobId, ApplicationID applicationId) {
        checkNotNull(jobId);

        synchronized (refCountersLock) {
            String warning =
                    "improper use of releaseJob() without a matching number of registerJob() calls for jobId "
                            + jobId;

            RefCount ref = jobRefCounters.get(jobId);

            if (ref == null || ref.references == 0) {
                log.warn(warning);
                return;
            }

            --ref.references;
            if (ref.references == 0) {
                ref.keepUntil = System.currentTimeMillis() + cleanupInterval;
            }

            // make sure application related data can be cleaned up
            RefCount applicationRef = applicationRefCounters.get(applicationId);

            if (applicationRef == null || applicationRef.references == 0) {
                log.warn(
                        "Cache reference count mismatch for application {}, which indicates {}.",
                        applicationId,
                        warning);
                return;
            }

            --applicationRef.references;
            if (applicationRef.references == 0) {
                applicationRef.keepUntil = System.currentTimeMillis() + cleanupInterval;
            }
        }
    }

    public int getNumberOfReferenceHolders(JobID jobId) {
        checkNotNull(jobId);

        synchronized (refCountersLock) {
            RefCount ref = jobRefCounters.get(jobId);
            if (ref == null) {
                return 0;
            } else {
                return ref.references;
            }
        }
    }

    /**
     * Returns the path to a local copy of the file associated with the provided job ID and blob
     * key.
     *
     * <p>We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
     * there, we will try to download it from the HA store, or directly from the {@link BlobServer}.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    @Override
    public File getFile(JobID jobId, PermanentBlobKey key) throws IOException {
        checkNotNull(jobId);
        return getFileInternal(jobId, key);
    }

    /**
     * Returns the path to a local copy of the file associated with the provided application ID and
     * blob key.
     *
     * <p>We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
     * there, we will try to download it from the HA store, or directly from the {@link BlobServer}.
     *
     * @param applicationId ID of the application this blob belongs to
     * @param key blob key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    @Override
    public File getFile(ApplicationID applicationId, PermanentBlobKey key) throws IOException {
        checkNotNull(applicationId);
        return getFileInternal(applicationId, key);
    }

    /**
     * Returns the content of the file for the BLOB with the provided job ID the blob key.
     *
     * <p>The method will first attempt to serve the BLOB from the local cache. If the BLOB is not
     * in the cache, the method will try to download it from the HA store, or directly from the
     * {@link BlobServer}.
     *
     * <p>Compared to {@code getFile}, {@code readFile} makes sure that the file is fully read in
     * the same write lock as the file is accessed. This avoids the scenario that the path is
     * returned as the file is deleted concurrently by other threads.
     *
     * @param jobId ID of the job this blob belongs to
     * @param blobKey BLOB key associated with the requested file
     * @return The content of the BLOB.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file.
     */
    @Override
    public byte[] readFile(JobID jobId, PermanentBlobKey blobKey) throws IOException {
        checkNotNull(jobId);
        checkNotNull(blobKey);

        final File localFile = BlobUtils.getStorageLocation(storageDir.deref(), jobId, blobKey);
        readWriteLock.readLock().lock();

        try {
            if (localFile.exists()) {
                blobCacheSizeTracker.update(jobId, blobKey);
                return FileUtils.readAllBytes(localFile.toPath());
            }
        } finally {
            readWriteLock.readLock().unlock();
        }

        // first try the distributed blob store (if available)
        // use a temporary file (thread-safe without locking)
        File incomingFile = createTemporaryFilename();
        try {
            try {
                if (blobView.get(jobId, blobKey, incomingFile)) {
                    // now move the temp file to our local cache atomically
                    readWriteLock.writeLock().lock();
                    try {
                        checkLimitAndMoveFile(incomingFile, jobId, blobKey, localFile, log, null);
                        return FileUtils.readAllBytes(localFile.toPath());
                    } finally {
                        readWriteLock.writeLock().unlock();
                    }
                }
            } catch (Exception e) {
                log.info(
                        "Failed to copy from blob store. Downloading from BLOB server instead.", e);
            }

            final InetSocketAddress currentServerAddress = serverAddress;

            if (currentServerAddress != null) {
                // fallback: download from the BlobServer
                BlobClient.downloadFromBlobServer(
                        jobId,
                        blobKey,
                        incomingFile,
                        currentServerAddress,
                        blobClientConfig,
                        numFetchRetries);

                readWriteLock.writeLock().lock();
                try {
                    checkLimitAndMoveFile(incomingFile, jobId, blobKey, localFile, log, null);
                    return FileUtils.readAllBytes(localFile.toPath());
                } finally {
                    readWriteLock.writeLock().unlock();
                }
            } else {
                throw new IOException(
                        "Cannot download from BlobServer, because the server address is unknown.");
            }

        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                log.warn(
                        "Could not delete the staging file {} for blob key {} and job {}.",
                        incomingFile,
                        blobKey,
                        jobId);
            }
        }
    }

    private void checkLimitAndMoveFile(
            File incomingFile,
            JobID jobId,
            BlobKey blobKey,
            File localFile,
            Logger log,
            @Nullable BlobStore blobStore)
            throws IOException {

        // Check the size limit and delete the files that exceeds the limit
        final long sizeOfIncomingFile = incomingFile.length();
        final List<Tuple2<JobID, BlobKey>> blobsToDelete =
                blobCacheSizeTracker.checkLimit(sizeOfIncomingFile);

        for (Tuple2<JobID, BlobKey> key : blobsToDelete) {
            if (deleteFile(key.f0, key.f1)) {
                blobCacheSizeTracker.untrack(key);
            }
        }

        // Move the file and register it to the tracker
        BlobUtils.moveTempFileToStore(incomingFile, jobId, blobKey, localFile, log, blobStore);
        blobCacheSizeTracker.track(jobId, blobKey, localFile.length());
    }

    /**
     * Delete the blob file with the given key.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey The key of the desired BLOB.
     */
    private boolean deleteFile(JobID jobId, BlobKey blobKey) {
        final File localFile =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.deref().getAbsolutePath(), jobId, blobKey));
        if (!localFile.delete() && localFile.exists()) {
            log.warn(
                    "Failed to delete locally cached BLOB {} at {}",
                    blobKey,
                    localFile.getAbsolutePath());
            return false;
        }
        return true;
    }

    /**
     * Returns a file handle to the file associated with the given blob key on the blob server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key identifying the file
     * @return file handle to the file
     * @throws IOException if creating the directory fails
     */
    @VisibleForTesting
    public File getStorageLocation(JobID jobId, BlobKey key) throws IOException {
        checkNotNull(jobId);
        return BlobUtils.getStorageLocation(storageDir.deref(), jobId, key);
    }

    /**
     * Returns a file handle to the file associated with the given blob key on the blob server.
     *
     * @param applicationId ID of the application this blob belongs to (or <tt>null</tt> if
     *     job-unrelated)
     * @param key identifying the file
     * @return file handle to the file
     * @throws IOException if creating the directory fails
     */
    @VisibleForTesting
    public File getStorageLocation(ApplicationID applicationId, BlobKey key) throws IOException {
        checkNotNull(applicationId);
        return BlobUtils.getStorageLocation(storageDir.deref(), applicationId, key);
    }

    /**
     * Returns the job reference counters - for testing purposes only!
     *
     * @return job reference counters (internal state!)
     */
    @VisibleForTesting
    Map<JobID, RefCount> getJobRefCounters() {
        return jobRefCounters;
    }

    /**
     * Cleanup task which is executed periodically to delete BLOBs whose ref-counter reached
     * <tt>0</tt>.
     */
    class PermanentBlobCleanupTask extends TimerTask {
        /** Cleans up BLOBs which are not referenced anymore. */
        @Override
        public void run() {
            synchronized (refCountersLock) {
                // Clean up job blobs
                cleanupRefCounters(
                        jobRefCounters.entrySet().iterator(),
                        jobId ->
                                new File(
                                        BlobUtils.getStorageLocationPath(
                                                storageDir.deref().getAbsolutePath(), jobId)),
                        blobCacheSizeTracker::untrackAll,
                        "job");

                // Clean up application blobs
                /*
                 * NOTE: We do not need to call blobCacheSizeTracker.untrackAll here because
                 * blobCacheSizeTracker currently only tracks and manages a subset of job-level
                 * blobs (e.g., ShuffleDescriptors, see FLINK-23354). Application-level blobs are
                 * not tracked by the size tracker.
                 */
                cleanupRefCounters(
                        applicationRefCounters.entrySet().iterator(),
                        applicationId ->
                                new File(
                                        BlobUtils.getStorageLocationPath(
                                                storageDir.deref().getAbsolutePath(),
                                                applicationId)),
                        applicationId -> {},
                        "application");
            }
        }
    }

    /**
     * Generic method to clean up BLOBs for both job and application ref-counters.
     *
     * @param entryIter iterator over the ref-count entries to process
     * @param getStorageLocation function to get the storage location for an entity ID
     * @param beforeDelete action to perform before deleting the directory (e.g., untracking)
     * @param entityType type description for logging (e.g., "job", "application")
     * @param <ID> type of the entity ID (JobID or ApplicationID)
     */
    private <ID> void cleanupRefCounters(
            Iterator<Map.Entry<ID, RefCount>> entryIter,
            Function<ID, File> getStorageLocation,
            Consumer<ID> beforeDelete,
            String entityType) {

        final long currentTimeMillis = System.currentTimeMillis();

        while (entryIter.hasNext()) {
            Map.Entry<ID, RefCount> entry = entryIter.next();
            RefCount ref = entry.getValue();

            if (ref.references <= 0 && ref.keepUntil > 0 && currentTimeMillis >= ref.keepUntil) {
                ID id = entry.getKey();
                final File localFile = getStorageLocation.apply(id);

                /*
                 * NOTE: normally it is not required to acquire the write lock to delete the storage
                 *       directory since there should be no one accessing it with the ref counter
                 *       being 0 - acquire it just in case, to always be on the safe side
                 */
                readWriteLock.writeLock().lock();

                boolean success = false;
                try {
                    beforeDelete.accept(id);
                    FileUtils.deleteDirectory(localFile);
                    success = true;
                } catch (Throwable t) {
                    log.warn(
                            "Failed to locally delete {} directory {}",
                            entityType,
                            localFile.getAbsolutePath(),
                            t);
                } finally {
                    readWriteLock.writeLock().unlock();
                }

                // let's only remove this directory from cleanup if the cleanup was successful
                // (does not need the write lock)
                if (success) {
                    entryIter.remove();
                }
            }
        }
    }

    @Override
    protected void cancelCleanupTask() {
        cleanupTimer.cancel();
    }
}
