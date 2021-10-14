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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

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
 * Files may thus still be be accessible upon recovery and do not need to be re-downloaded.
 */
public class PermanentBlobCache extends AbstractBlobCache implements PermanentBlobService {

    /** Job reference counters with a time-to-live (TTL). */
    @VisibleForTesting
    static class RefCount {
        /** Number of references to a job. */
        public int references = 0;

        /**
         * Timestamp in milliseconds when any job data should be cleaned up (no cleanup for
         * non-positive values).
         */
        public long keepUntil = -1;
    }

    private static final int DEFAULT_SIZE_LIMIT_MB = 100;

    /** Map to store the number of references to a specific job. */
    private final Map<JobID, RefCount> jobRefCounters = new HashMap<>();

    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;

    private final BlobCacheSizeTracker blobCacheSizeTracker;

    /**
     * Instantiates a new cache for permanent BLOBs which are also available in an HA store.
     *
     * @param blobClientConfig global configuration
     * @param blobView (distributed) HA blob store file system to retrieve files from first
     * @param serverAddress address of the {@link BlobServer} to use for fetching files from or
     *     {@code null} if none yet
     * @throws IOException thrown if the (local or distributed) file storage cannot be created or is
     *     not usable
     */
    public PermanentBlobCache(
            final Configuration blobClientConfig,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress)
            throws IOException {

        this(
                blobClientConfig,
                blobView,
                serverAddress,
                new BlobCacheSizeTracker(MemorySize.ofMebiBytes(DEFAULT_SIZE_LIMIT_MB).getBytes()));
    }

    @VisibleForTesting
    public PermanentBlobCache(
            final Configuration blobClientConfig,
            final BlobView blobView,
            @Nullable final InetSocketAddress serverAddress,
            BlobCacheSizeTracker blobCacheSizeTracker)
            throws IOException {

        super(
                blobClientConfig,
                blobView,
                LoggerFactory.getLogger(PermanentBlobCache.class),
                serverAddress);

        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);

        this.cleanupInterval = blobClientConfig.getLong(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
        this.cleanupTimer.schedule(
                new PermanentBlobCleanupTask(), cleanupInterval, cleanupInterval);

        this.blobCacheSizeTracker = blobCacheSizeTracker;
    }

    /**
     * Registers use of job-related BLOBs.
     *
     * <p>Using any other method to access BLOBs, e.g. {@link #getFile}, is only valid within calls
     * to <tt>registerJob(JobID)</tt> and {@link #releaseJob(JobID)}.
     *
     * @param jobId ID of the job this blob belongs to
     * @see #releaseJob(JobID)
     */
    public void registerJob(JobID jobId) {
        checkNotNull(jobId);

        synchronized (jobRefCounters) {
            RefCount ref = jobRefCounters.get(jobId);
            if (ref == null) {
                ref = new RefCount();
                jobRefCounters.put(jobId, ref);
            } else {
                // reset cleanup timeout
                ref.keepUntil = -1;
            }
            ++ref.references;
        }
    }

    /**
     * Unregisters use of job-related BLOBs and allow them to be released.
     *
     * @param jobId ID of the job this blob belongs to
     * @see #registerJob(JobID)
     */
    public void releaseJob(JobID jobId) {
        checkNotNull(jobId);

        synchronized (jobRefCounters) {
            RefCount ref = jobRefCounters.get(jobId);

            if (ref == null || ref.references == 0) {
                log.warn(
                        "improper use of releaseJob() without a matching number of registerJob() calls for jobId "
                                + jobId);
                return;
            }

            --ref.references;
            if (ref.references == 0) {
                ref.keepUntil = System.currentTimeMillis() + cleanupInterval;
            }
        }
    }

    public int getNumberOfReferenceHolders(JobID jobId) {
        checkNotNull(jobId);

        synchronized (jobRefCounters) {
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

        final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);
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
                                storageDir.getAbsolutePath(), jobId, blobKey));
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
        return BlobUtils.getStorageLocation(storageDir, jobId, key);
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
            synchronized (jobRefCounters) {
                Iterator<Map.Entry<JobID, RefCount>> entryIter =
                        jobRefCounters.entrySet().iterator();
                final long currentTimeMillis = System.currentTimeMillis();

                while (entryIter.hasNext()) {
                    Map.Entry<JobID, RefCount> entry = entryIter.next();
                    RefCount ref = entry.getValue();

                    if (ref.references <= 0
                            && ref.keepUntil > 0
                            && currentTimeMillis >= ref.keepUntil) {
                        JobID jobId = entry.getKey();

                        final File localFile =
                                new File(
                                        BlobUtils.getStorageLocationPath(
                                                storageDir.getAbsolutePath(), jobId));

                        /*
                         * NOTE: normally it is not required to acquire the write lock to delete the job's
                         *       storage directory since there should be no one accessing it with the ref
                         *       counter being 0 - acquire it just in case, to always be on the safe side
                         */
                        readWriteLock.writeLock().lock();

                        boolean success = false;
                        try {
                            blobCacheSizeTracker.untrackAll(jobId);
                            FileUtils.deleteDirectory(localFile);
                            success = true;
                        } catch (Throwable t) {
                            log.warn(
                                    "Failed to locally delete job directory "
                                            + localFile.getAbsolutePath(),
                                    t);
                        } finally {
                            readWriteLock.writeLock().unlock();
                        }

                        // let's only remove this directory from cleanup if the cleanup was
                        // successful
                        // (does not need the write lock)
                        if (success) {
                            entryIter.remove();
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void cancelCleanupTask() {
        cleanupTimer.cancel();
    }
}
