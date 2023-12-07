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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract high availability services based on distributed system(e.g. Zookeeper, Kubernetes). It
 * will help with creating all the leader election/retrieval services and the cleanup.
 *
 * <p>{@link #close()} and {@link #cleanupAllData()} should be implemented to destroy the resources.
 *
 * <p>The abstract class is also responsible for determining which component service should be
 * reused. For example, {@link #jobResultStore} is created once and could be reused many times.
 */
public abstract class AbstractHaServices implements HighAvailabilityServices {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /** The executor to run external IO operations on. */
    protected final Executor ioExecutor;

    /** The runtime configuration. */
    protected final Configuration configuration;

    /** Store for arbitrary blobs. */
    private final BlobStoreService blobStoreService;

    private final JobResultStore jobResultStore;

    protected AbstractHaServices(
            Configuration config,
            Executor ioExecutor,
            BlobStoreService blobStoreService,
            JobResultStore jobResultStore) {

        this.configuration = checkNotNull(config);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.blobStoreService = checkNotNull(blobStoreService);
        this.jobResultStore = checkNotNull(jobResultStore);
    }

    @Override
    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
        return createCheckpointRecoveryFactory();
    }

    @Override
    public JobGraphStore getJobGraphStore() throws Exception {
        return createJobGraphStore();
    }

    @Override
    public JobResultStore getJobResultStore() throws Exception {
        return jobResultStore;
    }

    @Override
    public BlobStore createBlobStore() {
        return blobStoreService;
    }

    @Override
    public void close() throws Exception {
        Throwable exception = null;

        try {
            blobStoreService.close();
        } catch (Throwable t) {
            exception = t;
        }

        try {
            if (getLeaderServices() != null) {
                getLeaderServices().close();
            }
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        try {
            internalClose();
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(
                    exception, "Could not properly close the " + getClass().getSimpleName());
        }
    }

    @Override
    public void cleanupAllData() throws Exception {
        logger.info("Clean up all data for {}.", getClass().getSimpleName());

        Throwable exception = null;

        boolean deletedHAData = false;

        try {
            internalCleanup();
            deletedHAData = true;
            blobStoreService.cleanupAllData();
        } catch (Exception t) {
            exception = t;
        }

        if (!deletedHAData) {
            logger.info(
                    "Cannot delete HA blobs because we failed to delete the pointers in the HA store.");
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(
                    exception,
                    "Could not properly clean up all data of high availability service.");
        }
        logger.info("Finished cleaning up the high availability data.");
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobID, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                    logger.info("Clean up the high availability data for job {}.", jobID);
                    try {
                        internalCleanupJobData(jobID);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                    logger.info(
                            "Finished cleaning up the high availability data for job {}.", jobID);
                },
                executor);
    }

    /**
     * Create the checkpoint recovery factory for the job manager.
     *
     * @return Checkpoint recovery factory
     */
    protected abstract CheckpointRecoveryFactory createCheckpointRecoveryFactory() throws Exception;

    /**
     * Create the submitted job graph store for the job manager.
     *
     * @return Submitted job graph store
     * @throws Exception if the submitted job graph store could not be created
     */
    protected abstract JobGraphStore createJobGraphStore() throws Exception;

    /**
     * Closes the components which is used for external operations(e.g. Zookeeper Client, Kubernetes
     * Client).
     *
     * @throws Exception if the close operation failed
     */
    protected abstract void internalClose() throws Exception;

    /**
     * Clean up the meta data in the distributed system(e.g. Zookeeper, Kubernetes ConfigMap).
     *
     * <p>If an exception occurs during internal cleanup, we will continue the cleanup in {@link
     * #cleanupAllData} and report exceptions only after all cleanup steps have been attempted.
     *
     * @throws Exception when do the cleanup operation on external storage.
     */
    protected abstract void internalCleanup() throws Exception;

    /**
     * Clean up the meta data in the distributed system(e.g. Zookeeper, Kubernetes ConfigMap) for
     * the specified Job. Method implementations need to be thread-safe.
     *
     * @param jobID The identifier of the job to cleanup.
     * @throws Exception when do the cleanup operation on external storage.
     */
    protected abstract void internalCleanupJobData(JobID jobID) throws Exception;
}
