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
import org.apache.flink.runtime.leaderservice.LeaderServices;
import org.apache.flink.runtime.persistentservice.PersistentServices;
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
 */
public class HighAvailabilityServicesImpl implements HighAvailabilityServices {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /** The executor to run external IO operations on. */
    private final LeaderServices leaderServices;

    /** The runtime configuration. */
    private final PersistentServices persistentServices;

    public HighAvailabilityServicesImpl(
            LeaderServices leaderServices, PersistentServices persistentServices) {
        this.leaderServices = checkNotNull(leaderServices);
        this.persistentServices = checkNotNull(persistentServices);
    }

    @Override
    public LeaderServices getLeaderServices() {
        return leaderServices;
    }

    @Override
    public PersistentServices getPersistentServices() {
        return persistentServices;
    }

    @Override
    public void close() throws Exception {
        Throwable exception = null;

        try {
            persistentServices.close();
        } catch (Throwable t) {
            exception = t;
        }

        try {
            leaderServices.close();
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
            leaderServices.cleanupServices();
            deletedHAData = true;
            persistentServices.cleanup();
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
                        leaderServices.cleanupJobData(jobID);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                    logger.info(
                            "Finished cleaning up the high availability data for job {}.", jobID);
                },
                executor);
    }
}
