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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava33.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava33.com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Store for {@link ArchivedApplication} instances. The store writes the archived application to
 * disk and keeps the most recently used applications in a memory cache for faster serving.
 * Moreover, the stored applications are periodically cleaned up.
 */
public class FileArchivedApplicationStore implements ArchivedApplicationStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileArchivedApplicationStore.class);

    private final File storageDir;

    private final Cache<ApplicationID, ApplicationDetails> applicationDetailsCache;

    private final Cache<JobID, JobDetails> jobDetailsCache;

    private final LoadingCache<ApplicationID, ArchivedApplication> archivedApplicationCache;

    private final Map<JobID, ApplicationID> jobIdToApplicationId = new HashMap<>();

    private final Map<ApplicationID, List<JobID>> applicationIdToJobIds = new HashMap<>();

    private final ScheduledFuture<?> cleanupFuture;

    private int numFinishedJobs;

    private int numFailedJobs;

    private int numCanceledJobs;

    public FileArchivedApplicationStore(
            File rootDir,
            Duration expirationTime,
            int maximumCapacity,
            long maximumCacheSizeBytes,
            ScheduledExecutor scheduledExecutor,
            Ticker ticker)
            throws IOException {

        final File storageDirectory = initArchivedApplicationStorageDirectory(rootDir);

        LOG.info(
                "Initializing {}: Storage directory {}, expiration time {}, maximum cache size {} bytes.",
                FileArchivedApplicationStore.class.getSimpleName(),
                storageDirectory,
                expirationTime.toMillis(),
                maximumCacheSizeBytes);

        this.storageDir = Preconditions.checkNotNull(storageDirectory);
        Preconditions.checkArgument(
                storageDirectory.exists() && storageDirectory.isDirectory(),
                "The storage directory must exist and be a directory.");
        this.applicationDetailsCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(expirationTime.toMillis(), TimeUnit.MILLISECONDS)
                        .maximumSize(maximumCapacity)
                        .removalListener(
                                (RemovalListener<ApplicationID, ApplicationDetails>)
                                        notification ->
                                                deleteArchivedApplication(notification.getKey()))
                        .ticker(ticker)
                        .build();
        this.jobDetailsCache = CacheBuilder.newBuilder().build();

        this.archivedApplicationCache =
                CacheBuilder.newBuilder()
                        .maximumWeight(maximumCacheSizeBytes)
                        .weigher(this::calculateSize)
                        .build(
                                new CacheLoader<ApplicationID, ArchivedApplication>() {
                                    @Override
                                    public ArchivedApplication load(ApplicationID applicationId)
                                            throws Exception {
                                        return loadArchivedApplication(applicationId);
                                    }
                                });

        this.cleanupFuture =
                scheduledExecutor.scheduleWithFixedDelay(
                        () -> {
                            applicationDetailsCache.cleanUp();
                            jobDetailsCache.cleanUp();
                        },
                        expirationTime.toMillis(),
                        expirationTime.toMillis(),
                        TimeUnit.MILLISECONDS);

        this.numFinishedJobs = 0;
        this.numFailedJobs = 0;
        this.numCanceledJobs = 0;
    }

    @Override
    public int size() {
        return Math.toIntExact(applicationDetailsCache.size());
    }

    @Override
    public Optional<ArchivedApplication> get(ApplicationID applicationId) {
        try {
            return Optional.of(archivedApplicationCache.get(applicationId));
        } catch (ExecutionException e) {
            LOG.debug(
                    "Could not load archived application for application id {}.", applicationId, e);
            return Optional.empty();
        }
    }

    @Override
    public void put(ArchivedApplication archivedApplication) throws IOException {
        final ApplicationID applicationId = archivedApplication.getApplicationId();

        final ApplicationState status = archivedApplication.getApplicationStatus();
        final String name = archivedApplication.getApplicationName();

        Preconditions.checkArgument(
                status.isTerminalState(),
                "The application "
                        + name
                        + '('
                        + applicationId
                        + ") is not in a terminal state. Instead it is in state "
                        + status
                        + '.');

        for (ExecutionGraphInfo executionGraphInfo : archivedApplication.getJobs().values()) {
            final JobID jobId = executionGraphInfo.getArchivedExecutionGraph().getJobID();

            final JobStatus jobStatus = executionGraphInfo.getArchivedExecutionGraph().getState();
            switch (jobStatus) {
                case FINISHED:
                    numFinishedJobs++;
                    break;
                case CANCELED:
                    numCanceledJobs++;
                    break;
                case FAILED:
                    numFailedJobs++;
                    break;
                case SUSPENDED:
                    break;
                default:
                    throw new IllegalStateException(
                            "The job "
                                    + executionGraphInfo.getArchivedExecutionGraph().getJobName()
                                    + '('
                                    + jobId
                                    + ") should have been in a known terminal state. "
                                    + "Instead it was in state "
                                    + jobStatus
                                    + '.');
            }

            jobIdToApplicationId.put(jobId, applicationId);
            applicationIdToJobIds
                    .computeIfAbsent(applicationId, ignored -> new ArrayList<>())
                    .add(jobId);
            jobDetailsCache.put(jobId, JobDetails.createDetailsForJob(executionGraphInfo));
        }

        storeArchivedApplication(archivedApplication);

        final ApplicationDetails detailsForApplication =
                ApplicationDetails.fromArchivedApplication(archivedApplication);

        applicationDetailsCache.put(applicationId, detailsForApplication);
        archivedApplicationCache.put(applicationId, archivedApplication);
    }

    @Override
    public Collection<ApplicationDetails> getApplicationDetails() {
        return applicationDetailsCache.asMap().values();
    }

    @Override
    public Optional<ExecutionGraphInfo> getExecutionGraphInfo(JobID jobId) {
        try {
            final ApplicationID applicationId = jobIdToApplicationId.get(jobId);
            if (applicationId == null) {
                return Optional.empty();
            }
            return Optional.of(archivedApplicationCache.get(applicationId))
                    .map(archivedApplication -> archivedApplication.getJobs().get(jobId));
        } catch (ExecutionException e) {
            LOG.debug(
                    "Could not load archived execution graph information for job id {}.", jobId, e);
            return Optional.empty();
        }
    }

    @Override
    public Collection<JobDetails> getJobDetails() {
        return jobDetailsCache.asMap().values();
    }

    @Override
    public JobsOverview getJobsOverview() {
        return new JobsOverview(0, numFinishedJobs, numCanceledJobs, numFailedJobs);
    }

    @Override
    public void close() throws IOException {
        cleanupFuture.cancel(false);

        applicationDetailsCache.invalidateAll();

        // clean up the storage directory
        FileUtils.deleteFileOrDirectory(storageDir);
    }

    // --------------------------------------------------------------
    // Internal methods
    // --------------------------------------------------------------

    private int calculateSize(
            ApplicationID applicationId, ArchivedApplication archivedApplication) {
        final File archivedApplicationFile = getArchivedApplicationFile(applicationId);

        if (archivedApplicationFile.exists()) {
            return Math.toIntExact(archivedApplicationFile.length());
        } else {
            LOG.debug(
                    "Could not find archived application file for {}. Estimating the size instead.",
                    applicationId);
            int estimatedSize = 0;
            for (ExecutionGraphInfo executionGraphInfo : archivedApplication.getJobs().values()) {
                final ArchivedExecutionGraph archivedExecutionGraph =
                        executionGraphInfo.getArchivedExecutionGraph();
                estimatedSize +=
                        archivedExecutionGraph.getAllVertices().size() * 1000
                                + archivedExecutionGraph.getAccumulatorsSerialized().size() * 1000;
            }
            return estimatedSize;
        }
    }

    private ArchivedApplication loadArchivedApplication(ApplicationID applicationId)
            throws IOException, ClassNotFoundException {
        final File archivedApplicationFile = getArchivedApplicationFile(applicationId);

        if (archivedApplicationFile.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(archivedApplicationFile)) {
                return InstantiationUtil.deserializeObject(
                        fileInputStream, getClass().getClassLoader());
            }
        } else {
            throw new FileNotFoundException(
                    "Could not find file for archived application "
                            + applicationId
                            + ". This indicates that the file either has been deleted or never written.");
        }
    }

    private void storeArchivedApplication(ArchivedApplication archivedApplication)
            throws IOException {
        final File archivedApplicationFile =
                getArchivedApplicationFile(archivedApplication.getApplicationId());

        try (FileOutputStream fileOutputStream = new FileOutputStream(archivedApplicationFile)) {
            InstantiationUtil.serializeObject(fileOutputStream, archivedApplication);
        }
    }

    private File getArchivedApplicationFile(ApplicationID applicationId) {
        return new File(storageDir, applicationId.toString());
    }

    private void deleteArchivedApplication(ApplicationID applicationId) {
        Preconditions.checkNotNull(applicationId);

        final File archivedApplicationFile = getArchivedApplicationFile(applicationId);

        try {
            FileUtils.deleteFileOrDirectory(archivedApplicationFile);
        } catch (IOException e) {
            LOG.debug("Could not delete file {}.", archivedApplicationFile, e);
        }

        archivedApplicationCache.invalidate(applicationId);
        applicationDetailsCache.invalidate(applicationId);
        List<JobID> jobIds = applicationIdToJobIds.remove(applicationId);
        if (jobIds != null) {
            jobIds.forEach(
                    jobId -> {
                        jobIdToApplicationId.remove(jobId);
                        jobDetailsCache.invalidate(jobId);
                    });
        }
    }

    private static File initArchivedApplicationStorageDirectory(File tmpDir) throws IOException {
        final int maxAttempts = 10;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            final File storageDirectory =
                    new File(tmpDir, "archivedApplicationStore-" + UUID.randomUUID());

            if (storageDirectory.mkdir()) {
                return storageDirectory;
            }
        }

        throw new IOException(
                "Could not create archivedApplicationStorage directory in " + tmpDir + '.');
    }

    // --------------------------------------------------------------
    // Testing methods
    // --------------------------------------------------------------

    @VisibleForTesting
    File getStorageDir() {
        return storageDir;
    }

    @VisibleForTesting
    LoadingCache<ApplicationID, ArchivedApplication> getArchivedApplicationCache() {
        return archivedApplicationCache;
    }
}
