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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerRegistry;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.util.concurrent.Executor;

/**
 * {@code DispatcherResourceCleanerFactory} instantiates {@link ResourceCleaner} instances that
 * clean cleanable resources from the {@link org.apache.flink.runtime.dispatcher.Dispatcher}.
 *
 * <p>We need to handle the {@link JobManagerRunnerRegistry} differently due to a dependency between
 * closing the {@link org.apache.flink.runtime.jobmaster.JobManagerRunner} and the {@link
 * HighAvailabilityServices}. This is fixed in {@code FLINK-24038} using a feature flag to
 * enable/disable single leader election for all the {@code JobManager} components. We can remove
 * the priority cleanup logic after removing the per-component leader election.
 */
public class DispatcherResourceCleanerFactory implements ResourceCleanerFactory {

    private final Executor cleanupExecutor;
    private final RetryStrategy retryStrategy;

    private final JobManagerRunnerRegistry jobManagerRunnerRegistry;
    private final JobGraphWriter jobGraphWriter;
    private final BlobServer blobServer;
    private final HighAvailabilityServices highAvailabilityServices;
    private final JobManagerMetricGroup jobManagerMetricGroup;

    public DispatcherResourceCleanerFactory(
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            DispatcherServices dispatcherServices) {
        this(
                dispatcherServices.getIoExecutor(),
                createExponentialRetryStategy(dispatcherServices.getConfiguration()),
                jobManagerRunnerRegistry,
                dispatcherServices.getJobGraphWriter(),
                dispatcherServices.getBlobServer(),
                dispatcherServices.getHighAvailabilityServices(),
                dispatcherServices.getJobManagerMetricGroup());
    }

    @VisibleForTesting
    public DispatcherResourceCleanerFactory(
            Executor cleanupExecutor,
            RetryStrategy retryStrategy,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            JobGraphWriter jobGraphWriter,
            BlobServer blobServer,
            HighAvailabilityServices highAvailabilityServices,
            JobManagerMetricGroup jobManagerMetricGroup) {
        this.cleanupExecutor = Preconditions.checkNotNull(cleanupExecutor);
        this.retryStrategy = retryStrategy;
        this.jobManagerRunnerRegistry = Preconditions.checkNotNull(jobManagerRunnerRegistry);
        this.jobGraphWriter = Preconditions.checkNotNull(jobGraphWriter);
        this.blobServer = Preconditions.checkNotNull(blobServer);
        this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
        this.jobManagerMetricGroup = Preconditions.checkNotNull(jobManagerMetricGroup);
    }

    @Override
    public ResourceCleaner createLocalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return DefaultResourceCleaner.forLocallyCleanableResources(
                        mainThreadExecutor, cleanupExecutor, retryStrategy)
                .withPrioritizedCleanup(jobManagerRunnerRegistry)
                .withRegularCleanup(jobGraphWriter)
                .withRegularCleanup(blobServer)
                .withRegularCleanup(jobManagerMetricGroup)
                .build();
    }

    @Override
    public ResourceCleaner createGlobalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {

        return DefaultResourceCleaner.forGloballyCleanableResources(
                        mainThreadExecutor, cleanupExecutor, retryStrategy)
                .withPrioritizedCleanup(jobManagerRunnerRegistry)
                .withRegularCleanup(jobGraphWriter)
                .withRegularCleanup(blobServer)
                .withRegularCleanup(highAvailabilityServices)
                .build();
    }

    private static RetryStrategy createExponentialRetryStategy(Configuration config) {
        return new ExponentialBackoffRetryStrategy(
                Integer.MAX_VALUE,
                config.get(JobManagerOptions.JOB_CLEANUP_MINIMUM_DELAY),
                config.get(JobManagerOptions.JOB_CLEANUP_MAXIMUM_DELAY));
    }
}
