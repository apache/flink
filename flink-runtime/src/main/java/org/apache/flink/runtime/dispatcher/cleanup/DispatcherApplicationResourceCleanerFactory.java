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
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.jobmanager.ApplicationWriter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.util.concurrent.Executor;

/**
 * {@code DispatcherApplicationResourceCleanerFactory} instantiates {@link
 * ApplicationResourceCleaner} instances that clean cleanable resources from the {@link
 * org.apache.flink.runtime.dispatcher.Dispatcher}.
 */
public class DispatcherApplicationResourceCleanerFactory
        implements ApplicationResourceCleanerFactory {

    private static final String APPLICATION_STORE_LABEL = "ApplicationStore";
    private static final String BLOB_SERVER_LABEL = "BlobServer";

    private final Executor cleanupExecutor;
    private final RetryStrategy retryStrategy;

    private final ApplicationWriter applicationWriter;
    private final BlobServer blobServer;

    public DispatcherApplicationResourceCleanerFactory(DispatcherServices dispatcherServices) {
        this(
                dispatcherServices.getIoExecutor(),
                CleanupRetryStrategyFactory.INSTANCE.createRetryStrategy(
                        dispatcherServices.getConfiguration()),
                dispatcherServices.getApplicationWriter(),
                dispatcherServices.getBlobServer());
    }

    @VisibleForTesting
    public DispatcherApplicationResourceCleanerFactory(
            Executor cleanupExecutor,
            RetryStrategy retryStrategy,
            ApplicationWriter applicationWriter,
            BlobServer blobServer) {
        this.cleanupExecutor = Preconditions.checkNotNull(cleanupExecutor);
        this.retryStrategy = retryStrategy;
        this.applicationWriter = Preconditions.checkNotNull(applicationWriter);
        this.blobServer = Preconditions.checkNotNull(blobServer);
    }

    @Override
    public ApplicationResourceCleaner createApplicationResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return DefaultApplicationResourceCleaner.forGloballyCleanableResources(
                        mainThreadExecutor, cleanupExecutor, retryStrategy)
                .withRegularCleanup(APPLICATION_STORE_LABEL, applicationWriter)
                .withRegularCleanup(BLOB_SERVER_LABEL, blobServer)
                .build();
    }
}
