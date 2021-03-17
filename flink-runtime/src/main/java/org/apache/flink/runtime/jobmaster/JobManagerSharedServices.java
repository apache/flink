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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class which holds all auxiliary shared services used by the {@link JobMaster}.
 * Consequently, the {@link JobMaster} should never shut these services down.
 */
public class JobManagerSharedServices {

    private final ScheduledExecutorService scheduledExecutorService;

    private final LibraryCacheManager libraryCacheManager;

    @Nonnull private final BlobWriter blobWriter;

    public JobManagerSharedServices(
            ScheduledExecutorService scheduledExecutorService,
            LibraryCacheManager libraryCacheManager,
            @Nonnull BlobWriter blobWriter) {

        this.scheduledExecutorService = checkNotNull(scheduledExecutorService);
        this.libraryCacheManager = checkNotNull(libraryCacheManager);
        this.blobWriter = blobWriter;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public LibraryCacheManager getLibraryCacheManager() {
        return libraryCacheManager;
    }

    @Nonnull
    public BlobWriter getBlobWriter() {
        return blobWriter;
    }

    /**
     * Shutdown the {@link JobMaster} services.
     *
     * <p>This method makes sure all services are closed or shut down, even when an exception
     * occurred in the shutdown of one component. The first encountered exception is thrown, with
     * successive exceptions added as suppressed exceptions.
     *
     * @throws Exception The first Exception encountered during shutdown.
     */
    public void shutdown() throws Exception {
        Throwable firstException = null;

        try {
            scheduledExecutorService.shutdownNow();
        } catch (Throwable t) {
            firstException = t;
        }

        libraryCacheManager.shutdown();

        if (firstException != null) {
            ExceptionUtils.rethrowException(
                    firstException, "Error while shutting down JobManager services");
        }
    }

    // ------------------------------------------------------------------------
    //  Creating the components from a configuration
    // ------------------------------------------------------------------------

    public static JobManagerSharedServices fromConfiguration(
            Configuration config, BlobServer blobServer, FatalErrorHandler fatalErrorHandler) {

        checkNotNull(config);
        checkNotNull(blobServer);

        final String classLoaderResolveOrder =
                config.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);

        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(config);

        final boolean failOnJvmMetaspaceOomError =
                config.getBoolean(CoreOptions.FAIL_ON_USER_CLASS_LOADING_METASPACE_OOM);
        final boolean checkClassLoaderLeak =
                config.getBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER);
        final BlobLibraryCacheManager libraryCacheManager =
                new BlobLibraryCacheManager(
                        blobServer,
                        BlobLibraryCacheManager.defaultClassLoaderFactory(
                                FlinkUserCodeClassLoaders.ResolveOrder.fromString(
                                        classLoaderResolveOrder),
                                alwaysParentFirstLoaderPatterns,
                                failOnJvmMetaspaceOomError ? fatalErrorHandler : null,
                                checkClassLoaderLeak));

        final ScheduledExecutorService futureExecutor =
                Executors.newScheduledThreadPool(
                        Hardware.getNumberCPUCores(),
                        new ExecutorThreadFactory("jobmanager-future"));

        return new JobManagerSharedServices(futureExecutor, libraryCacheManager, blobServer);
    }
}
