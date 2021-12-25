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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleMasterContextImpl;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class which holds all auxiliary shared services used by the {@link JobMaster}.
 * Consequently, the {@link JobMaster} should never shut these services down.
 */
public class JobManagerSharedServices {

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final ScheduledExecutorService futureExecutor;

    private final ExecutorService ioExecutor;

    private final LibraryCacheManager libraryCacheManager;

    private final ShuffleMaster<?> shuffleMaster;

    @Nonnull private final BlobWriter blobWriter;

    public JobManagerSharedServices(
            ScheduledExecutorService futureExecutor,
            ExecutorService ioExecutor,
            LibraryCacheManager libraryCacheManager,
            ShuffleMaster<?> shuffleMaster,
            @Nonnull BlobWriter blobWriter) {

        this.futureExecutor = checkNotNull(futureExecutor);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.libraryCacheManager = checkNotNull(libraryCacheManager);
        this.shuffleMaster = checkNotNull(shuffleMaster);
        this.blobWriter = blobWriter;
    }

    public ScheduledExecutorService getFutureExecutor() {
        return futureExecutor;
    }

    public Executor getIoExecutor() {
        return ioExecutor;
    }

    public LibraryCacheManager getLibraryCacheManager() {
        return libraryCacheManager;
    }

    public ShuffleMaster<?> getShuffleMaster() {
        return shuffleMaster;
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
        Throwable exception = null;

        try {
            ExecutorUtils.gracefulShutdown(
                    SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS, futureExecutor, ioExecutor);
        } catch (Throwable t) {
            exception = t;
        }

        try {
            shuffleMaster.close();
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        libraryCacheManager.shutdown();

        if (exception != null) {
            ExceptionUtils.rethrowException(
                    exception, "Error while shutting down JobManager services");
        }
    }

    // ------------------------------------------------------------------------
    //  Creating the components from a configuration
    // ------------------------------------------------------------------------

    public static JobManagerSharedServices fromConfiguration(
            Configuration config, BlobServer blobServer, FatalErrorHandler fatalErrorHandler)
            throws Exception {

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

        final int numberCPUCores = Hardware.getNumberCPUCores();
        final int jobManagerFuturePoolSize =
                config.getInteger(JobManagerOptions.JOB_MANAGER_FUTURE_POOL_SIZE, numberCPUCores);
        final ScheduledExecutorService futureExecutor =
                Executors.newScheduledThreadPool(
                        jobManagerFuturePoolSize, new ExecutorThreadFactory("jobmanager-future"));

        final int jobManagerIoPoolSize =
                config.getInteger(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, numberCPUCores);
        final ExecutorService ioExecutor =
                Executors.newFixedThreadPool(
                        jobManagerIoPoolSize, new ExecutorThreadFactory("jobmanager-io"));

        final ShuffleMasterContext shuffleMasterContext =
                new ShuffleMasterContextImpl(config, fatalErrorHandler);
        final ShuffleMaster<?> shuffleMaster =
                ShuffleServiceLoader.loadShuffleServiceFactory(config)
                        .createShuffleMaster(shuffleMasterContext);
        shuffleMaster.start();

        return new JobManagerSharedServices(
                futureExecutor, ioExecutor, libraryCacheManager, shuffleMaster, blobServer);
    }
}
