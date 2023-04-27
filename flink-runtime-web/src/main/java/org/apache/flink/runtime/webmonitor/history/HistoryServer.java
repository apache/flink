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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rest.handler.job.GeneratedLogUrlHandler;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.JobManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.TaskManagerLogUrlHeaders;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.Runnables;
import org.apache.flink.runtime.webmonitor.utils.LogUrlUtil;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The HistoryServer provides a WebInterface and REST API to retrieve information about finished
 * jobs for which the JobManager may have already shut down.
 *
 * <p>The HistoryServer regularly checks a set of directories for job archives created by the {@link
 * FsJobArchivist} and caches these in a local directory. See {@link HistoryServerArchiveFetcher}.
 *
 * <p>All configuration options are defined in{@link HistoryServerOptions}.
 *
 * <p>The WebInterface only displays the "Completed Jobs" page.
 *
 * <p>The REST API is limited to
 *
 * <ul>
 *   <li>/config
 *   <li>/joboverview
 *   <li>/jobs/:jobid/*
 * </ul>
 *
 * <p>and relies on static files that are served by the {@link
 * HistoryServerStaticFileServerHandler}.
 */
public class HistoryServer {

    private static final Logger LOG = LoggerFactory.getLogger(HistoryServer.class);
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    private final Configuration config;

    private final String webAddress;
    private final int webPort;
    private final long webRefreshIntervalMillis;
    private final File webDir;

    private final HistoryServerArchiveFetcher archiveFetcher;

    @Nullable private final SSLHandlerFactory serverSSLFactory;
    private WebFrontendBootstrap netty;

    private final long refreshIntervalMillis;
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("Flink-HistoryServer-ArchiveFetcher"));

    private final Object startupShutdownLock = new Object();
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final Thread shutdownHook;

    public static void main(String[] args) throws Exception {
        EnvironmentInformation.logEnvironmentInfo(LOG, "HistoryServer", args);

        ParameterTool pt = ParameterTool.fromArgs(args);
        String configDir = pt.getRequired("configDir");

        LOG.info("Loading configuration from {}", configDir);
        final Configuration flinkConfig = GlobalConfiguration.loadConfiguration(configDir);

        FileSystem.initialize(
                flinkConfig, PluginUtils.createPluginManagerFromRootFolder(flinkConfig));

        // run the history server
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        try {
            SecurityUtils.getInstalledContext()
                    .runSecured(
                            new Callable<Integer>() {
                                @Override
                                public Integer call() throws Exception {
                                    HistoryServer hs = new HistoryServer(flinkConfig);
                                    hs.run();
                                    return 0;
                                }
                            });
            System.exit(0);
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            LOG.error("Failed to run HistoryServer.", strippedThrowable);
            strippedThrowable.printStackTrace();
            System.exit(1);
        }
    }

    public HistoryServer(Configuration config) throws IOException, FlinkException {
        this(config, (event) -> {});
    }

    /**
     * Creates HistoryServer instance.
     *
     * @param config configuration
     * @param jobArchiveEventListener Listener for job archive operations. First param is operation,
     *     second param is id of the job.
     * @throws IOException When creation of SSL factory failed
     * @throws FlinkException When configuration error occurred
     */
    public HistoryServer(
            Configuration config,
            Consumer<HistoryServerArchiveFetcher.ArchiveEvent> jobArchiveEventListener)
            throws IOException, FlinkException {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(jobArchiveEventListener);

        this.config = config;
        if (HistoryServerUtils.isSSLEnabled(config)) {
            LOG.info("Enabling SSL for the history server.");
            try {
                this.serverSSLFactory = SSLUtils.createRestServerSSLEngineFactory(config);
            } catch (Exception e) {
                throw new IOException("Failed to initialize SSLContext for the history server.", e);
            }
        } else {
            this.serverSSLFactory = null;
        }

        webAddress = config.getString(HistoryServerOptions.HISTORY_SERVER_WEB_ADDRESS);
        webPort = config.getInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT);
        webRefreshIntervalMillis =
                config.getLong(HistoryServerOptions.HISTORY_SERVER_WEB_REFRESH_INTERVAL);

        String webDirectory = config.getString(HistoryServerOptions.HISTORY_SERVER_WEB_DIR);
        if (webDirectory == null) {
            webDirectory =
                    System.getProperty("java.io.tmpdir")
                            + File.separator
                            + "flink-web-history-"
                            + UUID.randomUUID();
        }
        webDir = new File(webDirectory);

        boolean cleanupExpiredArchives =
                config.getBoolean(HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS);

        String refreshDirectories =
                config.getString(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS);
        if (refreshDirectories == null) {
            throw new FlinkException(
                    HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS + " was not configured.");
        }
        List<RefreshLocation> refreshDirs = new ArrayList<>();
        for (String refreshDirectory : refreshDirectories.split(",")) {
            try {
                Path refreshPath = new Path(refreshDirectory);
                FileSystem refreshFS = refreshPath.getFileSystem();
                refreshDirs.add(new RefreshLocation(refreshPath, refreshFS));
            } catch (Exception e) {
                // there's most likely something wrong with the path itself, so we ignore it from
                // here on
                LOG.warn(
                        "Failed to create Path or FileSystem for directory '{}'. Directory will not be monitored.",
                        refreshDirectory,
                        e);
            }
        }

        if (refreshDirs.isEmpty()) {
            throw new FlinkException(
                    "Failed to validate any of the configured directories to monitor.");
        }

        refreshIntervalMillis =
                config.getLong(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL);
        int maxHistorySize = config.getInteger(HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS);
        if (maxHistorySize == 0 || maxHistorySize < -1) {
            throw new IllegalConfigurationException(
                    "Cannot set %s to 0 or less than -1",
                    HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS.key());
        }
        archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs,
                        webDir,
                        jobArchiveEventListener,
                        cleanupExpiredArchives,
                        maxHistorySize);

        this.shutdownHook =
                ShutdownHookUtil.addShutdownHook(
                        HistoryServer.this::stop, HistoryServer.class.getSimpleName(), LOG);
    }

    @VisibleForTesting
    int getWebPort() {
        return netty.getServerPort();
    }

    @VisibleForTesting
    void fetchArchives() {
        executor.execute(getArchiveFetchingRunnable());
    }

    public void run() {
        try {
            start();
            new CountDownLatch(1).await();
        } catch (Exception e) {
            LOG.error("Failure while running HistoryServer.", e);
        } finally {
            stop();
        }
    }

    // ------------------------------------------------------------------------
    // Life-cycle
    // ------------------------------------------------------------------------

    void start() throws IOException, InterruptedException {
        synchronized (startupShutdownLock) {
            LOG.info("Starting history server.");

            Files.createDirectories(webDir.toPath());
            LOG.info("Using directory {} as local cache.", webDir);

            Router router = new Router();

            LogUrlUtil.getValidLogUrlPattern(
                            config, HistoryServerOptions.HISTORY_SERVER_JOBMANAGER_LOG_URL_PATTERN)
                    .ifPresent(
                            pattern ->
                                    router.addGet(
                                            JobManagerLogUrlHeaders.getInstance()
                                                    .getTargetRestEndpointURL(),
                                            new GeneratedLogUrlHandler(
                                                    CompletableFuture.completedFuture(pattern))));
            LogUrlUtil.getValidLogUrlPattern(
                            config, HistoryServerOptions.HISTORY_SERVER_TASKMANAGER_LOG_URL_PATTERN)
                    .ifPresent(
                            pattern ->
                                    router.addGet(
                                            TaskManagerLogUrlHeaders.getInstance()
                                                    .getTargetRestEndpointURL(),
                                            new GeneratedLogUrlHandler(
                                                    CompletableFuture.completedFuture(pattern))));

            router.addGet("/:*", new HistoryServerStaticFileServerHandler(webDir));

            createDashboardConfigFile();

            executor.scheduleWithFixedDelay(
                    getArchiveFetchingRunnable(), 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);

            netty =
                    new WebFrontendBootstrap(
                            router, LOG, webDir, serverSSLFactory, webAddress, webPort, config);
        }
    }

    private Runnable getArchiveFetchingRunnable() {
        return Runnables.withUncaughtExceptionHandler(
                () -> archiveFetcher.fetchArchives(), FatalExitExceptionHandler.INSTANCE);
    }

    void stop() {
        if (shutdownRequested.compareAndSet(false, true)) {
            synchronized (startupShutdownLock) {
                LOG.info("Stopping history server.");

                try {
                    netty.shutdown();
                } catch (Throwable t) {
                    LOG.warn("Error while shutting down WebFrontendBootstrap.", t);
                }

                ExecutorUtils.gracefulShutdown(1, TimeUnit.SECONDS, executor);

                try {
                    LOG.info("Removing web dashboard root cache directory {}", webDir);
                    FileUtils.deleteDirectory(webDir);
                } catch (Throwable t) {
                    LOG.warn("Error while deleting web root directory {}", webDir, t);
                }

                LOG.info("Stopped history server.");

                // Remove shutdown hook to prevent resource leaks
                ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
            }
        }
    }

    // ------------------------------------------------------------------------
    // File generation
    // ------------------------------------------------------------------------

    static FileWriter createOrGetFile(File folder, String name) throws IOException {
        File file = new File(folder, name + ".json");
        if (!file.exists()) {
            Files.createFile(file.toPath());
        }
        FileWriter fr = new FileWriter(file);
        return fr;
    }

    private void createDashboardConfigFile() throws IOException {
        try (FileWriter fw = createOrGetFile(webDir, "config")) {
            fw.write(
                    createConfigJson(
                            DashboardConfiguration.from(
                                    webRefreshIntervalMillis,
                                    ZonedDateTime.now(),
                                    false,
                                    false,
                                    false,
                                    true)));
            fw.flush();
        } catch (IOException ioe) {
            LOG.error("Failed to write config file.");
            throw ioe;
        }
    }

    private static String createConfigJson(DashboardConfiguration dashboardConfiguration)
            throws IOException {
        return OBJECT_MAPPER.writeValueAsString(dashboardConfiguration);
    }

    /** Container for the {@link Path} and {@link FileSystem} of a refresh directory. */
    static class RefreshLocation {
        private final Path path;
        private final FileSystem fs;

        private RefreshLocation(Path path, FileSystem fs) {
            this.path = path;
            this.fs = fs;
        }

        public Path getPath() {
            return path;
        }

        public FileSystem getFs() {
            return fs;
        }
    }
}
