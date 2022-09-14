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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Utilities for the web runtime monitor. This class contains for example methods to build messages
 * with aggregate information about the state of an execution graph, to be send to the web server.
 */
public final class WebMonitorUtils {

    private static final String WEB_FRONTEND_BOOTSTRAP_CLASS_FQN =
            "org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap";

    private static final Logger LOG = LoggerFactory.getLogger(WebMonitorUtils.class);

    /** Singleton to hold the log file, the stdout file, the log directory. */
    public static class LogFileLocation {

        public final File logFile;
        public final File stdOutFile;
        public final File logDir;

        private LogFileLocation(
                @Nullable File logFile, @Nullable File stdOutFile, @Nullable File logDir) {
            this.logFile = logFile;
            this.stdOutFile = stdOutFile;
            this.logDir = logDir;
        }

        /**
         * Finds the Flink log directory using log.file Java property that is set during startup.
         */
        public static LogFileLocation find(Configuration config) {
            final String logEnv = "log.file";
            String logFilePath = System.getProperty(logEnv);

            if (logFilePath == null) {
                LOG.warn("Log file environment variable '{}' is not set.", logEnv);
                logFilePath = config.getString(WebOptions.LOG_PATH);
            }

            // not configured, cannot serve log files
            if (logFilePath == null || logFilePath.length() < 4) {
                LOG.warn(
                        "JobManager log files are unavailable in the web dashboard. "
                                + "Log file location not found in environment variable '{}' or configuration key '{}'.",
                        logEnv,
                        WebOptions.LOG_PATH.key());
                return new LogFileLocation(null, null, null);
            }

            String outFilePath = logFilePath.substring(0, logFilePath.length() - 3).concat("out");
            File logFile = resolveFileLocation(logFilePath);
            File logDir = null;
            if (logFile != null) {
                logDir = resolveFileLocation(logFile.getParent());
            }

            LOG.info("Determined location of main cluster component log file: {}", logFilePath);
            LOG.info("Determined location of main cluster component stdout file: {}", outFilePath);

            return new LogFileLocation(logFile, resolveFileLocation(outFilePath), logDir);
        }

        /**
         * Verify log file location.
         *
         * @param logFilePath Path to log file
         * @return File or null if not a valid log file
         */
        private static File resolveFileLocation(String logFilePath) {
            File logFile = new File(logFilePath);
            return (logFile.exists() && logFile.canRead()) ? logFile : null;
        }
    }

    /**
     * Checks whether the flink-runtime-web dependency is available and if so returns a
     * StaticFileServerHandler which can serve the static file contents.
     *
     * @param leaderRetriever to be used by the StaticFileServerHandler
     * @param timeout for lookup requests
     * @param tmpDir to be used by the StaticFileServerHandler to store temporary files
     * @param <T> type of the gateway to retrieve
     * @return StaticFileServerHandler if flink-runtime-web is in the classpath; Otherwise
     *     Optional.empty
     * @throws IOException if we cannot create the StaticFileServerHandler
     */
    public static <T extends RestfulGateway> Optional<StaticFileServerHandler<T>> tryLoadWebContent(
            GatewayRetriever<? extends T> leaderRetriever, Time timeout, File tmpDir)
            throws IOException {

        if (isFlinkRuntimeWebInClassPath()) {
            return Optional.of(new StaticFileServerHandler<>(leaderRetriever, timeout, tmpDir));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Loads the {@link WebMonitorExtension} which enables web submission.
     *
     * @param leaderRetriever to retrieve the leader
     * @param timeout for asynchronous requests
     * @param responseHeaders for the web submission handlers
     * @param localAddressFuture of the underlying REST server endpoint
     * @param uploadDir where the web submission handler store uploaded jars
     * @param executor to run asynchronous operations
     * @param configuration used to instantiate the web submission extension
     * @return Web submission extension
     * @throws FlinkException if the web submission extension could not be loaded
     */
    public static WebMonitorExtension loadWebSubmissionExtension(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            CompletableFuture<String> localAddressFuture,
            java.nio.file.Path uploadDir,
            Executor executor,
            Configuration configuration)
            throws FlinkException {

        if (isFlinkRuntimeWebInClassPath()) {
            try {
                final Constructor<?> webSubmissionExtensionConstructor =
                        Class.forName("org.apache.flink.runtime.webmonitor.WebSubmissionExtension")
                                .getConstructor(
                                        Configuration.class,
                                        GatewayRetriever.class,
                                        Map.class,
                                        CompletableFuture.class,
                                        java.nio.file.Path.class,
                                        Executor.class,
                                        Time.class);

                return (WebMonitorExtension)
                        webSubmissionExtensionConstructor.newInstance(
                                configuration,
                                leaderRetriever,
                                responseHeaders,
                                localAddressFuture,
                                uploadDir,
                                executor,
                                timeout);
            } catch (ClassNotFoundException
                    | NoSuchMethodException
                    | InstantiationException
                    | InvocationTargetException
                    | IllegalAccessException e) {
                throw new FlinkException("Could not load web submission extension.", e);
            }
        } else {
            throw new FlinkException(
                    "The module flink-runtime-web could not be found in the class path. Please add "
                            + "this jar in order to enable web based job submission.");
        }
    }

    /** Private constructor to prevent instantiation. */
    private WebMonitorUtils() {
        throw new RuntimeException();
    }

    /**
     * Returns {@code true} if the optional dependency {@code flink-runtime-web} is in the
     * classpath.
     */
    private static boolean isFlinkRuntimeWebInClassPath() {
        try {
            Class.forName(WEB_FRONTEND_BOOTSTRAP_CLASS_FQN);
            return true;
        } catch (ClassNotFoundException e) {
            // class not found means that there is no flink-runtime-web in the classpath
            return false;
        }
    }
}
