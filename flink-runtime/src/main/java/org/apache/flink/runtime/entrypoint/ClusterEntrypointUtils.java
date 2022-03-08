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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.util.ClusterUncaughtExceptionHandler;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/** Utility class for {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}. */
public final class ClusterEntrypointUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointUtils.class);

    private ClusterEntrypointUtils() {
        throw new UnsupportedOperationException("This class should not be instantiated.");
    }

    /**
     * Parses passed String array using the parameter definitions of the passed {@code
     * ParserResultFactory}. The method will call {@code System.exit} and print the usage
     * information to stdout in case of a parsing error.
     *
     * @param args The String array that shall be parsed.
     * @param parserResultFactory The {@code ParserResultFactory} that collects the parameter
     *     parsing instructions.
     * @param mainClass The main class initiating the parameter parsing.
     * @param <T> The parsing result type.
     * @return The parsing result.
     */
    public static <T> T parseParametersOrExit(
            String[] args, ParserResultFactory<T> parserResultFactory, Class<?> mainClass) {
        final CommandLineParser<T> commandLineParser = new CommandLineParser<>(parserResultFactory);

        try {
            return commandLineParser.parse(args);
        } catch (Exception e) {
            LOG.error("Could not parse command line arguments {}.", args, e);
            commandLineParser.printHelp(mainClass.getSimpleName());
            System.exit(ClusterEntrypoint.STARTUP_FAILURE_RETURN_CODE);
        }

        return null;
    }

    /**
     * Tries to find the user library directory.
     *
     * @return the user library directory if it exits, returns {@link Optional#empty()} if there is
     *     none
     */
    public static Optional<File> tryFindUserLibDirectory() {
        final File flinkHomeDirectory = deriveFlinkHomeDirectoryFromLibDirectory();
        final File usrLibDirectory =
                new File(flinkHomeDirectory, ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);

        if (!usrLibDirectory.isDirectory()) {
            return Optional.empty();
        }
        return Optional.of(usrLibDirectory);
    }

    @Nullable
    private static File deriveFlinkHomeDirectoryFromLibDirectory() {
        final String libDirectory = System.getenv().get(ConfigConstants.ENV_FLINK_LIB_DIR);

        if (libDirectory == null) {
            return null;
        } else {
            return new File(libDirectory).getParentFile();
        }
    }

    /**
     * Gets and verify the io-executor pool size based on configuration.
     *
     * @param config The configuration to read.
     * @return The legal io-executor pool size.
     */
    public static int getPoolSize(Configuration config) {
        final int poolSize =
                config.getInteger(
                        ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE,
                        4 * Hardware.getNumberCPUCores());
        Preconditions.checkArgument(
                poolSize > 0,
                String.format(
                        "Illegal pool size (%s) of io-executor, please re-configure '%s'.",
                        poolSize, ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key()));
        return poolSize;
    }

    /**
     * Sets the uncaught exception handler for current thread based on configuration.
     *
     * @param config the configuration to read.
     */
    public static void configureUncaughtExceptionHandler(Configuration config) {
        Thread.setDefaultUncaughtExceptionHandler(
                new ClusterUncaughtExceptionHandler(
                        config.get(ClusterOptions.UNCAUGHT_EXCEPTION_HANDLING)));
    }

    /**
     * Creates the working directory for the TaskManager process. This method ensures that the
     * working directory exists.
     *
     * @param configuration to extract the required settings from
     * @param envelopedResourceId identifying the TaskManager process
     * @return working directory
     * @throws IOException if the working directory could not be created
     */
    public static DeterminismEnvelope<WorkingDirectory> createTaskManagerWorkingDirectory(
            Configuration configuration, DeterminismEnvelope<ResourceID> envelopedResourceId)
            throws IOException {
        return envelopedResourceId.map(
                resourceId ->
                        WorkingDirectory.create(
                                generateTaskManagerWorkingDirectoryFile(
                                        configuration, resourceId)));
    }

    /**
     * Generates the working directory {@link File} for the TaskManager process. This method does
     * not ensure that the working directory exists.
     *
     * @param configuration to extract the required settings from
     * @param resourceId identifying the TaskManager process
     * @return working directory file
     */
    @VisibleForTesting
    public static File generateTaskManagerWorkingDirectoryFile(
            Configuration configuration, ResourceID resourceId) {
        return generateWorkingDirectoryFile(
                configuration,
                Optional.of(ClusterOptions.TASK_MANAGER_PROCESS_WORKING_DIR_BASE),
                "tm_" + resourceId);
    }

    /**
     * Generates the working directory {@link File} for the JobManager process. This method does not
     * ensure that the working directory exists.
     *
     * @param configuration to extract the required settings from
     * @param resourceId identifying the JobManager process
     * @return working directory file
     */
    @VisibleForTesting
    public static File generateJobManagerWorkingDirectoryFile(
            Configuration configuration, ResourceID resourceId) {
        return generateWorkingDirectoryFile(
                configuration,
                Optional.of(ClusterOptions.JOB_MANAGER_PROCESS_WORKING_DIR_BASE),
                "jm_" + resourceId);
    }

    /**
     * Generate the working directory from the given configuration. If a working dir option is
     * specified, then this config option will be read first. At last, {@link CoreOptions#TMP_DIRS}
     * will be used to extract the working directory base from.
     *
     * @param configuration to extract the working directory from
     * @param workingDirOption optional working dir option
     * @param workingDirectoryName name of the working directory to create
     * @return working directory
     */
    public static File generateWorkingDirectoryFile(
            Configuration configuration,
            Optional<ConfigOption<String>> workingDirOption,
            String workingDirectoryName) {
        final Optional<String> optionalWorkingDirectory =
                workingDirOption.flatMap(configuration::getOptional);

        final File workingDirectoryBase =
                optionalWorkingDirectory
                        .map(File::new)
                        .orElseGet(
                                () -> {
                                    final File tempDirectory =
                                            ConfigurationUtils.getRandomTempDirectory(
                                                    configuration);

                                    LOG.debug(
                                            "Picked {} randomly from the configured temporary directories to be used as working directory base.",
                                            tempDirectory);

                                    return tempDirectory;
                                });

        return new File(workingDirectoryBase, workingDirectoryName);
    }

    /**
     * Creates the working directory for the JobManager process. This method ensures that the
     * working diretory exists.
     *
     * @param configuration to extract the required settings from
     * @param envelopedResourceId identifying the TaskManager process
     * @return working directory
     * @throws IOException if the working directory could not be created
     */
    public static DeterminismEnvelope<WorkingDirectory> createJobManagerWorkingDirectory(
            Configuration configuration, DeterminismEnvelope<ResourceID> envelopedResourceId)
            throws IOException {
        return envelopedResourceId.map(
                resourceId ->
                        WorkingDirectory.create(
                                generateJobManagerWorkingDirectoryFile(configuration, resourceId)));
    }
}
