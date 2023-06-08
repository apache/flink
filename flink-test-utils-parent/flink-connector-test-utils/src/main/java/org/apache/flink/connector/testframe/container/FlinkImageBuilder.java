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

package org.apache.flink.connector.testframe.container;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.test.util.FileUtils;

import com.github.dockerjava.api.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A builder class for constructing Docker image based on flink-dist. */
public class FlinkImageBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkImageBuilder.class);
    private static final String FLINK_BASE_IMAGE_BUILD_NAME = "flink-base";
    private static final String DEFAULT_IMAGE_NAME_BUILD_PREFIX = "flink-configured";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);
    private static final String LOG4J_PROPERTIES_FILENAME = "log4j-console.properties";

    private final Map<Path, Path> filesToCopy = new HashMap<>();
    private final Properties logProperties = new Properties();

    private Path tempDirectory;
    private String imageNamePrefix = DEFAULT_IMAGE_NAME_BUILD_PREFIX;
    private String imageNameSuffix;
    private Path flinkDist;
    private String javaVersion;
    private Configuration conf;
    private Duration timeout = DEFAULT_TIMEOUT;
    private String startupCommand;
    private String baseImage;
    private String flinkHome = FlinkContainersSettings.getDefaultFlinkHome();

    /**
     * Sets temporary path for holding temp files when building the image.
     *
     * <p>Note that this parameter is required, because the builder doesn't have lifecycle
     * management, and it is the caller's responsibility to create and remove the temp directory.
     */
    public FlinkImageBuilder setTempDirectory(Path tempDirectory) {
        this.tempDirectory = tempDirectory;
        return this;
    }

    /**
     * Sets flink home.
     *
     * @param flinkHome The flink home.
     * @return The flink home.
     */
    public FlinkImageBuilder setFlinkHome(String flinkHome) {
        this.flinkHome = flinkHome;
        return this;
    }

    /**
     * Sets the prefix name of building image.
     *
     * <p>If the name is not specified, {@link #DEFAULT_IMAGE_NAME_BUILD_PREFIX} will be used.
     */
    public FlinkImageBuilder setImageNamePrefix(String imageNamePrefix) {
        this.imageNamePrefix = imageNamePrefix;
        return this;
    }

    /**
     * Sets the path of flink-dist directory.
     *
     * <p>If path is not specified, the dist directory under current project will be used.
     */
    public FlinkImageBuilder setFlinkDistPath(Path flinkDist) {
        this.flinkDist = flinkDist;
        return this;
    }

    /**
     * Sets JDK version in the image.
     *
     * <p>This version string will be used as the tag of openjdk image. If version is not specified,
     * the JDK version of the current JVM will be used.
     *
     * @see <a href="https://hub.docker.com/_/openjdk">OpenJDK on Docker Hub</a> for all available
     *     tags.
     */
    public FlinkImageBuilder setJavaVersion(String javaVersion) {
        this.javaVersion = javaVersion;
        return this;
    }

    /**
     * Sets Flink configuration. This configuration will be used for generating flink-conf.yaml for
     * configuring JobManager and TaskManager.
     */
    public FlinkImageBuilder setConfiguration(Configuration conf) {
        this.conf = conf;
        return this;
    }

    /**
     * Sets log4j properties.
     *
     * <p>Containers will use "log4j-console.properties" under flink-dist as the base configuration
     * of loggers. Properties specified by this method will be appended to the config file, or
     * overwrite the property if already exists in the base config file.
     */
    public FlinkImageBuilder setLogProperties(Properties logProperties) {
        this.logProperties.putAll(logProperties);
        return this;
    }

    /** Copies file into the image. */
    public FlinkImageBuilder copyFile(Path localPath, Path containerPath) {
        filesToCopy.put(localPath, containerPath);
        return this;
    }

    /** Sets timeout for building the image. */
    public FlinkImageBuilder setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    /** Use this image for building a JobManager. */
    public FlinkImageBuilder asJobManager() {
        checkStartupCommandNotSet();
        this.startupCommand = "bin/jobmanager.sh start-foreground && tail -f /dev/null";
        this.imageNameSuffix = "jobmanager";
        return this;
    }

    /** Use this image for building a TaskManager. */
    public FlinkImageBuilder asTaskManager() {
        checkStartupCommandNotSet();
        this.startupCommand = "bin/taskmanager.sh start-foreground && tail -f /dev/null";
        this.imageNameSuffix = "taskmanager";
        return this;
    }

    /** Use a custom command for starting up the container. */
    public FlinkImageBuilder useCustomStartupCommand(String command) {
        checkStartupCommandNotSet();
        this.startupCommand = command;
        this.imageNameSuffix = "custom";
        return this;
    }

    /**
     * Sets base image.
     *
     * @param baseImage The base image.
     * @return A reference to this Builder.
     */
    public FlinkImageBuilder setBaseImage(String baseImage) {
        this.baseImage = baseImage;
        return this;
    }

    /** Build the image. */
    public ImageFromDockerfile build() throws ImageBuildException {
        sanityCheck();
        final String finalImageName = imageNamePrefix + "-" + imageNameSuffix;
        try {
            if (baseImage == null) {
                baseImage = FLINK_BASE_IMAGE_BUILD_NAME;
                if (flinkDist == null) {
                    flinkDist = FileUtils.findFlinkDist();
                }
                // Build base image first
                buildBaseImage(flinkDist);
            }

            final Path flinkConfFile = createTemporaryFlinkConfFile(conf, tempDirectory);

            final Path log4jPropertiesFile = createTemporaryLog4jPropertiesFile(tempDirectory);
            // Copy flink-conf.yaml into image
            filesToCopy.put(
                    flinkConfFile,
                    Paths.get(flinkHome, "conf", GlobalConfiguration.FLINK_CONF_FILENAME));
            filesToCopy.put(
                    log4jPropertiesFile, Paths.get(flinkHome, "conf", LOG4J_PROPERTIES_FILENAME));

            final ImageFromDockerfile image =
                    new ImageFromDockerfile(finalImageName)
                            .withDockerfileFromBuilder(
                                    builder -> {
                                        // Build from base image
                                        builder.from(baseImage);
                                        // Copy files into image
                                        filesToCopy.forEach(
                                                (from, to) ->
                                                        builder.copy(to.toString(), to.toString()));
                                        builder.cmd(startupCommand);
                                    });
            filesToCopy.forEach((from, to) -> image.withFileFromPath(to.toString(), from));
            return image;
        } catch (Exception e) {
            throw new ImageBuildException(finalImageName, e);
        }
    }

    // ----------------------- Helper functions -----------------------

    private void buildBaseImage(Path flinkDist) throws TimeoutException {
        if (baseImageExists()) {
            return;
        }
        LOG.info("Building Flink base image with flink-dist at {}", flinkDist);
        new ImageFromDockerfile(FLINK_BASE_IMAGE_BUILD_NAME)
                .withDockerfileFromBuilder(
                        builder ->
                                builder.from(
                                                "eclipse-temurin:"
                                                        + getJavaVersionSuffix()
                                                        + "-jre-jammy")
                                        .copy(flinkHome, flinkHome)
                                        .build())
                .withFileFromPath(flinkHome, flinkDist)
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean baseImageExists() {
        try {
            DockerClientFactory.instance()
                    .client()
                    .inspectImageCmd(FLINK_BASE_IMAGE_BUILD_NAME)
                    .exec();
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    private String getJavaVersionSuffix() {
        if (this.javaVersion != null) {
            return this.javaVersion;
        } else {
            String javaSpecVersion = System.getProperty("java.vm.specification.version");
            LOG.info("Using JDK version {} of the current VM", javaSpecVersion);
            switch (javaSpecVersion) {
                case "1.8":
                    return "8";
                case "11":
                    return "11";
                case "17":
                    return "17";
                default:
                    throw new IllegalStateException("Unexpected Java version: " + javaSpecVersion);
            }
        }
    }

    private Path createTemporaryFlinkConfFile(Configuration finalConfiguration, Path tempDirectory)
            throws IOException {
        // Create a temporary flink-conf.yaml file and write merged configurations into it
        Path flinkConfFile = tempDirectory.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);
        Files.write(
                flinkConfFile,
                finalConfiguration.toMap().entrySet().stream()
                        .map(entry -> entry.getKey() + ": " + entry.getValue())
                        .collect(Collectors.toList()));

        return flinkConfFile;
    }

    private Path createTemporaryLog4jPropertiesFile(Path tempDirectory) throws IOException {
        // Create a temporary log4j.properties file and write merged properties into it
        Path log4jPropFile = tempDirectory.resolve(LOG4J_PROPERTIES_FILENAME);
        try (OutputStream output = new FileOutputStream(log4jPropFile.toFile())) {
            logProperties.store(output, null);
        }

        return log4jPropFile;
    }

    private void checkStartupCommandNotSet() {
        if (this.startupCommand != null) {
            throw new IllegalStateException(
                    "Cannot set startup command of container multiple times");
        }
    }

    private void sanityCheck() {
        checkNotNull(this.tempDirectory, "Temporary path is not specified");
        checkState(Files.isDirectory(this.tempDirectory));
        checkNotNull(
                this.startupCommand, "JobManager or TaskManager is not specified for the image");
    }
}
