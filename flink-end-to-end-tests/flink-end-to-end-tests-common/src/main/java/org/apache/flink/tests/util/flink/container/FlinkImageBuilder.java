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

package org.apache.flink.tests.util.flink.container;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.tests.util.util.FileUtils;

import com.github.dockerjava.api.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder class for constructing Docker image based on flink-dist. */
public class FlinkImageBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkImageBuilder.class);
    private static final String FLINK_BASE_IMAGE_NAME = "flink-dist-base";
    private static final String DEFAULT_IMAGE_NAME = "flink-dist-configured";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    private final Map<Path, Path> filesToCopy = new HashMap<>();

    private String imageName = DEFAULT_IMAGE_NAME;
    private String imageNameSuffix;
    private Path flinkDist = FileUtils.findFlinkDist();
    private String javaVersion;
    private Configuration conf;
    private Duration timeout = DEFAULT_TIMEOUT;
    private String startupCommand;

    /**
     * Sets the name of building image.
     *
     * <p>If the name is not specified, {@link #DEFAULT_IMAGE_NAME} will be used.
     */
    public FlinkImageBuilder setImageName(String imageName) {
        this.imageName = imageName;
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
     * <p>If version is not specified, the JDK version of the current JVM will be used.
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
        this.startupCommand = "flink/bin/jobmanager.sh start-foreground && tail -f /dev/null";
        this.imageNameSuffix = "jobmanager";
        return this;
    }

    /** Use this image for building a TaskManager. */
    public FlinkImageBuilder asTaskManager() {
        checkStartupCommandNotSet();
        this.startupCommand = "flink/bin/taskmanager.sh start-foreground && tail -f /dev/null";
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

    /** Build the image. */
    public ImageFromDockerfile build() throws TimeoutException, IOException {
        sanityCheck();
        // Build base image first
        buildBaseImage(flinkDist);
        final Path flinkConfFile = createTemporaryFlinkConfFile();
        // Copy flink-conf.yaml into image
        filesToCopy.put(
                flinkConfFile, Paths.get("flink", "conf", GlobalConfiguration.FLINK_CONF_FILENAME));

        final ImageFromDockerfile image =
                new ImageFromDockerfile(imageName + "-" + imageNameSuffix)
                        .withDockerfileFromBuilder(
                                builder -> {
                                    // Build from base image
                                    builder.from(FLINK_BASE_IMAGE_NAME);
                                    // Copy files into image
                                    filesToCopy.forEach(
                                            (from, to) ->
                                                    builder.copy(to.toString(), to.toString()));
                                    builder.cmd(startupCommand);
                                });
        filesToCopy.forEach((from, to) -> image.withFileFromPath(to.toString(), from));
        return image;
    }

    // ----------------------- Helper functions -----------------------

    private void buildBaseImage(Path flinkDist) throws TimeoutException {
        if (!baseImageExists()) {
            LOG.info("Building Flink base image with flink-dist at {}", flinkDist);
            new ImageFromDockerfile(FLINK_BASE_IMAGE_NAME)
                    .withDockerfileFromBuilder(
                            builder ->
                                    builder.from("openjdk:" + getJavaVersionSuffix())
                                            .copy("flink", "flink")
                                            .build())
                    .withFileFromPath("flink", flinkDist)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private boolean baseImageExists() {
        try {
            DockerClientFactory.instance().client().inspectImageCmd(FLINK_BASE_IMAGE_NAME).exec();
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
                default:
                    throw new IllegalStateException("Unexpected Java version: " + javaSpecVersion);
            }
        }
    }

    private Path createTemporaryFlinkConfFile() throws IOException {
        final Path tempDirectory =
                Files.createTempDirectory("flink-dist-build-" + UUID.randomUUID());

        // Load Flink configurations in flink-dist
        final Configuration finalConfiguration =
                GlobalConfiguration.loadConfiguration(
                        flinkDist.resolve("conf").toAbsolutePath().toString());

        if (this.conf != null) {
            finalConfiguration.addAll(this.conf);
        }

        Path flinkConfFile = tempDirectory.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);

        Files.write(
                flinkConfFile,
                finalConfiguration.toMap().entrySet().stream()
                        .map(entry -> entry.getKey() + ": " + entry.getValue())
                        .collect(Collectors.toList()));

        return flinkConfFile;
    }

    private void checkStartupCommandNotSet() {
        if (this.startupCommand != null) {
            throw new IllegalStateException("Cannot set startup command for multiple times");
        }
    }

    private void sanityCheck() {
        checkNotNull(
                this.startupCommand, "JobManager or TaskManager is not specified for the image");
    }
}
