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
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A builder class for {@link FlinkContainers}. */
public class FlinkContainersBuilder {

    private final FlinkContainersConfig config;

    private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();
    private final Configuration flinkConfig;
    private final Map<String, String> envVars = new HashMap<>();

    private Network network = Network.newNetwork();
    private Logger logger;
    private String baseImage;

    public FlinkContainersBuilder(FlinkContainersConfig config) {
        this.config = config;
        this.flinkConfig = config.getFlinkConfiguration();
    }

    /**
     * Lets Flink cluster depending on another container, and bind the network of Flink cluster to
     * the dependent one.
     */
    public FlinkContainersBuilder dependsOn(GenericContainer<?> container) {
        container.withNetwork(this.network);
        this.dependentContainers.add(container);
        return this;
    }

    /** Sets environment variable to containers. */
    public FlinkContainersBuilder setEnvironmentVariable(String key, String value) {
        this.envVars.put(key, value);
        return this;
    }

    /** Sets network of the Flink cluster. */
    public FlinkContainersBuilder setNetwork(Network network) {
        this.network = network;
        return this;
    }

    /** Sets a logger to the cluster in order to consume STDOUT of containers to the logger. */
    public FlinkContainersBuilder setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public FlinkContainersBuilder setBaseImage(String baseImage) {
        this.baseImage = baseImage;
        return this;
    }

    /** Builds {@link FlinkContainers}. */
    public FlinkContainers build() {
        // Setup Zookeeper HA
        GenericContainer<?> zookeeper = null;
        if (config.isZookeeperHA()) {
            zookeeper = buildZookeeperContainer();
        }

        // Create temporary directory for building Flink image
        final Path imageBuildingTempDir;
        try {
            imageBuildingTempDir = Files.createTempDirectory("flink-image-build");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory", e);
        }

        // Build JobManager
        final GenericContainer<?> jobManager = buildJobManagerContainer(imageBuildingTempDir);

        // Build TaskManagers
        final List<GenericContainer<?>> taskManagers =
                buildTaskManagerContainers(imageBuildingTempDir);

        // Mount HA storage to JobManager
        if (config.isZookeeperHA()) {
            createTempDirAndMountToContainer(
                    "flink-recovery", config.getHaStoragePath(), jobManager);
        }

        // Mount checkpoint storage to JobManager
        createTempDirAndMountToContainer(
                "flink-checkpoint", config.getCheckpointPath(), jobManager);

        return new FlinkContainers(jobManager, taskManagers, zookeeper, flinkConfig);
    }

    // --------------------------- Helper Functions -------------------------------------

    private GenericContainer<?> buildJobManagerContainer(Path tempDirectory) {
        // Configure JobManager
        final Configuration jobManagerConf = new Configuration();
        jobManagerConf.addAll(this.flinkConfig);
        // Build JobManager container
        final ImageFromDockerfile jobManagerImage;
        try {
            jobManagerImage =
                    new FlinkImageBuilder()
                            .setTempDirectory(tempDirectory)
                            .setConfiguration(jobManagerConf)
                            .setLogProperties(config.getLogProperties())
                            .setBaseImage(baseImage)
                            .asJobManager()
                            .build();
        } catch (ImageBuildException e) {
            throw new RuntimeException("Failed to build JobManager image", e);
        }
        return configureContainer(
                        new GenericContainer<>(jobManagerImage),
                        config.getJobManagerHostname(),
                        "JobManager")
                .withExposedPorts(jobManagerConf.get(RestOptions.PORT));
    }

    private List<GenericContainer<?>> buildTaskManagerContainers(Path tempDirectory) {
        List<GenericContainer<?>> taskManagers = new ArrayList<>();
        for (int i = 0; i < config.getNumTaskManagers(); i++) {
            // Configure TaskManager
            final Configuration taskManagerConf = new Configuration();
            taskManagerConf.addAll(this.flinkConfig);
            final String taskManagerHostName = config.getTaskManagerHostnamePrefix() + i;
            taskManagerConf.set(TaskManagerOptions.HOST, taskManagerHostName);
            // Build TaskManager container
            final ImageFromDockerfile taskManagerImage;
            try {
                taskManagerImage =
                        new FlinkImageBuilder()
                                .setTempDirectory(tempDirectory)
                                .setConfiguration(taskManagerConf)
                                .setLogProperties(config.getLogProperties())
                                .setBaseImage(baseImage)
                                .asTaskManager()
                                .build();
            } catch (ImageBuildException e) {
                throw new RuntimeException("Failed to build TaskManager image", e);
            }
            taskManagers.add(
                    configureContainer(
                            new GenericContainer<>(taskManagerImage),
                            taskManagerHostName,
                            "TaskManager-" + i));
        }
        return taskManagers;
    }

    private GenericContainer<?> buildZookeeperContainer() {
        return configureContainer(
                new GenericContainer<>(DockerImageName.parse("zookeeper").withTag("3.5.9")),
                config.getZookeeperHostname(),
                "Zookeeper");
    }

    private GenericContainer<?> configureContainer(
            GenericContainer<?> container, String networkAlias, String logPrefix) {
        // Set dependent containers
        for (GenericContainer<?> dependentContainer : dependentContainers) {
            container.dependsOn(dependentContainer);
        }
        // Setup network
        container.withNetwork(network);
        container.withNetworkAliases(networkAlias);
        // Setup logger
        if (logger != null) {
            container.withLogConsumer(new Slf4jLogConsumer(logger).withPrefix(logPrefix));
        }
        // Add environment variables
        container.withEnv(envVars);
        container.withWorkingDirectory(config.getFlinkHome());
        return container;
    }

    private void createTempDirAndMountToContainer(
            String tempDirPrefix, String containerPath, GenericContainer<?> container) {
        try {
            Path tempDirPath = Files.createTempDirectory(tempDirPrefix);
            container.withFileSystemBind(tempDirPath.toAbsolutePath().toString(), containerPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create temporary recovery directory", e);
        }
    }
}
