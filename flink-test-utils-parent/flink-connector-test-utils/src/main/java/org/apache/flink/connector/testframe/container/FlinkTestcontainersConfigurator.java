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
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Orchestrates configuration of Flink containers within Testcontainers framework. */
class FlinkTestcontainersConfigurator {

    private final TestcontainersSettings testcontainersSettings;
    private final FlinkContainersSettings flinkContainersSettings;

    FlinkTestcontainersConfigurator(
            FlinkContainersSettings flinkContainersSettings,
            TestcontainersSettings testcontainersSettings) {
        this.testcontainersSettings = testcontainersSettings;
        this.flinkContainersSettings = flinkContainersSettings;
    }

    private GenericContainer<?> configureJobManagerContainer(Path tempDirectory) {
        // Configure JobManager
        final Configuration jobManagerConf = new Configuration();
        jobManagerConf.addAll(flinkContainersSettings.getFlinkConfig());
        // Build JobManager container
        final ImageFromDockerfile jobManagerImage;
        try {
            jobManagerImage =
                    new FlinkImageBuilder()
                            .setTempDirectory(tempDirectory)
                            .setConfiguration(jobManagerConf)
                            .setLogProperties(flinkContainersSettings.getLogProperties())
                            .setBaseImage(flinkContainersSettings.getBaseImage())
                            .asJobManager()
                            .build();
        } catch (ImageBuildException e) {
            throw new RuntimeException("Failed to build JobManager image", e);
        }
        return configureContainer(
                        new GenericContainer<>(jobManagerImage),
                        flinkContainersSettings.getJobManagerHostname(),
                        "JobManager")
                .withExposedPorts(jobManagerConf.get(RestOptions.PORT));
    }

    private List<GenericContainer<?>> configureTaskManagerContainers(Path tempDirectory) {
        List<GenericContainer<?>> taskManagers = new ArrayList<>();
        for (int i = 0; i < flinkContainersSettings.getNumTaskManagers(); i++) {
            // Configure TaskManager
            final Configuration taskManagerConf = new Configuration();
            taskManagerConf.addAll(flinkContainersSettings.getFlinkConfig());
            final String taskManagerHostName =
                    flinkContainersSettings.getTaskManagerHostnamePrefix() + i;
            taskManagerConf.set(TaskManagerOptions.HOST, taskManagerHostName);
            // Build TaskManager container
            final ImageFromDockerfile taskManagerImage;
            try {
                taskManagerImage =
                        new FlinkImageBuilder()
                                .setTempDirectory(tempDirectory)
                                .setConfiguration(taskManagerConf)
                                .setLogProperties(flinkContainersSettings.getLogProperties())
                                .setBaseImage(flinkContainersSettings.getBaseImage())
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

    private GenericContainer<?> configureZookeeperContainer() {
        return configureContainer(
                new GenericContainer<>(DockerImageName.parse("zookeeper").withTag("3.7.1")),
                flinkContainersSettings.getZookeeperHostname(),
                "Zookeeper");
    }

    private GenericContainer<?> configureContainer(
            GenericContainer<?> container, String networkAlias, String logPrefix) {
        // Set dependent containers
        for (GenericContainer<?> dependentContainer : testcontainersSettings.getDependencies()) {
            dependentContainer.withNetwork(testcontainersSettings.getNetwork());
            container.dependsOn(dependentContainer);
        }
        // Setup network
        container.withNetwork(testcontainersSettings.getNetwork());
        container.withNetworkAliases(networkAlias);
        // Setup logger
        Logger logger = testcontainersSettings.getLogger();
        if (logger != null) {
            container.withLogConsumer(new Slf4jLogConsumer(logger).withPrefix(logPrefix));
        }
        // Add environment variables
        container.withEnv(testcontainersSettings.getEnvVars());
        container.withWorkingDirectory(flinkContainersSettings.getFlinkHome());
        return container;
    }

    /** Configures and creates {@link FlinkContainers}. */
    public FlinkContainers configure() {
        // Create temporary directory for building Flink image
        final Path imageBuildingTempDir;
        try {
            imageBuildingTempDir = Files.createTempDirectory("flink-image-build");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory", e);
        }

        // Build JobManager
        final GenericContainer<?> jobManager = configureJobManagerContainer(imageBuildingTempDir);

        // Build TaskManagers
        final List<GenericContainer<?>> taskManagers =
                configureTaskManagerContainers(imageBuildingTempDir);

        // Setup Zookeeper HA
        GenericContainer<?> zookeeper = null;
        // Mount HA storage to JobManager
        if (flinkContainersSettings.isZookeeperHA()) {
            zookeeper = configureZookeeperContainer();
            createTempDirAndMountToContainer(
                    "flink-recovery", flinkContainersSettings.getHaStoragePath(), jobManager);
        }

        // Mount checkpoint storage to JobManager
        createTempDirAndMountToContainer(
                "flink-checkpoint", flinkContainersSettings.getCheckpointPath(), jobManager);

        return new FlinkContainers(
                jobManager, taskManagers, zookeeper, flinkContainersSettings.getFlinkConfig());
    }

    void createTempDirAndMountToContainer(
            String tempDirPrefix, String containerPath, GenericContainer<?> container) {
        try {
            Path tempDirPath = Files.createTempDirectory(tempDirPrefix);
            File file = tempDirPath.toFile();
            file.setReadable(true, false);
            file.setWritable(true, false);
            file.setExecutable(true, false);
            container.withFileSystemBind(tempDirPath.toAbsolutePath().toString(), containerPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create temporary recovery directory", e);
        }
    }
}
