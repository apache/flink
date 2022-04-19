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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder class for {@link FlinkContainers}. */
public class FlinkContainersBuilder {

    // Hostname of components within container network
    // Hostname of JobManager container
    public static final String JOB_MANAGER_HOSTNAME = "jobmanager";
    // Hostname prefix of TaskManagers. Use "taskmanager-0" for example to access TM with index 0
    public static final String TASK_MANAGER_HOSTNAME_PREFIX = "taskmanager-";
    // Hostname of Zookeeper container
    public static final String ZOOKEEPER_HOSTNAME = "zookeeper";

    // Directories for storing states
    // Path in the JM container for storing checkpoints and savepoints
    public static final Path CHECKPOINT_PATH = Paths.get("/flink/checkpoint");
    // Path in the JM container for persisting HA metadata
    public static final Path HA_STORAGE_PATH = Paths.get("/flink/recovery");

    private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();
    private final Configuration conf = new Configuration();
    private final Map<String, String> envVars = new HashMap<>();
    private final Properties logProperties = new Properties();

    private int numTaskManagers = 1;
    private Network network = Network.newNetwork();
    private Logger logger;
    private boolean enableZookeeperHA = false;

    /** Sets number of TaskManagers. */
    public FlinkContainersBuilder setNumTaskManagers(int numTaskManagers) {
        this.numTaskManagers = numTaskManagers;
        return this;
    }

    /** Sets a single configuration of the cluster. */
    public <T> FlinkContainersBuilder setConfiguration(ConfigOption<T> option, T value) {
        this.conf.set(option, value);
        return this;
    }

    /** Sets configurations of the cluster. */
    public FlinkContainersBuilder setConfiguration(Configuration conf) {
        this.conf.addAll(conf);
        return this;
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

    /**
     * Sets log4j property.
     *
     * <p>Containers will use "log4j-console.properties" under flink-dist as the base configuration
     * of loggers. Properties specified by this method will be appended to the config file, or
     * overwrite the property if already exists in the base config file.
     */
    public FlinkContainersBuilder setLogProperty(String key, String value) {
        this.logProperties.setProperty(key, value);
        return this;
    }

    /**
     * Sets log4j properties.
     *
     * <p>Containers will use "log4j-console.properties" under flink-dist as the base configuration
     * of loggers. Properties specified by this method will be appended to the config file, or
     * overwrite the property if already exists in the base config file.
     */
    public FlinkContainersBuilder setLogProperties(Properties logProperties) {
        this.logProperties.putAll(logProperties);
        return this;
    }

    /** Enables high availability service. */
    public FlinkContainersBuilder enableZookeeperHA() {
        this.enableZookeeperHA = true;
        return this;
    }

    /** Builds {@link FlinkContainers}. */
    public FlinkContainers build() {
        // Setup Zookeeper HA
        GenericContainer<?> zookeeper = null;
        if (enableZookeeperHA) {
            enableZookeeperHAConfigurations();
            zookeeper = buildZookeeperContainer();
        }

        // Add common configurations
        this.conf.set(JobManagerOptions.ADDRESS, JOB_MANAGER_HOSTNAME);
        this.conf.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                CHECKPOINT_PATH.toAbsolutePath().toUri().toString());
        this.conf.set(RestOptions.BIND_ADDRESS, "0.0.0.0");

        this.conf.set(JobManagerOptions.BIND_HOST, "0.0.0.0");
        this.conf.set(TaskManagerOptions.BIND_HOST, "0.0.0.0");
        this.conf.removeConfig(TaskManagerOptions.HOST);

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
        if (enableZookeeperHA) {
            createTempDirAndMountToContainer("flink-recovery", HA_STORAGE_PATH, jobManager);
        }

        // Mount checkpoint storage to JobManager
        createTempDirAndMountToContainer("flink-checkpoint", CHECKPOINT_PATH, jobManager);

        return new FlinkContainers(jobManager, taskManagers, zookeeper, conf);
    }

    // --------------------------- Helper Functions -------------------------------------

    private GenericContainer<?> buildJobManagerContainer(Path tempDirectory) {
        // Configure JobManager
        final Configuration jobManagerConf = new Configuration();
        jobManagerConf.addAll(this.conf);
        // Build JobManager container
        final ImageFromDockerfile jobManagerImage;
        try {
            jobManagerImage =
                    new FlinkImageBuilder()
                            .setTempDirectory(tempDirectory)
                            .setConfiguration(jobManagerConf)
                            .setLogProperties(logProperties)
                            .asJobManager()
                            .build();
        } catch (ImageBuildException e) {
            throw new RuntimeException("Failed to build JobManager image", e);
        }
        return configureContainer(
                        new GenericContainer<>(jobManagerImage), JOB_MANAGER_HOSTNAME, "JobManager")
                .withExposedPorts(jobManagerConf.get(RestOptions.PORT));
    }

    private List<GenericContainer<?>> buildTaskManagerContainers(Path tempDirectory) {
        List<GenericContainer<?>> taskManagers = new ArrayList<>();
        for (int i = 0; i < numTaskManagers; i++) {
            // Configure TaskManager
            final Configuration taskManagerConf = new Configuration();
            taskManagerConf.addAll(this.conf);
            final String taskManagerHostName = TASK_MANAGER_HOSTNAME_PREFIX + i;
            taskManagerConf.set(TaskManagerOptions.HOST, taskManagerHostName);
            // Build TaskManager container
            final ImageFromDockerfile taskManagerImage;
            try {
                taskManagerImage =
                        new FlinkImageBuilder()
                                .setTempDirectory(tempDirectory)
                                .setConfiguration(taskManagerConf)
                                .setLogProperties(logProperties)
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
                ZOOKEEPER_HOSTNAME,
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
        return container;
    }

    private void enableZookeeperHAConfigurations() {
        // Set HA related configurations
        checkNotNull(this.conf, "Configuration should not be null for setting HA service");
        conf.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        conf.set(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, ZOOKEEPER_HOSTNAME + ":" + "2181");
        conf.set(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, "/flink");
        conf.set(HighAvailabilityOptions.HA_CLUSTER_ID, "flink-container-" + UUID.randomUUID());
        conf.set(HighAvailabilityOptions.HA_STORAGE_PATH, HA_STORAGE_PATH.toUri().toString());
    }

    private void createTempDirAndMountToContainer(
            String tempDirPrefix, Path containerPath, GenericContainer<?> container) {
        try {
            Path tempDirPath = Files.createTempDirectory(tempDirPrefix);
            container.withFileSystemBind(
                    tempDirPath.toAbsolutePath().toString(),
                    containerPath.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create temporary recovery directory", e);
        }
    }
}
