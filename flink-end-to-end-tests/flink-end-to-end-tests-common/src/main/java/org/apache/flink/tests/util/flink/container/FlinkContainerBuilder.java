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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder class for {@link FlinkContainer}. */
public class FlinkContainerBuilder {

    private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();
    private final Configuration conf = new Configuration();
    private final Map<String, String> envVars = new HashMap<>();

    private int numTaskManagers = 1;
    private Network network = Network.newNetwork();
    private Logger logger;
    private boolean enableHAService = false;

    /** Sets number of TaskManagers. */
    public FlinkContainerBuilder numTaskManagers(int numTaskManagers) {
        this.numTaskManagers = numTaskManagers;
        return this;
    }

    /** Sets configuration of the cluster. */
    public FlinkContainerBuilder setConfiguration(Configuration conf) {
        this.conf.addAll(conf);
        return this;
    }

    /**
     * Lets Flink cluster depending on another container, and bind the network of Flink cluster to
     * the dependent one.
     */
    public FlinkContainerBuilder dependsOn(GenericContainer<?> container) {
        container.withNetwork(this.network);
        this.dependentContainers.add(container);
        return this;
    }

    /** Sets environment variable to containers. */
    public FlinkContainerBuilder setEnvironmentVariable(String key, String value) {
        this.envVars.put(key, value);
        return this;
    }

    /** Sets network of the Flink cluster. */
    public FlinkContainerBuilder setNetwork(Network network) {
        this.network = network;
        return this;
    }

    /** Sets a logger to the cluster in order to consume STDOUT of containers to the logger. */
    public FlinkContainerBuilder setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    /** Enables high availability service. */
    public FlinkContainerBuilder enableHAService() {
        this.enableHAService = true;
        return this;
    }

    /** Builds {@link FlinkContainer}. */
    public FlinkContainer build() {
        GenericContainer<?> haService = null;
        if (enableHAService) {
            enableHAServiceConfigurations();
            haService = buildZookeeperContainer();
        }

        this.conf.set(JobManagerOptions.ADDRESS, FlinkContainer.JOB_MANAGER_HOSTNAME);
        this.conf.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                FlinkContainer.CHECKPOINT_PATH.toAbsolutePath().toUri().toString());

        final GenericContainer<?> jobManager = buildJobManagerContainer();
        final List<GenericContainer<?>> taskManagers = buildTaskManagerContainers();

        if (enableHAService) {
            try {
                Path recoveryPath = Files.createTempDirectory("flink-recovery");
                jobManager.withFileSystemBind(
                        recoveryPath.toAbsolutePath().toString(),
                        FlinkContainer.HA_STORAGE_PATH.toAbsolutePath().toString());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create temporary recovery directory", e);
            }
        }

        try {
            Path checkpointPath = Files.createTempDirectory("flink-checkpoint");
            jobManager.withFileSystemBind(
                    checkpointPath.toAbsolutePath().toString(),
                    FlinkContainer.CHECKPOINT_PATH.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create temporary checkpoint directory", e);
        }

        return new FlinkContainer(jobManager, taskManagers, haService, conf);
    }

    // --------------------------- Helper Functions -------------------------------------

    private GenericContainer<?> buildJobManagerContainer() {
        // Configure JobManager
        final Configuration jobManagerConf = new Configuration();
        jobManagerConf.addAll(this.conf);
        // Build JobManager container
        final ImageFromDockerfile jobManagerImage;
        try {
            jobManagerImage =
                    new FlinkImageBuilder().setConfiguration(jobManagerConf).asJobManager().build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build JobManager image", e);
        }
        final GenericContainer<?> jobManager = buildContainer(jobManagerImage);
        // Setup network for JobManager
        jobManager
                .withNetworkAliases(FlinkContainer.JOB_MANAGER_HOSTNAME)
                .withExposedPorts(jobManagerConf.get(RestOptions.PORT));
        // Setup logger
        if (this.logger != null) {
            jobManager.withLogConsumer(new Slf4jLogConsumer(this.logger).withPrefix("JobManager"));
        }
        return jobManager;
    }

    private List<GenericContainer<?>> buildTaskManagerContainers() {
        List<GenericContainer<?>> taskManagers = new ArrayList<>();
        for (int i = 0; i < numTaskManagers; i++) {
            // Configure TaskManager
            final Configuration taskManagerConf = new Configuration();
            taskManagerConf.addAll(this.conf);
            final String taskManagerHostName = FlinkContainer.TASK_MANAGER_HOSTNAME_PREFIX + i;
            taskManagerConf.set(TaskManagerOptions.HOST, taskManagerHostName);
            // Build TaskManager container
            final ImageFromDockerfile taskManagerImage;
            try {
                taskManagerImage =
                        new FlinkImageBuilder()
                                .setConfiguration(taskManagerConf)
                                .asTaskManager()
                                .build();
            } catch (Exception e) {
                throw new RuntimeException("Failed to build TaskManager image", e);
            }
            final GenericContainer<?> taskManager = buildContainer(taskManagerImage);
            // Setup network for TaskManager
            taskManager.withNetworkAliases(taskManagerHostName);
            // Setup logger
            if (this.logger != null) {
                taskManager.withLogConsumer(
                        new Slf4jLogConsumer(this.logger).withPrefix("TaskManager-" + i));
            }
            taskManagers.add(taskManager);
        }
        return taskManagers;
    }

    private GenericContainer<?> buildZookeeperContainer() {
        final ImageFromDockerfile zookeeperImage;
        try {
            zookeeperImage =
                    new FlinkImageBuilder()
                            .setConfiguration(this.conf)
                            .useCustomStartupCommand(
                                    "flink/bin/zookeeper.sh start-foreground 1 && tail -f /dev/null")
                            .setImageName("flink-zookeeper")
                            .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build Zookeeper image", e);
        }
        final GenericContainer<?> zookeeper = buildContainer(zookeeperImage);
        // Setup network for Zookeeper
        zookeeper.withNetworkAliases(FlinkContainer.ZOOKEEPER_HOSTNAME);
        // Setup logger
        if (this.logger != null) {
            zookeeper.withLogConsumer(new Slf4jLogConsumer(this.logger).withPrefix("Zookeeper"));
        }
        return zookeeper;
    }

    private GenericContainer<?> buildContainer(ImageFromDockerfile image) {
        final GenericContainer<?> container = new GenericContainer<>(image);
        for (GenericContainer<?> dependentContainer : dependentContainers) {
            container.dependsOn(dependentContainer);
        }
        // Bind network to container
        container.withNetwork(this.network);
        // Add environment variables
        container.withEnv(envVars);
        return container;
    }

    private void enableHAServiceConfigurations() {
        // Set HA related configurations
        checkNotNull(this.conf, "Configuration should not be null for setting HA service");
        conf.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        conf.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                FlinkContainer.ZOOKEEPER_HOSTNAME + ":" + "2181");
        conf.set(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, "/flink");
        conf.set(HighAvailabilityOptions.HA_CLUSTER_ID, "flink-container-" + UUID.randomUUID());
        conf.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                FlinkContainer.HA_STORAGE_PATH.toUri().toString());
    }
}
