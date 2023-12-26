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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;
import static org.apache.flink.configuration.JobManagerOptions.SLOT_REQUEST_TIMEOUT;
import static org.apache.flink.configuration.MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL;

/** The central configuration holder for Flink container-based test environments. */
public class FlinkContainersSettings {
    private final String baseImage;
    private final int numTaskManagers;
    private final int numSlotsPerTaskManager;
    private final Collection<String> jarPaths;
    private final Configuration flinkConfig;
    private final String taskManagerHostnamePrefix;
    private final Boolean buildFromFlinkDist;
    private final String flinkDistLocation;
    private final String flinkHome;
    private final String checkpointPath;
    private final String haStoragePath;
    private final Boolean zookeeperHA;
    private final String zookeeperHostname;
    private final Properties logProperties;

    // Defaults
    private static final long DEFAULT_METRIC_FETCHER_UPDATE_INTERVAL_MS = 1000L;
    private static final long DEFAULT_SLOT_REQUEST_TIMEOUT_MS = 10_000L;
    private static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 5_000L;
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1000L;
    private static final MemorySize DEFAULT_TM_TOTAL_PROCESS_MEMORY = MemorySize.ofMebiBytes(1728);
    private static final MemorySize DEFAULT_JM_TOTAL_PROCESS_MEMORY = MemorySize.ofMebiBytes(1600);

    private static final int DEFAULT_NUM_TASK_MANAGERS = 1;
    private static final int DEFAULT_NUM_SLOTS_PER_TASK_MANAGER = 1;
    private static final String DEFAULT_TASK_MANAGERS_HOSTNAME_PREFIX = "taskmanager-";
    private static final String DEFAULT_JOB_MANAGER_HOSTNAME = "jobmanager";
    private static final String DEFAULT_BIND_ADDRESS = "0.0.0.0";
    private static final String DEFAULT_FLINK_HOME = "/opt/flink";
    private static final String DEFAULT_CHECKPOINT_PATH = DEFAULT_FLINK_HOME + "/checkpoint";
    private static final String DEFAULT_HA_STORAGE_PATH = DEFAULT_FLINK_HOME + "/recovery";

    private static final String DEFAULT_ZOOKEEPER_HOSTNAME = "zookeeper";
    private static final String DEFAULT_ZOOKEEPER_QUORUM = DEFAULT_ZOOKEEPER_HOSTNAME + ":2181";
    // --

    private FlinkContainersSettings(Builder builder) {
        baseImage = builder.baseImage;
        numTaskManagers = builder.numTaskManagers;
        numSlotsPerTaskManager = builder.numSlotsPerTaskManager;
        jarPaths = builder.jarPaths;
        flinkConfig = builder.flinkConfiguration;
        taskManagerHostnamePrefix = builder.taskManagerHostnamePrefix;
        buildFromFlinkDist = builder.buildFromFlinkDist;
        flinkDistLocation = builder.flinkDistLocation;
        flinkHome = builder.flinkHome;
        checkpointPath = builder.checkpointPath;
        haStoragePath = builder.haStoragePath;
        zookeeperHA = builder.zookeeperHA;
        zookeeperHostname = builder.zookeeperHostname;
        logProperties = builder.logProperties;
    }

    /**
     * A new builder for {@code FlinkContainersConfig}.
     *
     * @return The builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@code FlinkContainersConfig} based on defaults.
     *
     * @return The Flink containers config.
     */
    public static FlinkContainersSettings defaultConfig() {
        return builder().build();
    }

    /**
     * {@code FlinkContainersConfig} based on provided Flink configuration.
     *
     * @param config The config.
     * @return The flink containers config.
     */
    public static FlinkContainersSettings basedOn(Configuration config) {
        return builder().basedOn(config).build();
    }

    /** {@code FlinkContainersConfig} builder static inner class. */
    public static final class Builder {
        private String baseImage;
        private int numTaskManagers = DEFAULT_NUM_TASK_MANAGERS;
        private int numSlotsPerTaskManager = DEFAULT_NUM_SLOTS_PER_TASK_MANAGER;
        private Collection<String> jarPaths = new ArrayList<>();
        private Configuration flinkConfiguration = defaultFlinkConfig();
        private String taskManagerHostnamePrefix = DEFAULT_TASK_MANAGERS_HOSTNAME_PREFIX;
        private Boolean buildFromFlinkDist = true;
        private String flinkDistLocation;
        private String flinkHome = DEFAULT_FLINK_HOME;
        private String checkpointPath = DEFAULT_CHECKPOINT_PATH;
        private String haStoragePath = DEFAULT_HA_STORAGE_PATH;
        private Boolean zookeeperHA = false;
        private String zookeeperHostname = DEFAULT_ZOOKEEPER_HOSTNAME;
        private Properties logProperties = defaultLoggingProperties();

        private Builder() {}

        /**
         * Sets the {@code baseImage} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param baseImage The {@code baseImage} to set.
         * @return A reference to this Builder.
         */
        public Builder baseImage(String baseImage) {
            this.baseImage = baseImage;
            this.buildFromFlinkDist = false;
            return this;
        }

        /**
         * Sets the {@code flinkDistLocation} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param flinkDistLocation The {@code flinkDistLocation} to set.
         * @return A reference to this Builder.
         */
        public Builder flinkDistLocation(String flinkDistLocation) {
            this.flinkDistLocation = flinkDistLocation;
            this.buildFromFlinkDist = true;
            return this;
        }

        /**
         * Sets the path of the Flink distribution inside the container. Returns a reference to this
         * Builder enabling method chaining.
         *
         * @param flinkHome The {@code flinkHome} to set.
         * @return A reference to this Builder.
         */
        public Builder flinkHome(String flinkHome) {
            this.flinkHome = flinkHome;
            return this;
        }

        /**
         * Sets the {@code checkpointPath} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param checkpointPath The checkpoint path to set.
         * @return A reference to this Builder.
         */
        public Builder checkpointPath(String checkpointPath) {
            this.checkpointPath = checkpointPath;
            return setConfigOption(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, toUri(checkpointPath));
        }

        /**
         * Sets the {@code haStoragePath} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param haStoragePath The path for storing HA data.
         * @return A reference to this Builder.
         */
        public Builder haStoragePath(String haStoragePath) {
            this.haStoragePath = haStoragePath;
            return setConfigOption(HighAvailabilityOptions.HA_STORAGE_PATH, toUri(haStoragePath));
        }

        /**
         * Sets the {@code zookeeperHostname} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param zookeeperHostname The Zookeeper hostname.
         * @return A reference to this Builder.
         */
        public Builder zookeeperHostname(String zookeeperHostname) {
            this.zookeeperHostname = zookeeperHostname;
            return this;
        }

        /**
         * Enables Zookeeper HA. NOTE: this option uses default HA configuration. If you want to use
         * non-default configuration, you should provide all settings, including the HA_MODE
         * directly via the {@code basedOn()} method instead.
         *
         * @return A reference to this Builder.
         */
        public Builder enableZookeeperHA() {
            zookeeperHA = true;
            setConfigOption(HighAvailabilityOptions.HA_MODE, "zookeeper");
            setConfigOption(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, DEFAULT_ZOOKEEPER_QUORUM);
            setConfigOption(
                    HighAvailabilityOptions.HA_CLUSTER_ID, "flink-container-" + UUID.randomUUID());
            setConfigOption(
                    HighAvailabilityOptions.HA_STORAGE_PATH, toUri(DEFAULT_HA_STORAGE_PATH));
            return this;
        }

        /**
         * Sets the {@code numTaskManagers} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param numTaskManagers The {@code numTaskManagers} to set.
         * @return A reference to this Builder.
         */
        public Builder numTaskManagers(int numTaskManagers) {
            this.numTaskManagers = numTaskManagers;
            return this;
        }

        /**
         * Sets the {@code numSlotsPerTaskManager} and returns a reference to this Builder enabling
         * method chaining. It also adds this property into the {@code flinkConfiguration} field.
         *
         * @param numSlotsPerTaskManager The {@code numSlotsPerTaskManager} to set.
         * @return A reference to this Builder.
         */
        public Builder numSlotsPerTaskManager(int numSlotsPerTaskManager) {
            this.numSlotsPerTaskManager = numSlotsPerTaskManager;
            return setConfigOption(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTaskManager);
        }

        /**
         * Sets the {@code jarPaths} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param jarPaths The {@code jarPaths} to set.
         * @return A reference to this Builder.
         */
        public Builder jarPaths(String... jarPaths) {
            this.jarPaths = Arrays.asList(jarPaths);
            return this;
        }

        /**
         * Sets the {@code jarPaths} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param jarPaths The {@code jarPaths} to set.
         * @return A reference to this Builder.
         */
        public Builder jarPaths(Collection<String> jarPaths) {
            this.jarPaths = jarPaths;
            return this;
        }

        /**
         * Sets a single Flink configuration parameter (the options for flink-conf.yaml) and returns
         * a reference to this Builder enabling method chaining.
         *
         * @param <T> The type parameter.
         * @param option The option.
         * @param value The value.
         * @return A reference to this Builder.
         */
        public <T> Builder setConfigOption(ConfigOption<T> option, T value) {
            this.flinkConfiguration.set(option, value);
            return this;
        }

        /**
         * Sets a single Flink logging configuration property in the log4j format and returns a
         * reference to this Builder enabling method chaining.
         *
         * @param key The property key.
         * @param value The property value.
         * @return A reference to this Builder.
         */
        public Builder setLogProperty(String key, String value) {
            this.logProperties.setProperty(key, value);
            return this;
        }

        /**
         * Merges the provided {@code config} with the default config, potentially overwriting the
         * defaults in case of collisions. Returns a reference to this Builder enabling method
         * chaining.
         *
         * @param <T> the type parameter
         * @param config The {@code config} to add.
         * @return A reference to this Builder.
         */
        public <T> Builder basedOn(Configuration config) {
            this.flinkConfiguration.addAll(config);
            return this;
        }

        /**
         * Sets the {@code flinkConfiguration} value to {@code config} and returns a reference to
         * this Builder enabling method chaining.
         *
         * @param <T> the type parameter
         * @param config The {@code config} to set.
         * @return A reference to this Builder.
         */
        public <T> Builder fullConfiguration(Configuration config) {
            this.flinkConfiguration = config;
            return this;
        }

        /**
         * Sets the {@code taskManagerHostnamePrefix} and returns a reference to this Builder
         * enabling method chaining.
         *
         * @param taskManagerHostnamePrefix The {@code taskManagerHostnamePrefix} to set.
         * @return A reference to this Builder.
         */
        public Builder taskManagerHostnamePrefix(String taskManagerHostnamePrefix) {
            this.taskManagerHostnamePrefix = taskManagerHostnamePrefix;
            return this;
        }

        /**
         * Sets the job manager hostname and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param jobManagerHostname The job manager hostname to set.
         * @return A reference to this Builder.
         */
        public Builder jobManagerHostname(String jobManagerHostname) {
            return setConfigOption(JobManagerOptions.ADDRESS, jobManagerHostname);
        }

        private Configuration defaultFlinkConfig() {
            final Configuration config = new Configuration();
            config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, DEFAULT_TM_TOTAL_PROCESS_MEMORY);
            config.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, DEFAULT_JM_TOTAL_PROCESS_MEMORY);
            config.set(HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL_MS);
            config.set(HEARTBEAT_TIMEOUT, DEFAULT_HEARTBEAT_TIMEOUT_MS);
            config.set(SLOT_REQUEST_TIMEOUT, DEFAULT_SLOT_REQUEST_TIMEOUT_MS);
            config.set(METRIC_FETCHER_UPDATE_INTERVAL, DEFAULT_METRIC_FETCHER_UPDATE_INTERVAL_MS);
            config.set(JobManagerOptions.ADDRESS, DEFAULT_JOB_MANAGER_HOSTNAME);

            config.set(RestOptions.BIND_ADDRESS, DEFAULT_BIND_ADDRESS);
            config.set(TaskManagerOptions.BIND_HOST, DEFAULT_BIND_ADDRESS);
            config.set(JobManagerOptions.BIND_HOST, DEFAULT_BIND_ADDRESS);

            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, toUri(DEFAULT_CHECKPOINT_PATH));

            return config;
        }

        private Properties defaultLoggingProperties() {
            final Properties logProperties = new Properties();
            logProperties.setProperty(
                    "appender.rolling.strategy.max", "${env:MAX_LOG_FILE_NUMBER:-10}");
            logProperties.setProperty("appender.rolling.policies.size.size", "100MB");
            logProperties.setProperty("logger.zookeeper.name", "org.apache.zookeeper");
            logProperties.setProperty(
                    "logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3");
            logProperties.setProperty("logger.zookeeper.level", "INFO");
            logProperties.setProperty("appender.console.name", "ConsoleAppender");
            logProperties.setProperty("appender.rolling.fileName", "${sys:log.file}");
            logProperties.setProperty("appender.rolling.filePattern", "${sys:log.file}.%i");
            logProperties.setProperty("rootLogger.appenderRef.rolling.ref", "RollingFileAppender");
            logProperties.setProperty("logger.pekko.name", "org.apache.pekko");
            logProperties.setProperty("appender.console.type", "CONSOLE");
            logProperties.setProperty("appender.rolling.append", "true");
            logProperties.setProperty("appender.console.layout.type", "PatternLayout");
            logProperties.setProperty("appender.rolling.name", "RollingFileAppender");
            logProperties.setProperty("rootLogger.appenderRef.console.ref", "ConsoleAppender");
            logProperties.setProperty(
                    "appender.rolling.policies.size.type", "SizeBasedTriggeringPolicy");
            logProperties.setProperty(
                    "appender.console.layout.pattern",
                    "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
            logProperties.setProperty("rootLogger.level", "INFO");
            logProperties.setProperty("logger.hadoop.name", "org.apache.hadoop");
            logProperties.setProperty("logger.shaded_zookeeper.level", "INFO");
            logProperties.setProperty("appender.rolling.layout.type", "PatternLayout");
            logProperties.setProperty("logger.kafka.name", "org.apache.kafka");
            logProperties.setProperty("logger.netty.level", "OFF");
            logProperties.setProperty("appender.rolling.type", "RollingFile");
            logProperties.setProperty("logger.pekko.level", "INFO");
            logProperties.setProperty("logger.hadoop.level", "INFO");
            logProperties.setProperty(
                    "appender.rolling.layout.pattern",
                    "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
            logProperties.setProperty(
                    "appender.rolling.policies.startup.type", "OnStartupTriggeringPolicy");
            logProperties.setProperty(
                    "logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline");
            logProperties.setProperty("appender.rolling.strategy.type", "DefaultRolloverStrategy");
            logProperties.setProperty("appender.rolling.policies.type", "Policies");
            logProperties.setProperty("monitorInterval", "30");
            logProperties.setProperty("logger.kafka.level", "INFO");
            return logProperties;
        }

        /**
         * Returns a {@code FlinkContainersConfig} built from the parameters previously set.
         *
         * @return A {@code FlinkContainersConfig} built with parameters of this {@code
         *     FlinkContainersConfig.Builder}.
         */
        public FlinkContainersSettings build() {
            return new FlinkContainersSettings(this);
        }
    }

    /**
     * Gets base image.
     *
     * @return The base image.
     */
    public String getBaseImage() {
        return baseImage;
    }

    /**
     * Gets number of task managers.
     *
     * @return The number task managers.
     */
    public int getNumTaskManagers() {
        return numTaskManagers;
    }

    /**
     * Gets number slots per task manager.
     *
     * @return The number slots per task manager.
     */
    public int getNumSlotsPerTaskManager() {
        return numSlotsPerTaskManager;
    }

    /**
     * Gets jar paths.
     *
     * @return The jar paths.
     */
    public Collection<String> getJarPaths() {
        return jarPaths;
    }

    /**
     * Gets flink configuration.
     *
     * @return The flink configuration.
     */
    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    /**
     * Gets task manager hostname prefix.
     *
     * @return The task manager hostname prefix.
     */
    public String getTaskManagerHostnamePrefix() {
        return taskManagerHostnamePrefix;
    }

    /**
     * Gets job manager hostname.
     *
     * @return The job manager hostname.
     */
    public String getJobManagerHostname() {
        return flinkConfig.getString(JobManagerOptions.ADDRESS);
    }

    /**
     * Returns whether to build from flink-dist or from an existing base container. Also see the
     * {@code baseImage} property.
     */
    public Boolean isBuildFromFlinkDist() {
        return buildFromFlinkDist;
    }

    /**
     * Gets flink dist location.
     *
     * @return The flink dist location.
     */
    public String getFlinkDistLocation() {
        return flinkDistLocation;
    }

    /**
     * Gets flink home.
     *
     * @return The flink home path.
     */
    public String getFlinkHome() {
        return flinkHome;
    }

    /**
     * Gets checkpoint path.
     *
     * @return The checkpoint path.
     */
    public String getCheckpointPath() {
        return checkpointPath;
    }

    /**
     * Gets HA storage path.
     *
     * @return The ha storage path.
     */
    public String getHaStoragePath() {
        return haStoragePath;
    }

    /**
     * Gets ha storage path uri.
     *
     * @return The HA storage path as URI (prefixed with file://).
     */
    public String getHaStoragePathUri() {
        return Paths.get(getHaStoragePath()).toUri().toString();
    }

    /**
     * Gets default flink home.
     *
     * @return The default flink home path.
     */
    public static String getDefaultFlinkHome() {
        return DEFAULT_FLINK_HOME;
    }

    /**
     * Gets default checkpoint path.
     *
     * @return The default checkpoint path.
     */
    public static String getDefaultCheckpointPath() {
        return DEFAULT_CHECKPOINT_PATH;
    }

    /** Is zookeeper HA boolean. */
    public Boolean isZookeeperHA() {
        return zookeeperHA;
    }

    /**
     * Gets Zookeeper hostname.
     *
     * @return The Zookeeper hostname.
     */
    public String getZookeeperHostname() {
        return zookeeperHostname;
    }

    /**
     * Gets logging properties.
     *
     * @return The logging properties.
     */
    public Properties getLogProperties() {
        return logProperties;
    }

    private static String toUri(String path) {
        return Paths.get(path).toUri().toString();
    }
}
