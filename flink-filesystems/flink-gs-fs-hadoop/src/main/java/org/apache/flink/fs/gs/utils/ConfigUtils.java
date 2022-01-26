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

package org.apache.flink.fs.gs.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Optional;

/** Utilities class for configuration of Hadoop and Google Storage. */
public class ConfigUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    private static final String HADOOP_CONFIG_PREFIX = "fs.gs.";

    private static final String[] FLINK_CONFIG_PREFIXES = {"gs.", HADOOP_CONFIG_PREFIX};

    private static final String[][] MIRRORED_CONFIG_KEYS = {};

    private static final String FLINK_SHADING_PREFIX = "";

    /**
     * Loads the Hadoop configuration, by loading from a Hadoop conf dir (if one exists) and then
     * overlaying properties derived from the Flink config.
     *
     * @param flinkConfig The Flink config
     * @param configContext The config context.
     * @return The Hadoop config.
     */
    public static org.apache.hadoop.conf.Configuration getHadoopConfiguration(
            Configuration flinkConfig, ConfigContext configContext) {

        // create a starting hadoop configuration
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();

        // look for a hadoop configuration directory and load configuration from it if found
        Optional<String> hadoopConfigDir =
                Optional.ofNullable(flinkConfig.get(CoreOptions.FLINK_HADOOP_CONF_DIR));
        if (!hadoopConfigDir.isPresent()) {
            hadoopConfigDir = configContext.getenv("HADOOP_CONF_DIR");
        }
        hadoopConfigDir.ifPresent(
                configDir -> {
                    LOGGER.info("Loading Hadoop config resources from {}", configDir);
                    configContext.addHadoopResourcesFromDir(hadoopConfig, configDir);
                });

        // now, load hadoop config from flink and add to base hadoop config
        HadoopConfigLoader hadoopConfigLoader =
                new HadoopConfigLoader(
                        FLINK_CONFIG_PREFIXES,
                        MIRRORED_CONFIG_KEYS,
                        HADOOP_CONFIG_PREFIX,
                        Collections.emptySet(),
                        Collections.emptySet(),
                        FLINK_SHADING_PREFIX);
        hadoopConfigLoader.setFlinkConfig(flinkConfig);
        org.apache.hadoop.conf.Configuration flinkHadoopConfig =
                hadoopConfigLoader.getOrLoadHadoopConfig();
        hadoopConfig.addResource(flinkHadoopConfig);

        // reload the config resources and return it
        hadoopConfig.reloadConfiguration();
        return hadoopConfig;
    }

    /**
     * Creates a StorageOptions instance for the given Hadoop config and environment.
     *
     * @param hadoopConfig The Hadoop config.
     * @param configContext The config context.
     * @return The StorageOptions instance.
     */
    public static StorageOptions getStorageOptions(
            org.apache.hadoop.conf.Configuration hadoopConfig, ConfigContext configContext) {

        // follow the same rules as for the Hadoop connector, i.e.
        // 1) only use service credentials at all if Hadoop
        // "google.cloud.auth.service.account.enable" is true (default: true)
        // 2) use GOOGLE_APPLICATION_CREDENTIALS as location of credentials, if supplied
        // 3) use Hadoop "google.cloud.auth.service.account.json.keyfile" as location of
        // credentials, if supplied
        // 4) use no credentials

        // store any credentials we are to use, here
        Optional<String> credentialsPath = Optional.empty();

        // only look for credentials if service account support is enabled
        boolean enableServiceAccount =
                hadoopConfig.getBoolean("google.cloud.auth.service.account.enable", true);
        if (enableServiceAccount) {

            // load google application credentials, and then fall back to
            // "google.cloud.auth.service.account.json.keyfile" from Hadoop
            credentialsPath = configContext.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (credentialsPath.isPresent()) {
                LOGGER.info(
                        "GSRecoverableWriter is using GOOGLE_APPLICATION_CREDENTIALS at {}",
                        credentialsPath.get());
            } else {
                credentialsPath =
                        Optional.ofNullable(
                                hadoopConfig.get("google.cloud.auth.service.account.json.keyfile"));
                credentialsPath.ifPresent(
                        path ->
                                LOGGER.info(
                                        "GSRecoverableWriter is using credentials from Hadoop at {}",
                                        path));
            }
        }

        // construct the storage options
        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        if (credentialsPath.isPresent()) {
            LOGGER.info(
                    "Creating GSRecoverableWriter using credentials from {}",
                    credentialsPath.get());
            configContext.setStorageCredentialsFromFile(
                    storageOptionsBuilder, credentialsPath.get());
        } else {
            LOGGER.info("Creating GSRecoverableWriter using no credentials");
        }

        return storageOptionsBuilder.build();
    }

    /**
     * Helper to serialize a Hadoop config to a string, for logging.
     *
     * @param hadoopConfig The Hadoop config.
     * @return A string with the Hadoop properties.
     * @throws RuntimeException On underlying IO failure
     */
    public static String stringifyHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig)
            throws RuntimeException {
        try (Writer writer = new StringWriter()) {
            org.apache.hadoop.conf.Configuration.dumpConfiguration(hadoopConfig, writer);
            return writer.toString();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Interface that provides context-specific config helper functions, factored out to support
     * unit testing. *
     */
    public interface ConfigContext {
        /**
         * Returns a named environment variable.
         *
         * @param name Name of variable
         * @return Value of variable
         */
        Optional<String> getenv(String name);

        /**
         * Adds resources to the Hadoop configuration for the provided Hadoop config dir directory.
         *
         * @param config The Hadoop configuration.
         * @param configDir The Hadoop config directory.
         */
        void addHadoopResourcesFromDir(
                org.apache.hadoop.conf.Configuration config, String configDir);

        /**
         * Assigns credentials to the storage options builder from credentials at the given path.
         *
         * @param storageOptionsBuilder The storage options builder.
         * @param credentialsPath The path of the credentials file.
         */
        void setStorageCredentialsFromFile(
                StorageOptions.Builder storageOptionsBuilder, String credentialsPath);
    }
}
