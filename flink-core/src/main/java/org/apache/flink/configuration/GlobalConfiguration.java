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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Global configuration object for Flink. Similar to Java properties configuration objects it
 * includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

    public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

    // the keys whose values should be hidden
    private static final String[] SENSITIVE_KEYS =
            new String[] {"password", "secret", "fs.azure.account.key", "apikey"};

    // the hidden content to be displayed
    public static final String HIDDEN_CONTENT = "******";

    // key separator character
    private static final String KEY_SEPARATOR = ".";

    private static final Yaml yaml = new Yaml(new SafeConstructor());

    // --------------------------------------------------------------------------------------------

    private GlobalConfiguration() {}

    // --------------------------------------------------------------------------------------------

    /**
     * Loads the global configuration from the environment. Fails if an error occurs during loading.
     * Returns an empty configuration object if the environment variable is not set. In production
     * this variable is set but tests and local execution/debugging don't have this environment
     * variable set. That's why we should fail if it is not set.
     *
     * @return Returns the Configuration
     */
    public static Configuration loadConfiguration() {
        return loadConfiguration(new Configuration());
    }

    /**
     * Loads the global configuration and adds the given dynamic properties configuration.
     *
     * @param dynamicProperties The given dynamic properties
     * @return Returns the loaded global configuration with dynamic properties
     */
    public static Configuration loadConfiguration(Configuration dynamicProperties) {
        final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
        if (configDir == null) {
            return new Configuration(dynamicProperties);
        }

        return loadConfiguration(configDir, dynamicProperties);
    }

    /**
     * Loads the configuration files from the specified directory.
     *
     * <p>YAML files are supported as configuration files.
     *
     * @param configDir the directory which contains the configuration files
     */
    public static Configuration loadConfiguration(final String configDir) {
        return loadConfiguration(configDir, null);
    }

    /**
     * Loads the configuration files from the specified directory. If the dynamic properties
     * configuration is not null, then it is added to the loaded configuration.
     *
     * @param configDir directory to load the configuration from
     * @param dynamicProperties configuration file containing the dynamic properties. Null if none.
     * @return The configuration loaded from the given configuration directory
     */
    public static Configuration loadConfiguration(
            final String configDir, @Nullable final Configuration dynamicProperties) {

        if (configDir == null) {
            throw new IllegalArgumentException(
                    "Given configuration directory is null, cannot load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new IllegalConfigurationException(
                    "The given configuration directory name '"
                            + configDir
                            + "' ("
                            + confDirFile.getAbsolutePath()
                            + ") does not describe an existing directory.");
        }

        // get Flink yaml configuration file
        final File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);

        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException(
                    "The Flink config file '"
                            + yamlConfigFile
                            + "' ("
                            + yamlConfigFile.getAbsolutePath()
                            + ") does not exist.");
        }

        Configuration configuration = loadYAMLResource(yamlConfigFile);

        if (dynamicProperties != null) {
            configuration.addAll(dynamicProperties);
        }

        return configuration;
    }

    /**
     * Flattens a nested configuration map to be only one level deep.
     *
     * <p>Nested keys are concatinated using the {@code KEY_SEPARATOR} character. So that:
     *
     * <pre>
     * keyA:
     *   keyB:
     *     keyC: "hello"
     *     keyD: "world"
     * </pre>
     *
     * <p>becomes:
     *
     * <pre>
     * keyA.keyB.keyC: "hello"
     * keyA.keyB.keyD: "world"
     * </pre>
     *
     * @param config an arbitrarily nested config map
     * @param keyPrefix The string to prefix the keys in the current config level
     * @return A flattened, 1 level deep map
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> flatten(Map<String, Object> config, String keyPrefix) {
        final Map<String, String> flattenedMap = new HashMap<>();

        if (config == null) {
            return flattenedMap;
        }

        config.forEach(
                (key, value) -> {
                    String flattenedKey = keyPrefix + key;
                    if (value instanceof Map) {
                        Map<String, Object> e = (Map<String, Object>) value;
                        flattenedMap.putAll(flatten(e, flattenedKey + KEY_SEPARATOR));
                    } else {
                        flattenedMap.put(flattenedKey, value == null ? "" : value.toString());
                    }
                });

        return flattenedMap;
    }

    private static Map<String, String> flatten(Map<String, Object> config) {
        // Since we start flattening from the root, keys should not be prefixed with anything.
        return flatten(config, "");
    }

    /**
     * Loads a YAML-file of key-value pairs.
     *
     * <p>Keys can be expressed either as nested keys or as {@literal KEY_SEPARATOR} seperated keys.
     * For example, the following configurations are equivalent:
     *
     * <pre>
     * jobmanager.rpc.address: localhost # network address for communication with the job manager
     * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
     * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
     * </pre>
     *
     * <pre>
     * jobmanager:
     *     rpc:
     *         address: localhost # network address for communication with the job manager
     *         port: 6123         # network port to connect to for communication with the job manager
     * taskmanager:
     *     rpc:
     *         port: 6122         # network port the task manager expects incoming IPC connections
     * </pre>
     *
     * @param file the YAML file to read from
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
     */
    private static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        try (FileInputStream inputStream = new FileInputStream((file))) {
            Map<String, String> configDocument = flatten(yaml.load(inputStream));

            configDocument.forEach(
                    (key, value) -> {
                        LOG.info(
                                "Loading configuration property: {}, {}",
                                key,
                                isSensitive(key) ? HIDDEN_CONTENT : value);
                        config.setString(key, value);
                    });

            return config;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error finding YAML configuration file.", e);
        } catch (IOException | YAMLException | ClassCastException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }
    }

    /**
     * Check whether the key is a hidden key.
     *
     * @param key the config key
     */
    public static boolean isSensitive(String key) {
        Preconditions.checkNotNull(key, "key is null");
        final String keyInLower = key.toLowerCase();
        for (String hideKey : SENSITIVE_KEYS) {
            if (keyInLower.length() >= hideKey.length() && keyInLower.contains(hideKey)) {
                return true;
            }
        }
        return false;
    }
}
