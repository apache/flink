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

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for migrating legacy Flink configuration file {@code flink-conf.yaml} to the new
 * format starting from Flink 2.0. This class provides methods to load legacy configuration files
 * and convert them into the new configuration format.
 */
public class ConfigurationFileMigrationUtils {

    private static final Logger LOG =
            LoggerFactory.getLogger(ConfigurationFileMigrationUtils.class);

    /**
     * This file is only used to help users migrate their legacy configuration files to the new
     * configuration file `config.yaml` starting from Flink 2.0.
     */
    @VisibleForTesting public static final String LEGACY_FLINK_CONF_FILENAME = "flink-conf.yaml";

    /**
     * Migrates the legacy Flink configuration from the specified directory to a standard YAML
     * format representation.
     *
     * <p>This method loads the legacy configuration file named {@code flink-conf.yaml} from the
     * specified directory. If the file is found, it converts the legacy format into a standard
     * {@link Configuration} object in YAML format.
     *
     * @param configDir the directory where the legacy configuration file is located
     * @return a {@link Configuration} object in standard YAML format
     */
    public static Configuration migrateLegacyToStandardYamlConfig(final String configDir) {
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
        Map<String, String> configuration;
        File yamlConfigFile = new File(confDirFile, LEGACY_FLINK_CONF_FILENAME);
        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException(
                    "The Flink config file '"
                            + yamlConfigFile
                            + "' ("
                            + yamlConfigFile.getAbsolutePath()
                            + ") does not exist.");
        } else {
            LOG.info(
                    "Using legacy YAML parser to load flink configuration file from {}.",
                    yamlConfigFile.getAbsolutePath());
            configuration = loadLegacyYAMLResource(yamlConfigFile);
        }

        Configuration standardYamlConfig = new Configuration();
        configuration.forEach(standardYamlConfig::setString);
        return standardYamlConfig;
    }

    /**
     * Loads a YAML-file of key-value pairs.
     *
     * <p>Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a
     * single-line comment.
     *
     * <p>Example:
     *
     * <pre>
     * jobmanager.rpc.address: localhost # network address for communication with the job manager
     * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
     * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
     * </pre>
     *
     * <p>This does not span the whole YAML specification, but only the *syntax* of simple YAML
     * key-value pairs (see issue #113 on GitHub). If at any point in time, there is a need to go
     * beyond simple key-value pairs syntax compatibility will allow to introduce a YAML parser
     * library.
     *
     * @param file the YAML file to read from
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
     */
    @VisibleForTesting
    public static Map<String, String> loadLegacyYAMLResource(File file) {
        Map<String, String> config = new HashMap<>();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": Line is not a key-value pair (missing space after ':'?)");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": Key or value was empty");
                        continue;
                    }

                    config.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }
}
