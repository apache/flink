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

package org.apache.flink.runtime.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.ModifiableClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ModifiableClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.util.MathUtils.checkedDownCast;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility class to extract related parameters from {@link Configuration} and to sanity check them.
 */
public class ConfigurationParserUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationParserUtils.class);

    /**
     * Parses the configuration to get the number of slots and validates the value.
     *
     * @param configuration configuration object
     * @return the number of slots in task manager
     */
    public static int getSlot(Configuration configuration) {
        int slots = configuration.get(TaskManagerOptions.NUM_TASK_SLOTS, 1);
        // we need this because many configs have been written with a "-1" entry
        if (slots == -1) {
            slots = 1;
        }

        ConfigurationParserUtils.checkConfigParameter(
                slots >= 1,
                slots,
                TaskManagerOptions.NUM_TASK_SLOTS.key(),
                "Number of task slots must be at least one.");

        return slots;
    }

    /**
     * Validates a condition for a config parameter and displays a standard exception, if the
     * condition does not hold.
     *
     * @param condition The condition that must hold. If the condition is false, an exception is
     *     thrown.
     * @param parameter The parameter value. Will be shown in the exception message.
     * @param name The name of the config parameter. Will be shown in the exception message.
     * @param errorMessage The optional custom error message to append to the exception message.
     * @throws IllegalConfigurationException if the condition does not hold
     */
    public static void checkConfigParameter(
            boolean condition, Object parameter, String name, String errorMessage)
            throws IllegalConfigurationException {
        if (!condition) {
            throw new IllegalConfigurationException(
                    "Invalid configuration value for "
                            + name
                            + " : "
                            + parameter
                            + " - "
                            + errorMessage);
        }
    }

    /**
     * Parses the configuration to get the page size and validates the value.
     *
     * @param configuration configuration object
     * @return size of memory segment
     */
    public static int getPageSize(Configuration configuration) {
        final int pageSize =
                checkedDownCast(
                        configuration.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes());

        // check page size of for minimum size
        checkConfigParameter(
                pageSize >= MemoryManager.MIN_PAGE_SIZE,
                pageSize,
                TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
                "Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
        // check page size for power of two
        checkConfigParameter(
                MathUtils.isPowerOf2(pageSize),
                pageSize,
                TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
                "Memory segment size must be a power of 2.");

        return pageSize;
    }

    /**
     * Generate configuration from only the config file and dynamic properties.
     *
     * @param args the commandline arguments
     * @param cmdLineSyntax the syntax for this application
     * @return generated configuration
     * @throws FlinkParseException if the configuration cannot be generated
     */
    public static Configuration loadCommonConfiguration(String[] args, String cmdLineSyntax)
            throws FlinkParseException {
        final CommandLineParser<ClusterConfiguration> commandLineParser =
                new CommandLineParser<>(new ClusterConfigurationParserFactory());

        final ClusterConfiguration clusterConfiguration;

        try {
            clusterConfiguration = commandLineParser.parse(args);
        } catch (FlinkParseException e) {
            LOG.error("Could not parse the command line options.", e);
            commandLineParser.printHelp(cmdLineSyntax);
            throw e;
        }

        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
        return GlobalConfiguration.loadConfiguration(
                clusterConfiguration.getConfigDir(), dynamicProperties);
    }

    public static List<String> loadAndModifyConfiguration(String[] args, String cmdLineSyntax)
            throws FlinkParseException {
        final CommandLineParser<ModifiableClusterConfiguration> commandLineParser =
                new CommandLineParser<>(new ModifiableClusterConfigurationParserFactory());

        final ModifiableClusterConfiguration modifiableClusterConfiguration;
        try {
            modifiableClusterConfiguration = commandLineParser.parse(args);
        } catch (FlinkParseException e) {
            LOG.error("Could not parse the command line options.", e);
            commandLineParser.printHelp(cmdLineSyntax);
            throw e;
        }

        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(
                        modifiableClusterConfiguration.getDynamicProperties());
        // 1. Load configuration and append dynamic properties to configuration.
        Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        modifiableClusterConfiguration.getConfigDir(), dynamicProperties);

        // 2. Replace the specified key's value with a new one if it matches the old value.
        List<Tuple3<String, String, String>> replaceKeyValues =
                modifiableClusterConfiguration.getReplaceKeyValues();
        replaceKeyValues.forEach(
                tuple3 -> {
                    String key = tuple3.f0;
                    String oldValue = tuple3.f1;
                    String newValue = tuple3.f2;
                    if (oldValue.equals(
                            configuration.get(
                                    ConfigOptions.key(key).stringType().noDefaultValue()))) {
                        configuration.setString(key, newValue);
                    }
                });

        // 3. Remove the specified key value pairs if the value matches.
        Properties removeKeyValues = modifiableClusterConfiguration.getRemoveKeyValues();
        final Set<String> propertyNames = removeKeyValues.stringPropertyNames();

        for (String propertyName : propertyNames) {
            if (removeKeyValues
                    .getProperty(propertyName)
                    .equals(
                            configuration.getString(
                                    ConfigOptions.key(propertyName)
                                            .stringType()
                                            .noDefaultValue()))) {
                configuration.removeKey(propertyName);
            }
        }

        // 4. Remove the specified key value pairs.
        List<String> removeKeys = modifiableClusterConfiguration.getRemoveKeys();
        removeKeys.forEach(configuration::removeKey);
        return ConfigurationUtils.convertConfigToWritableLines(
                configuration, modifiableClusterConfiguration.flattenConfig());
    }

    public static List<String> migrateLegacyConfigurationToStandardYaml(
            String[] args, String cmdLineSyntax) throws FlinkParseException {
        final CommandLineParser<ClusterConfiguration> commandLineParser =
                new CommandLineParser<>(new ClusterConfigurationParserFactory());

        final ClusterConfiguration clusterConfiguration;

        try {
            clusterConfiguration = commandLineParser.parse(args);
        } catch (FlinkParseException e) {
            LOG.error("Could not parse the command line options.", e);
            commandLineParser.printHelp(cmdLineSyntax);
            throw e;
        }

        checkState(
                new File(
                                clusterConfiguration.getConfigDir(),
                                GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME)
                        .exists());
        Configuration configuration =
                GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir(), null);

        Configuration standardYamlConfig = new Configuration(true);
        standardYamlConfig.addAll(configuration);

        return ConfigurationUtils.convertConfigToWritableLines(standardYamlConfig, false);
    }
}
