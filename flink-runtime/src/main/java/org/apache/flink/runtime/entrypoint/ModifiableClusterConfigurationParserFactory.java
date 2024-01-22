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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.ConfigurationCommandLineOptions.FLATTEN_CONFIG_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.ConfigurationCommandLineOptions.REMOVE_KEY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.ConfigurationCommandLineOptions.REMOVE_KEY_VALUE_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.ConfigurationCommandLineOptions.REPLACE_KEY_VALUE_OPTION;

/** A class can be used to extract the configuration from command line and modify it. */
public class ModifiableClusterConfigurationParserFactory
        implements ParserResultFactory<ModifiableClusterConfiguration> {

    public static Options options() {
        final Options options = new Options();
        options.addOption(CONFIG_DIR_OPTION);
        options.addOption(REMOVE_KEY_OPTION);
        options.addOption(REMOVE_KEY_VALUE_OPTION);
        options.addOption(REPLACE_KEY_VALUE_OPTION);
        options.addOption(DYNAMIC_PROPERTY_OPTION);
        options.addOption(FLATTEN_CONFIG_OPTION);

        return options;
    }

    @Override
    public Options getOptions() {
        return options();
    }

    @Override
    public ModifiableClusterConfiguration createResult(@Nonnull CommandLine commandLine) {
        String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());

        Properties dynamicProperties =
                commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());

        List<String> removeKeyList = new ArrayList<>();
        String[] removeKeys = commandLine.getOptionValues(REMOVE_KEY_OPTION.getOpt());
        if (removeKeys != null) {
            removeKeyList = Arrays.asList(removeKeys);
        }

        Properties removeKeyValues =
                commandLine.getOptionProperties(REMOVE_KEY_VALUE_OPTION.getOpt());

        List<Tuple3<String, String, String>> replaceKeyValueList = new ArrayList<>();
        String[] replaceKeyValues = commandLine.getOptionValues(REPLACE_KEY_VALUE_OPTION.getOpt());
        if (replaceKeyValues != null) {
            for (int i = 0; i < replaceKeyValues.length; i += 3) {
                replaceKeyValueList.add(
                        new Tuple3<>(
                                replaceKeyValues[i],
                                replaceKeyValues[i + 1],
                                replaceKeyValues[i + 2]));
            }
        }

        return new ModifiableClusterConfiguration(
                commandLine.hasOption(FLATTEN_CONFIG_OPTION),
                configDir,
                dynamicProperties,
                removeKeyValues,
                removeKeyList,
                replaceKeyValueList);
    }
}
