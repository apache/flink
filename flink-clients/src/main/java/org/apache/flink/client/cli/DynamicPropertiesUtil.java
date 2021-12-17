/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.util.Properties;

/**
 * Helper class for supporting dynamic property commandline options in {@link CustomCommandLine
 * CustomCommandLines}.
 */
class DynamicPropertiesUtil {

    /**
     * Dynamic properties allow the user to specify additional configuration values with -D, such as
     * <tt> -Dfs.overwrite-files=true -Dtaskmanager.memory.network.min=536346624</tt>.
     */
    static final Option DYNAMIC_PROPERTIES =
            Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .desc(
                            "Allows specifying multiple generic configuration options. The available "
                                    + "options can be found at https://nightlies.apache.org/flink/flink-docs-stable/ops/config.html")
                    .build();

    /**
     * Parses dynamic properties from the given {@link CommandLine} and sets them on the {@link
     * Configuration}.
     */
    static void encodeDynamicProperties(
            final CommandLine commandLine, final Configuration effectiveConfiguration) {

        final Properties properties = commandLine.getOptionProperties(DYNAMIC_PROPERTIES.getOpt());

        properties
                .stringPropertyNames()
                .forEach(
                        key -> {
                            final String value = properties.getProperty(key);
                            if (value != null) {
                                effectiveConfiguration.setString(key, value);
                            } else {
                                effectiveConfiguration.setString(key, "true");
                            }
                        });
    }
}
