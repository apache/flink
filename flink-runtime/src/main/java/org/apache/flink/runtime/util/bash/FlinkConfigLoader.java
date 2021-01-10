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

package org.apache.flink.runtime.util.bash;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.Options;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Util class for loading configuration from commandline arguments. It parses only the configuration
 * file and dynamic properties, ignores other commandline options.
 */
public class FlinkConfigLoader {

    private static final Options CMD_OPTIONS = ClusterConfigurationParserFactory.options();

    public static Configuration loadConfiguration(String[] args) throws FlinkException {
        return ConfigurationParserUtils.loadCommonConfiguration(
                filterCmdArgs(args), BashJavaUtils.class.getSimpleName());
    }

    private static String[] filterCmdArgs(String[] args) {
        final List<String> filteredArgs = new ArrayList<>();
        final Iterator<String> iter = Arrays.asList(args).iterator();

        while (iter.hasNext()) {
            String token = iter.next();
            if (CMD_OPTIONS.hasOption(token)) {
                filteredArgs.add(token);
                if (CMD_OPTIONS.getOption(token).hasArg() && iter.hasNext()) {
                    filteredArgs.add(iter.next());
                }
            } else if (token.startsWith("-D")) {
                // "-Dkey=value"
                filteredArgs.add(token);
            }
        }

        return filteredArgs.toArray(new String[0]);
    }

    private FlinkConfigLoader() {}
}
