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

package org.apache.flink.table.gateway.service.context;

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The context memorized initial configuration. */
public class DefaultContext {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultContext.class);

    private final Configuration flinkConfig;
    private final List<URL> dependencies;

    public DefaultContext(Map<String, String> flinkConfig, List<URL> dependencies) {
        this(Configuration.fromMap(flinkConfig), dependencies);
    }

    public DefaultContext(Configuration flinkConfig, List<URL> dependencies) {
        this.flinkConfig = flinkConfig;
        this.dependencies = dependencies;
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public List<URL> getDependencies() {
        return dependencies;
    }

    private static Options collectCommandLineOptions(List<CustomCommandLine> commandLines) {
        final Options customOptions = new Options();
        for (CustomCommandLine customCommandLine : commandLines) {
            customCommandLine.addGeneralOptions(customOptions);
            customCommandLine.addRunOptions(customOptions);
        }
        return CliFrontendParser.mergeOptions(
                CliFrontendParser.getRunCommandOptions(), customOptions);
    }

    private static Configuration createExecutionConfig(
            CommandLine commandLine,
            Options commandLineOptions,
            List<CustomCommandLine> availableCommandLines,
            List<URL> dependencies)
            throws FlinkException {
        LOG.debug("Available commandline options: {}", commandLineOptions);
        List<String> options =
                Stream.of(commandLine.getOptions())
                        .map(o -> o.getOpt() + "=" + o.getValue())
                        .collect(Collectors.toList());
        LOG.debug(
                "Instantiated commandline args: {}, options: {}",
                commandLine.getArgList(),
                options);

        final CustomCommandLine activeCommandLine =
                findActiveCommandLine(availableCommandLines, commandLine);
        LOG.debug(
                "Available commandlines: {}, active commandline: {}",
                availableCommandLines,
                activeCommandLine);

        Configuration executionConfig = activeCommandLine.toConfiguration(commandLine);

        try {
            final ProgramOptions programOptions = ProgramOptions.create(commandLine);
            final ExecutionConfigAccessor executionConfigAccessor =
                    ExecutionConfigAccessor.fromProgramOptions(programOptions, dependencies);
            executionConfigAccessor.applyToConfiguration(executionConfig);
        } catch (CliArgsException e) {
            throw new SqlGatewayException("Invalid deployment run options.", e);
        }

        LOG.info("Execution config: {}", executionConfig);
        return executionConfig;
    }

    private static CustomCommandLine findActiveCommandLine(
            List<CustomCommandLine> availableCommandLines, CommandLine commandLine) {
        for (CustomCommandLine cli : availableCommandLines) {
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new SqlGatewayException("Could not find a matching deployment.");
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Build the {@link DefaultContext} from flink-conf.yaml, dynamic configuration and users
     * specified jars.
     *
     * @param dynamicConfig user specified configuration.
     * @param dependencies user specified jars
     * @param discoverExecutionConfig flag whether to load the execution configuration
     * @param discoverPythonJar flag whetehr to load the python jar
     */
    public static DefaultContext load(
            Configuration dynamicConfig,
            List<URL> dependencies,
            boolean discoverExecutionConfig,
            boolean discoverPythonJar) {
        // 1. find the configuration directory
        String flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();

        // 2. load the global configuration
        Configuration configuration = GlobalConfiguration.loadConfiguration(flinkConfigDir);
        configuration.addAll(dynamicConfig);

        // 3. load the custom command lines
        List<CustomCommandLine> commandLines =
                CliFrontend.loadCustomCommandLines(configuration, flinkConfigDir);

        // initialize default file system
        FileSystem.initialize(
                configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        if (discoverPythonJar) {
            dependencies = new ArrayList<>(dependencies);
            dependencies.addAll(discoverPythonDependencies());
        }

        if (discoverExecutionConfig) {
            Options commandLineOptions = collectCommandLineOptions(commandLines);

            try {
                CommandLine deploymentCommandLine =
                        CliFrontendParser.parse(commandLineOptions, new String[] {}, true);
                configuration.addAll(
                        createExecutionConfig(
                                deploymentCommandLine,
                                commandLineOptions,
                                commandLines,
                                dependencies));
            } catch (Exception e) {
                throw new SqlGatewayException(
                        "Could not load available CLI with Environment Deployment entry.", e);
            }
        }

        return new DefaultContext(configuration, dependencies);
    }

    private static List<URL> discoverPythonDependencies() {
        try {
            URL location =
                    Class.forName(
                                    "org.apache.flink.python.PythonFunctionRunner",
                                    false,
                                    Thread.currentThread().getContextClassLoader())
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation();
            if (Paths.get(location.toURI()).toFile().isFile()) {
                return Collections.singletonList(location);
            }
        } catch (URISyntaxException | ClassNotFoundException e) {
            LOG.warn("Failed to find flink-python jar." + e);
        }
        return Collections.emptyList();
    }
}
