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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.YamlConfigUtils;
import org.apache.flink.table.client.config.entries.DeploymentEntry;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.descriptors.FunctionDescriptorValidator;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Context describing default environment, command line options, flink config, etc.
 *
 * <p>When the {@link Executor} execute `reset` commands, the session can restore from the "default"
 * context.
 */
public class DefaultContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultContext.class);

    private final Environment defaultEnv;
    private final List<URL> dependencies;
    private final Configuration flinkConfig;

    public DefaultContext(
            Environment defaultEnv,
            List<URL> dependencies,
            Configuration flinkConfig,
            List<CustomCommandLine> commandLines) {
        this.defaultEnv = defaultEnv;
        this.dependencies = dependencies;
        this.flinkConfig = flinkConfig;
        Options commandLineOptions = collectCommandLineOptions(commandLines);

        // initialize default file system
        FileSystem.initialize(
                flinkConfig, PluginUtils.createPluginManagerFromRootFolder(flinkConfig));

        // add python dependencies
        if (containsPythonFunction(defaultEnv)) {
            addPythonDependency();
        }

        // put environment entry into Configuration
        // reset to flinkConfig because we have stored all the options into the flinkConfig
        defaultEnv.getConfiguration().asMap().forEach(flinkConfig::setString);
        flinkConfig.addAll(
                YamlConfigUtils.convertExecutionEntryToConfiguration(defaultEnv.getExecution()));
        try {
            CommandLine deploymentCommandLine =
                    createCommandLine(defaultEnv.getDeployment(), commandLineOptions);
            flinkConfig.addAll(
                    createExecutionConfig(
                            deploymentCommandLine, commandLineOptions, commandLines, dependencies));
        } catch (Exception e) {
            throw new SqlExecutionException(
                    "Could not load available CLI with Environment Deployment entry.", e);
        }
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public Environment getDefaultEnv() {
        return defaultEnv;
    }

    public List<URL> getDependencies() {
        return dependencies;
    }

    private Options collectCommandLineOptions(List<CustomCommandLine> commandLines) {
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
            throw new SqlExecutionException("Invalid deployment run options.", e);
        }

        LOG.info("Executor config: {}", executionConfig);
        return executionConfig;
    }

    private static CustomCommandLine findActiveCommandLine(
            List<CustomCommandLine> availableCommandLines, CommandLine commandLine) {
        for (CustomCommandLine cli : availableCommandLines) {
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new SqlExecutionException("Could not find a matching deployment.");
    }

    private static CommandLine createCommandLine(
            DeploymentEntry deployment, Options commandLineOptions) {
        try {
            return deployment.getCommandLine(commandLineOptions);
        } catch (Exception e) {
            throw new SqlExecutionException("Invalid deployment options.", e);
        }
    }

    private boolean containsPythonFunction(Environment environment) {
        return environment.getFunctions().values().stream()
                .anyMatch(
                        f ->
                                FunctionDescriptorValidator.FROM_VALUE_PYTHON.equals(
                                        f.getDescriptor()
                                                .toProperties()
                                                .get(FunctionDescriptorValidator.FROM)));
    }

    private void addPythonDependency() {
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
                this.dependencies.add(location);
            }
        } catch (URISyntaxException | ClassNotFoundException e) {
            throw new SqlExecutionException(
                    "Python UDF detected but flink-python jar not found. "
                            + "If you starts SQL-Client via `sql-client.sh`, please add the flink-python jar "
                            + "via `-j` command option manually.",
                    e);
        }
    }
}
