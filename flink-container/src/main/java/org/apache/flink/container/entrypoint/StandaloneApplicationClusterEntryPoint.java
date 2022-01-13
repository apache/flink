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

package org.apache.flink.container.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

/** An {@link ApplicationClusterEntryPoint} which is started with a job in a predefined location. */
@Internal
public final class StandaloneApplicationClusterEntryPoint extends ApplicationClusterEntryPoint {

    private StandaloneApplicationClusterEntryPoint(
            final Configuration configuration, final PackagedProgram program) {
        super(configuration, program, StandaloneResourceManagerFactory.getInstance());
    }

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG, StandaloneApplicationClusterEntryPoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new StandaloneApplicationClusterConfigurationParserFactory(),
                        StandaloneApplicationClusterEntryPoint.class);

        Configuration configuration = loadConfigurationFromClusterConfig(clusterConfiguration);
        PackagedProgram program = null;
        try {
            program = getPackagedProgram(clusterConfiguration, configuration);
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }

        try {
            configureExecution(configuration, program);
        } catch (Exception e) {
            LOG.error("Could not apply application configuration.", e);
            System.exit(1);
        }

        StandaloneApplicationClusterEntryPoint entrypoint =
                new StandaloneApplicationClusterEntryPoint(configuration, program);

        ClusterEntrypoint.runClusterEntrypoint(entrypoint);
    }

    @Override
    protected boolean supportsReactiveMode() {
        return true;
    }

    @VisibleForTesting
    static Configuration loadConfigurationFromClusterConfig(
            StandaloneApplicationClusterConfiguration clusterConfiguration) {
        Configuration configuration = loadConfiguration(clusterConfiguration);
        setStaticJobId(clusterConfiguration, configuration);
        SavepointRestoreSettings.toConfiguration(
                clusterConfiguration.getSavepointRestoreSettings(), configuration);
        return configuration;
    }

    private static PackagedProgram getPackagedProgram(
            final StandaloneApplicationClusterConfiguration clusterConfiguration,
            Configuration flinkConfiguration)
            throws FlinkException {
        final File userLibDir = ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null);
        PackagedProgramRetriever programRetriever =
                DefaultPackagedProgramRetriever.create(
                        userLibDir,
                        clusterConfiguration.getJobClassName(),
                        clusterConfiguration.getArgs(),
                        flinkConfiguration);

        // No need to do pipelineJars validation if it is a PyFlink job.
        if (!(PackagedProgramUtils.isPython(clusterConfiguration.getJobClassName())
                || PackagedProgramUtils.isPython(clusterConfiguration.getArgs()))
                && flinkConfiguration.getOptional(PipelineOptions.JARS).isPresent()) {
            final List<File> pipelineJars = checkJarFileForApplicationMode(flinkConfiguration);
            Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");
            programRetriever =  DefaultPackagedProgramRetriever.create(
                    userLibDir, pipelineJars.get(0), clusterConfiguration.getJobClassName(), clusterConfiguration.getArgs(), flinkConfiguration);
        }

        return programRetriever.getPackagedProgram();
    }

    private static void setStaticJobId(
            StandaloneApplicationClusterConfiguration clusterConfiguration,
            Configuration configuration) {
        final JobID jobId = clusterConfiguration.getJobId();
        if (jobId != null) {
            configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        }
    }

    @VisibleForTesting
    static List<File> checkJarFileForApplicationMode(Configuration configuration) {
        return configuration.get(PipelineOptions.JARS).stream()
                .map(
                        FunctionUtils.uncheckedFunction(
                                uri -> {
                                    final URI jarURI = PackagedProgramUtils.resolveURI(uri);
                                    if (jarURI.getScheme().equals("local") && jarURI.isAbsolute()) {
                                        return new File(jarURI.getPath());
                                    }
                                    throw new IllegalArgumentException(
                                            "Only \"file\" is supported as schema for the job jars."
                                                    + " An example of such path is: file:///opt/flink/examples/streaming/WindowJoin.jar");
                                }))
                .collect(Collectors.toList());
    }
}
