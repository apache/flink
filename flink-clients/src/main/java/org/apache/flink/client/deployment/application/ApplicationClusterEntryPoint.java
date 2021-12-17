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

package org.apache.flink.client.deployment.application;

import org.apache.flink.client.cli.ProgramOptionsUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for cluster entry points targeting executing applications in "Application Mode". The
 * lifecycle of the entry point is bound to that of the specific application being executed, and the
 * {@code main()} method of the application is run on the cluster.
 */
public class ApplicationClusterEntryPoint extends ClusterEntrypoint {

    private final PackagedProgram program;

    private final ResourceManagerFactory<?> resourceManagerFactory;

    protected ApplicationClusterEntryPoint(
            final Configuration configuration,
            final PackagedProgram program,
            final ResourceManagerFactory<?> resourceManagerFactory) {
        super(configuration);
        this.program = checkNotNull(program);
        this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
    }

    @Override
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(final Configuration configuration) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                new DefaultDispatcherRunnerFactory(
                        ApplicationDispatcherLeaderProcessFactoryFactory.create(
                                configuration, SessionDispatcherFactory.INSTANCE, program)),
                resourceManagerFactory,
                JobRestEndpointFactory.INSTANCE);
    }

    @Override
    protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
            final Configuration configuration, final ScheduledExecutor scheduledExecutor) {
        return new MemoryExecutionGraphInfoStore();
    }

    protected static void configureExecution(
            final Configuration configuration, final PackagedProgram program) throws Exception {
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.JARS,
                program.getJobJarAndDependencies(),
                URL::toString);
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.CLASSPATHS,
                getClasspath(configuration, program),
                URL::toString);

        // If it is a PyFlink Application, we need to extract Python dependencies from the program
        // arguments, and
        // configure to execution configurations.
        if (PackagedProgramUtils.isPython(program.getMainClassName())) {
            ProgramOptionsUtils.configurePythonExecution(configuration, program);
        }
    }

    private static List<URL> getClasspath(
            final Configuration configuration, final PackagedProgram program)
            throws MalformedURLException {
        final List<URL> classpath =
                ConfigUtils.decodeListFromConfig(
                        configuration, PipelineOptions.CLASSPATHS, URL::new);
        classpath.addAll(program.getClasspaths());
        return Collections.unmodifiableList(
                classpath.stream().distinct().collect(Collectors.toList()));
    }

    @Override
    protected void cleanupDirectories() throws IOException {
        // Close the packaged program explicitly to clean up temporary jars.
        program.close();
        super.cleanupDirectories();
    }
}
