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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.deployment.application.executors.WebSubmissionExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ApplicationRunner} which runs the user specified application using the {@link
 * EmbeddedExecutor}. This runner invokes methods of the provided {@link DispatcherGateway}
 * directly, and it does not go through the REST API.
 *
 * <p>In addition, this runner does not wait for the application to finish, but it submits the
 * application in a {@code DETACHED} mode. As a consequence, applications with jobs that rely on
 * operations like {@code [collect, print, printToErr, count]} will fail.
 */
@Internal
public class DetachedApplicationRunner implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DetachedApplicationRunner.class);

    private final boolean enforceSingleJobExecution;

    public DetachedApplicationRunner(final boolean enforceSingleJobExecution) {
        this.enforceSingleJobExecution = enforceSingleJobExecution;
    }

    @Override
    public List<JobID> run(
            final DispatcherGateway dispatcherGateway,
            final PackagedProgram program,
            final Configuration configuration) {
        checkNotNull(dispatcherGateway);
        checkNotNull(program);
        checkNotNull(configuration);
        return tryExecuteJobs(dispatcherGateway, program, configuration);
    }

    private List<JobID> tryExecuteJobs(
            final DispatcherGateway dispatcherGateway,
            final PackagedProgram program,
            final Configuration configuration) {
        configuration.set(DeploymentOptions.ATTACHED, false);

        final List<JobID> applicationJobIds = new ArrayList<>();
        final PipelineExecutorServiceLoader executorServiceLoader =
                new WebSubmissionExecutorServiceLoader(applicationJobIds, dispatcherGateway);

        try {
            ClientUtils.executeProgram(
                    executorServiceLoader, configuration, program, enforceSingleJobExecution, true);
        } catch (ProgramInvocationException e) {
            LOG.warn("Could not execute application: ", e);
            throw new FlinkRuntimeException("Could not execute application.", e);
        }

        return applicationJobIds;
    }
}
