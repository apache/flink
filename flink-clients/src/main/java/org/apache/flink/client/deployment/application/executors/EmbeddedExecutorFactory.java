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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.EmbeddedJobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.time.Duration;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An {@link PipelineExecutorFactory} for the {@link EmbeddedExecutor}. */
@Internal
public class EmbeddedExecutorFactory implements PipelineExecutorFactory {

    private final Collection<JobID> applicationJobIds;

    private final Collection<JobID> suspendedJobIds;

    private final Collection<JobID> terminalJobIds;

    private final DispatcherGateway dispatcherGateway;

    private final ScheduledExecutor retryExecutor;

    /**
     * Creates an {@link EmbeddedExecutorFactory}.
     *
     * @param applicationJobIds a list that is going to be filled by the {@link EmbeddedExecutor}
     *     with job ids of all jobs that are part of the current application execution. This is
     *     essentially used to return the job ids to the caller.
     * @param suspendedJobIds ids of jobs that are suspended from a previous application execution,
     *     which are not supposed to be modified by the {@link EmbeddedExecutor}.
     * @param terminalJobIds ids of jobs that are already in a terminal state in a previous
     *     application execution, which are not supposed to be modified by the {@link
     *     EmbeddedExecutor}.
     * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit
     *     jobs.
     */
    public EmbeddedExecutorFactory(
            final Collection<JobID> applicationJobIds,
            final Collection<JobID> suspendedJobIds,
            final Collection<JobID> terminalJobIds,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor retryExecutor) {
        this.applicationJobIds = checkNotNull(applicationJobIds);
        this.suspendedJobIds = checkNotNull(suspendedJobIds);
        this.terminalJobIds = checkNotNull(terminalJobIds);
        this.dispatcherGateway = checkNotNull(dispatcherGateway);
        this.retryExecutor = checkNotNull(retryExecutor);
    }

    @Override
    public String getName() {
        return EmbeddedExecutor.NAME;
    }

    @Override
    public boolean isCompatibleWith(final Configuration configuration) {
        // this is always false because we simply have a special executor loader
        // for this one that does not check for compatibility.
        return false;
    }

    @Override
    public PipelineExecutor getExecutor(final Configuration configuration) {
        checkNotNull(configuration);
        return new EmbeddedExecutor(
                applicationJobIds,
                suspendedJobIds,
                terminalJobIds,
                dispatcherGateway,
                configuration,
                (jobId, userCodeClassloader) -> {
                    final Duration timeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
                    return new EmbeddedJobClient(
                            jobId, dispatcherGateway, retryExecutor, timeout, userCodeClassloader);
                });
    }
}
