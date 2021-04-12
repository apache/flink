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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.EmbeddedJobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An {@link PipelineExecutorFactory} for the {@link EmbeddedExecutor}. */
@Internal
public class EmbeddedExecutorFactory implements PipelineExecutorFactory {

    private final Collection<JobID> submittedJobIds;

    private final DispatcherGateway dispatcherGateway;

    private final ScheduledExecutor retryExecutor;

    /**
     * Creates an {@link EmbeddedExecutorFactory}.
     *
     * @param submittedJobIds a list that is going to be filled with the job ids of the new jobs
     *     that will be submitted. This is essentially used to return the submitted job ids to the
     *     caller.
     * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit
     *     jobs.
     */
    public EmbeddedExecutorFactory(
            final Collection<JobID> submittedJobIds,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor retryExecutor) {
        this.submittedJobIds = checkNotNull(submittedJobIds);
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
                submittedJobIds,
                dispatcherGateway,
                (jobId, userCodeClassloader) -> {
                    final Time timeout =
                            Time.milliseconds(
                                    configuration.get(ClientOptions.CLIENT_TIMEOUT).toMillis());
                    return new EmbeddedJobClient(
                            jobId, dispatcherGateway, retryExecutor, timeout, userCodeClassloader);
                });
    }
}
