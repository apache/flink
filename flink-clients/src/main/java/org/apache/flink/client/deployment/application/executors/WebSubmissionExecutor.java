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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.application.SingleJobApplication;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** The {@link PipelineExecutor} to be used when executing a job from web submission. */
public class WebSubmissionExecutor extends EmbeddedExecutor {

    public WebSubmissionExecutor(
            Collection<JobID> submittedJobIds,
            DispatcherGateway dispatcherGateway,
            Configuration configuration,
            EmbeddedJobClientCreator jobClientCreator) {
        super(submittedJobIds, dispatcherGateway, configuration, jobClientCreator);
    }

    @Override
    CompletableFuture<Acknowledge> internalSubmit(
            final DispatcherGateway dispatcherGateway,
            final StreamGraph streamGraph,
            final Duration rpcTimeout) {
        SingleJobApplication application = new SingleJobApplication(streamGraph);
        return dispatcherGateway.submitApplication(application, rpcTimeout);
    }
}
