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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerUtils;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An {@link PipelineExecutor} for executing a {@link Pipeline} locally. */
@Internal
public class LocalExecutor implements PipelineExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);
    private final ExecutorService executorService =
            Executors.newFixedThreadPool(1, new ExecutorThreadFactory("Flink-LocalExecutor-IO"));

    public static final String NAME = "local";

    private final Configuration configuration;
    private final Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory;
    private final List<JobStatusChangedListener> jobStatusChangedListeners;

    public static LocalExecutor create(Configuration configuration) {
        return new LocalExecutor(configuration, MiniCluster::new);
    }

    public static LocalExecutor createWithFactory(
            Configuration configuration,
            Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
        return new LocalExecutor(configuration, miniClusterFactory);
    }

    private LocalExecutor(
            Configuration configuration,
            Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
        this.configuration = configuration;
        this.miniClusterFactory = miniClusterFactory;
        this.jobStatusChangedListeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        Thread.currentThread().getContextClassLoader(),
                        configuration,
                        executorService);
    }

    @Override
    public CompletableFuture<JobClient> execute(
            Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassloader)
            throws Exception {
        checkNotNull(pipeline);
        checkNotNull(configuration);

        Configuration effectiveConfig = new Configuration();
        effectiveConfig.addAll(this.configuration);
        effectiveConfig.addAll(configuration);

        // we only support attached execution with the local executor.
        checkState(configuration.get(DeploymentOptions.ATTACHED));

        final StreamGraph streamGraph =
                PipelineExecutorUtils.getStreamGraph(pipeline, configuration);

        streamGraph.serializeUserDefinedInstances();
        return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory)
                .submitJob(streamGraph, userCodeClassloader)
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable == null) {
                                PipelineExecutorUtils.notifyJobStatusListeners(
                                        pipeline, streamGraph, jobStatusChangedListeners);
                            } else {
                                LOG.error(
                                        "Failed to submit job graph to local mini cluster.",
                                        throwable);
                            }
                        });
    }
}
