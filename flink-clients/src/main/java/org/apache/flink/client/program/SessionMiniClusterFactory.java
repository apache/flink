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

package org.apache.flink.client.program;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.client.program.PerJobMiniClusterFactory.shutDownCluster;

/**
 * Starts a {@link MiniCluster} for all submitted job. This class only tear down the MiniCluster
 * when jvm shut down.
 */
public final class SessionMiniClusterFactory implements LocalMiniClusterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SessionMiniClusterFactory.class);

    public static final MiniCluster MINI_CLUSTER;

    static {
        MiniClusterConfiguration miniClusterConfig = getMiniClusterConfig();
        MINI_CLUSTER = new MiniCluster(miniClusterConfig);
        LOG.info("In SessionMiniClusterFactory, MINI_CLUSTER is : " + MINI_CLUSTER);
        try {
            MINI_CLUSTER.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    System.out.println(
                                            "JVM is shutting down. Running cleanup hook...");
                                    try {
                                        shutDownCluster(MINI_CLUSTER);
                                    } catch (Exception e) {
                                        System.err.println(
                                                "Error closing resource during shutdown: "
                                                        + e.getMessage());
                                    }
                                }));
    }

    /** Starts a {@link MiniCluster} and submits a job. */
    @Override
    public CompletableFuture<JobClient> submitJob(
            ExecutionPlan executionPlan, ClassLoader userCodeClassloader) throws Exception {
        return MINI_CLUSTER
                .submitJob(executionPlan)
                .thenApplyAsync(
                        FunctionUtils.uncheckedFunction(
                                submissionResult -> {
                                    org.apache.flink.client.ClientUtils
                                            .waitUntilJobInitializationFinished(
                                                    () ->
                                                            MINI_CLUSTER
                                                                    .getJobStatus(
                                                                            submissionResult
                                                                                    .getJobID())
                                                                    .get(),
                                                    () ->
                                                            MINI_CLUSTER
                                                                    .requestJobResult(
                                                                            submissionResult
                                                                                    .getJobID())
                                                                    .get(),
                                                    userCodeClassloader);
                                    return submissionResult;
                                }))
                .thenApply(
                        result ->
                                new MiniClusterJobClient(
                                        result.getJobID(),
                                        MINI_CLUSTER,
                                        userCodeClassloader,
                                        MiniClusterJobClient.JobFinalizationBehavior
                                                .NOTHING))
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                LOG.error(
                                        "Failed to create the JobClient."
                                                + " But we retain the mini cluster for subsequent jobs.");
                            }
                        })
                .thenApply(Function.identity());
    }

    private static MiniClusterConfiguration getMiniClusterConfig() {
        Configuration configuration = new Configuration(new Configuration());

        if (!configuration.contains(RestOptions.BIND_PORT)) {
            configuration.set(RestOptions.BIND_PORT, "0");
        }

        int numTaskManagers = configuration.get(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS);

        int numSlotsPerTaskManager =
                configuration.get(TaskManagerOptions.MINI_CLUSTER_NUMBER_SLOTS_PER_TASK_MANAGER);

        return new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumTaskManagers(numTaskManagers)
                .setRpcServiceSharing(RpcServiceSharing.SHARED)
                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                .build();
    }
}
