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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.ExceptionUtils;

import java.util.Optional;

import static org.junit.Assert.fail;

/** Test utilities. */
public class TestUtils {

    /**
     * Execute the job and wait for the job result synchronously.
     *
     * @throws Exception If executing the environment throws an exception which does not have {@link
     *     SuccessException} as a cause.
     */
    public static void tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
        JobClient jobClient = null;
        try {
            StreamGraph graph = see.getStreamGraph(name);
            jobClient = see.executeAsync(graph);
            jobClient.getJobExecutionResult().get();
        } catch (Throwable root) {
            if (jobClient != null) {
                try {
                    jobClient.cancel().get();
                } catch (Exception e) {
                    // Exception could be thrown if the job has already finished.
                    // Ignore the exception.
                }
            }

            Optional<SuccessException> successAsCause =
                    ExceptionUtils.findThrowable(root, SuccessException.class);

            if (!successAsCause.isPresent()) {
                root.printStackTrace();
                fail("Test failed: " + root.getMessage());
            }
        }
    }

    public static void submitJobAndWaitForResult(
            ClusterClient<?> client, JobGraph jobGraph, ClassLoader classLoader) throws Exception {
        client.submitJob(jobGraph)
                .thenCompose(client::requestJobResult)
                .get()
                .toJobExecutionResult(classLoader);
    }

    public static void waitUntilJobInitializationFinished(
            JobID id, MiniClusterWithClientResource miniCluster, ClassLoader userCodeClassloader)
            throws JobInitializationException {
        ClusterClient<?> clusterClient = miniCluster.getClusterClient();
        ClientUtils.waitUntilJobInitializationFinished(
                () -> clusterClient.getJobStatus(id).get(),
                () -> clusterClient.requestJobResult(id).get(),
                userCodeClassloader);
    }
}
