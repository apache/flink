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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A factory class for creating instances of {@link AdaptiveExecutionHandler}. This factory provides
 * a method to create an appropriate handler based on the type of the execution plan.
 */
public class AdaptiveExecutionHandlerFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveExecutionHandlerFactory.class);

    /**
     * Creates an instance of {@link AdaptiveExecutionHandler} based on the provided execution plan.
     *
     * <p>TODO: Currently, adaptive execution cannot work with batch job progress recovery, so we
     * always use {@link NonAdaptiveExecutionHandler} if batch job recovery is enabled. This
     * limitation will be removed in the future when we adapt adaptive batch execution to batch job
     * recovery.
     *
     * <p>{@link PipelineOptions#PARALLELISM_OVERRIDES} is not applied to a {@link StreamGraph} that
     * is generated incrementally: the generator sizes the inputs of a job vertex before the vertex,
     * and with it the job vertex id the override is keyed by, exists.
     *
     * @param executionPlan The execution plan, which can be either a {@link JobGraph} or a {@link
     *     StreamGraph}.
     * @param enableBatchJobRecovery Whether to enable batch job recovery.
     * @param userClassLoader The class loader for the user code.
     * @param serializationExecutor The executor used for serialization tasks.
     * @return An instance of {@link AdaptiveExecutionHandler}.
     * @throws IllegalArgumentException if the execution plan is neither a {@link JobGraph} nor a
     *     {@link StreamGraph}.
     */
    public static AdaptiveExecutionHandler create(
            ExecutionPlan executionPlan,
            boolean enableBatchJobRecovery,
            ClassLoader userClassLoader,
            Executor serializationExecutor)
            throws DynamicCodeLoadingException {
        if (executionPlan instanceof JobGraph) {
            return new NonAdaptiveExecutionHandler((JobGraph) executionPlan);
        }
        checkState(executionPlan instanceof StreamGraph, "Unsupported execution plan.");
        final StreamGraph streamGraph = (StreamGraph) executionPlan;
        if (enableBatchJobRecovery) {
            return new NonAdaptiveExecutionHandler(streamGraph.getJobGraph(userClassLoader));
        }
        if (!streamGraph
                .getJobConfiguration()
                .get(PipelineOptions.PARALLELISM_OVERRIDES)
                .isEmpty()) {
            LOG.warn(
                    "Ignoring {} for job {}: the adaptive batch scheduler does not apply it to a"
                            + " StreamGraph that it generates incrementally.",
                    PipelineOptions.PARALLELISM_OVERRIDES.key(),
                    streamGraph.getJobID());
        }
        return new DefaultAdaptiveExecutionHandler(
                userClassLoader, streamGraph, serializationExecutor);
    }
}
