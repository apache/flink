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

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

import java.util.function.Function;

/** Interface for retrieving stream graph context details for adaptive batch jobs. */
public interface ExecutionPlanSchedulingContext {

    /**
     * Retrieves the parallelism of consumers connected to the specified intermediate data set.
     *
     * @param executionJobVertexParallelismRetriever A function that retrieves the parallelism of a
     *     job vertex.
     * @param intermediateDataSet The intermediate data set whose consumer parallelism is queried.
     * @return The parallelism of the consumers.
     */
    int getConsumersParallelism(
            Function<JobVertexID, Integer> executionJobVertexParallelismRetriever,
            IntermediateDataSet intermediateDataSet);

    /**
     * Retrieves the maximum parallelism of consumers connected to the specified intermediate data
     * set.
     *
     * @param executionJobVertexMaxParallelismRetriever A function that retrieves the maximum
     *     parallelism of a job vertex.
     * @param intermediateDataSet The intermediate data set whose consumer maximum parallelism is
     *     queried.
     * @return The maximum parallelism of the consumers.
     */
    int getConsumersMaxParallelism(
            Function<JobVertexID, Integer> executionJobVertexMaxParallelismRetriever,
            IntermediateDataSet intermediateDataSet);

    /**
     * Retrieves the count of pending operators waiting to be transferred to job vertices.
     *
     * @return the number of pending operators.
     */
    int getPendingOperatorCount();

    /**
     * Retrieves the JSON representation of the stream graph for the original job.
     *
     * @return the JSON representation of the stream graph, or null if the stream graph is not
     *     available.
     */
    @Nullable
    String getStreamGraphJson();
}
