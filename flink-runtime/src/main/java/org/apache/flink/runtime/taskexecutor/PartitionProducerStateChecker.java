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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.concurrent.CompletableFuture;

/**
 * Intermediate partition state checker to query the JobManager about the state of the producer of a
 * result partition.
 *
 * <p>These checks are triggered when a partition request is answered with a PartitionNotFound
 * event. This usually happens when the producer of that partition has not registered itself with
 * the network stack or terminated.
 */
public interface PartitionProducerStateChecker {

    /**
     * Requests the execution state of the execution producing a result partition.
     *
     * @param jobId ID of the job the partition belongs to.
     * @param intermediateDataSetId ID of the parent intermediate data set.
     * @param resultPartitionId ID of the result partition to check. This identifies the producing
     *     execution and partition.
     * @return Future holding the execution state of the producing execution.
     */
    CompletableFuture<ExecutionState> requestPartitionProducerState(
            JobID jobId,
            IntermediateDataSetID intermediateDataSetId,
            ResultPartitionID resultPartitionId);
}
