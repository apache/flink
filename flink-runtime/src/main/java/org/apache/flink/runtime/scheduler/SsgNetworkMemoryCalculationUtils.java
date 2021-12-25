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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.EdgeManagerBuildUtil;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Util to analyze inputs & outputs of {@link ExecutionJobVertex} and calculate network memory
 * requirement for slot sharing group (SSG).
 */
public class SsgNetworkMemoryCalculationUtils {

    /**
     * Calculates network memory requirement of {@link ExecutionJobVertex} and update {@link
     * ResourceProfile} of corresponding slot sharing group.
     */
    static void enrichNetworkMemory(
            SlotSharingGroup ssg,
            Function<JobVertexID, ExecutionJobVertex> ejvs,
            ShuffleMaster<?> shuffleMaster) {

        ResourceProfile original = ssg.getResourceProfile();

        // Updating network memory for UNKNOWN is also beneficial, but currently it's not
        // supported and the enriching logic only works for 'fine-grained resource management'.
        if (original.equals(ResourceProfile.UNKNOWN)
                || !original.getNetworkMemory().equals(MemorySize.ZERO)) {
            return;
        }

        MemorySize networkMemory = MemorySize.ZERO;
        for (JobVertexID jvId : ssg.getJobVertexIds()) {
            ExecutionJobVertex ejv = ejvs.apply(jvId);
            TaskInputsOutputsDescriptor desc = buildTaskInputsOutputsDescriptor(ejv, ejvs);
            MemorySize requiredNetworkMemory = shuffleMaster.computeShuffleMemorySizeForTask(desc);
            networkMemory = networkMemory.add(requiredNetworkMemory);
        }

        ResourceProfile enriched =
                ResourceProfile.newBuilder()
                        .setCpuCores(original.getCpuCores())
                        .setTaskHeapMemory(original.getTaskHeapMemory())
                        .setTaskOffHeapMemory(original.getTaskOffHeapMemory())
                        .setManagedMemory(original.getManagedMemory())
                        .setNetworkMemory(networkMemory)
                        .setExtendedResources(original.getExtendedResources().values())
                        .build();
        ssg.setResourceProfile(enriched);
    }

    private static TaskInputsOutputsDescriptor buildTaskInputsOutputsDescriptor(
            ExecutionJobVertex ejv, Function<JobVertexID, ExecutionJobVertex> ejvs) {

        Map<IntermediateDataSetID, Integer> maxInputChannelNums = getMaxInputChannelNums(ejv);
        Map<IntermediateDataSetID, Integer> maxSubpartitionNums = getMaxSubpartitionNums(ejv, ejvs);
        JobVertex jv = ejv.getJobVertex();
        Map<IntermediateDataSetID, ResultPartitionType> partitionTypes = getPartitionTypes(jv);

        return TaskInputsOutputsDescriptor.from(
                maxInputChannelNums, maxSubpartitionNums, partitionTypes);
    }

    private static Map<IntermediateDataSetID, Integer> getMaxInputChannelNums(
            ExecutionJobVertex ejv) {

        Map<IntermediateDataSetID, Integer> ret = new HashMap<>();
        List<JobEdge> inputEdges = ejv.getJobVertex().getInputs();

        for (int i = 0; i < inputEdges.size(); i++) {
            JobEdge inputEdge = inputEdges.get(i);
            IntermediateResult consumedResult = ejv.getInputs().get(i);

            // the inputs order should match in JobGraph and ExecutionGraph
            checkState(consumedResult.getId().equals(inputEdge.getSourceId()));

            int maxNum =
                    EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                            ejv.getParallelism(),
                            consumedResult.getNumberOfAssignedPartitions(),
                            inputEdge.getDistributionPattern());
            ret.put(consumedResult.getId(), maxNum);
        }

        return ret;
    }

    private static Map<IntermediateDataSetID, Integer> getMaxSubpartitionNums(
            ExecutionJobVertex ejv, Function<JobVertexID, ExecutionJobVertex> ejvs) {

        Map<IntermediateDataSetID, Integer> ret = new HashMap<>();
        List<IntermediateDataSet> producedDataSets = ejv.getJobVertex().getProducedDataSets();

        for (int i = 0; i < producedDataSets.size(); i++) {
            IntermediateDataSet producedDataSet = producedDataSets.get(i);

            checkState(
                    producedDataSet.getConsumers().size() == 1,
                    "Currently a result should have exactly one consumer job vertex.");

            JobEdge outputEdge = producedDataSet.getConsumers().get(0);
            ExecutionJobVertex consumerJobVertex = ejvs.apply(outputEdge.getTarget().getID());
            int maxNum =
                    EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                            ejv.getParallelism(),
                            consumerJobVertex.getParallelism(),
                            outputEdge.getDistributionPattern());
            ret.put(producedDataSet.getId(), maxNum);
        }

        return ret;
    }

    private static Map<IntermediateDataSetID, ResultPartitionType> getPartitionTypes(JobVertex jv) {
        Map<IntermediateDataSetID, ResultPartitionType> ret = new HashMap<>();
        jv.getProducedDataSets().forEach(ds -> ret.putIfAbsent(ds.getId(), ds.getResultType()));
        return ret;
    }

    /** Private default constructor to avoid being instantiated. */
    private SsgNetworkMemoryCalculationUtils() {}
}
