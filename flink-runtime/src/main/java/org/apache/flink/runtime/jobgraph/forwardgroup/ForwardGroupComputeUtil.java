/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Common utils for computing forward groups. */
public class ForwardGroupComputeUtil {

    public static Map<JobVertexID, JobVertexForwardGroup> computeForwardGroupsAndCheckParallelism(
            final Iterable<JobVertex> topologicallySortedVertices) {
        final Map<JobVertexID, JobVertexForwardGroup> forwardGroupsByJobVertexId =
                computeForwardGroups(
                        topologicallySortedVertices, ForwardGroupComputeUtil::getForwardProducers);
        // the vertex's parallelism in parallelism-decided forward group should have been set at
        // compilation phase
        topologicallySortedVertices.forEach(
                jobVertex -> {
                    JobVertexForwardGroup forwardGroup =
                            forwardGroupsByJobVertexId.get(jobVertex.getID());
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        checkState(jobVertex.getParallelism() == forwardGroup.getParallelism());
                    }
                });
        return forwardGroupsByJobVertexId;
    }

    public static Map<JobVertexID, JobVertexForwardGroup> computeForwardGroups(
            final Iterable<JobVertex> topologicallySortedVertices,
            final Function<JobVertex, Set<JobVertex>> forwardProducersRetriever) {

        final Map<JobVertex, Set<JobVertex>> vertexToGroup = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (JobVertex vertex : topologicallySortedVertices) {
            Set<JobVertex> currentGroup = new HashSet<>();
            currentGroup.add(vertex);
            vertexToGroup.put(vertex, currentGroup);

            for (JobVertex producerVertex : forwardProducersRetriever.apply(vertex)) {
                final Set<JobVertex> producerGroup = vertexToGroup.get(producerVertex);

                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getID()
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + vertex.getID()
                                    + ". This should be a forward group building bug.");
                }

                if (currentGroup != producerGroup) {
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, vertexToGroup);
                }
            }
        }

        final Map<JobVertexID, JobVertexForwardGroup> ret = new HashMap<>();
        for (Set<JobVertex> vertexGroup :
                VertexGroupComputeUtil.uniqueVertexGroups(vertexToGroup)) {
            if (vertexGroup.size() > 1) {
                JobVertexForwardGroup forwardGroup = new JobVertexForwardGroup(vertexGroup);
                for (JobVertexID jobVertexId : forwardGroup.getJobVertexIds()) {
                    ret.put(jobVertexId, forwardGroup);
                }
            }
        }
        return ret;
    }

    @VisibleForTesting
    public static Map<Integer, StreamNodeForwardGroup>
            computeStreamNodeForwardGroupAndCheckParallelism(
                    final Map<StreamNode, List<StreamNode>>
                            topologicallySortedChainedStreamNodesMap,
                    final Function<StreamNode, Set<StreamNode>> forwardProducersRetriever) {
        final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeId =
                computeStreamNodeForwardGroup(
                        topologicallySortedChainedStreamNodesMap, forwardProducersRetriever);
        topologicallySortedChainedStreamNodesMap
                .keySet()
                .forEach(
                        startNode -> {
                            StreamNodeForwardGroup forwardGroup =
                                    forwardGroupsByStartNodeId.get(startNode.getId());
                            if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                                checkState(
                                        startNode.getParallelism()
                                                == forwardGroup.getParallelism());
                            }
                        });
        return forwardGroupsByStartNodeId;
    }

    public static Map<Integer, StreamNodeForwardGroup> computeStreamNodeForwardGroup(
            final Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap,
            final Function<StreamNode, Set<StreamNode>> forwardProducersRetriever) {
        final Map<StreamNode, Set<StreamNode>> nodeToGroup = new IdentityHashMap<>();
        for (StreamNode currentNode : topologicallySortedChainedStreamNodesMap.keySet()) {
            Set<StreamNode> currentGroup = new HashSet<>();
            currentGroup.add(currentNode);
            nodeToGroup.put(currentNode, currentGroup);
            for (StreamNode producerNode : forwardProducersRetriever.apply(currentNode)) {
                final Set<StreamNode> producerGroup = nodeToGroup.get(producerNode);
                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerNode.getId()
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + currentNode.getId()
                                    + ". This should be a forward group building bug.");
                }
                if (currentGroup != producerGroup) {
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, nodeToGroup);
                }
            }
        }
        final Map<Integer, StreamNodeForwardGroup> result = new HashMap<>();
        for (Set<StreamNode> nodeGroup : VertexGroupComputeUtil.uniqueVertexGroups(nodeToGroup)) {
            if (!nodeGroup.isEmpty()) {
                StreamNodeForwardGroup streamNodeForwardGroup =
                        new StreamNodeForwardGroup(
                                nodeGroup, topologicallySortedChainedStreamNodesMap);
                for (StreamNode startNode : streamNodeForwardGroup.getStartNodes()) {
                    result.put(startNode.getId(), streamNodeForwardGroup);
                }
            }
        }
        return result;
    }

    public static boolean canTargetMergeIntoSourceForwardGroup(
            ForwardGroup sourceForwardGroup, ForwardGroup targetForwardGroup) {
        if (sourceForwardGroup == null || targetForwardGroup == null) {
            return false;
        }

        if (sourceForwardGroup == targetForwardGroup) {
            return true;
        }

        if (sourceForwardGroup.isParallelismDecided()
                && targetForwardGroup.isParallelismDecided()
                && sourceForwardGroup.getParallelism() != targetForwardGroup.getParallelism()) {
            return false;
        }

        if (sourceForwardGroup.isMaxParallelismDecided()
                && targetForwardGroup.isMaxParallelismDecided()
                && sourceForwardGroup.getMaxParallelism()
                        > targetForwardGroup.getMaxParallelism()) {
            return false;
        }

        return true;
    }

    static Set<JobVertex> getForwardProducers(final JobVertex jobVertex) {
        return jobVertex.getInputs().stream()
                .filter(JobEdge::isForward)
                .map(JobEdge::getSource)
                .map(IntermediateDataSet::getProducer)
                .collect(Collectors.toSet());
    }
}
