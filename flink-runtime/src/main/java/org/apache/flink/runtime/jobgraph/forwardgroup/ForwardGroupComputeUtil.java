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

import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
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

        final Map<JobVertex, Set<JobVertex>> vertexToGroup =
                computeVertexToGroup(topologicallySortedVertices, forwardProducersRetriever);

        final Map<JobVertexID, JobVertexForwardGroup> ret = new HashMap<>();
        for (Set<JobVertex> vertexGroup :
                VertexGroupComputeUtil.uniqueVertexGroups(vertexToGroup)) {
            if (vertexGroup.size() > 1) {
                JobVertexForwardGroup forwardGroup = new JobVertexForwardGroup(vertexGroup);
                for (JobVertexID jobVertexId : forwardGroup.getVertexIds()) {
                    ret.put(jobVertexId, forwardGroup);
                }
            }
        }
        return ret;
    }

    /**
     * We calculate forward group by a set of stream nodes.
     *
     * @param topologicallySortedStreamNodes topologically sorted chained stream nodes
     * @param forwardProducersRetriever records all upstream stream nodes which connected to the
     *     given stream node with forward edge
     * @return a map of forward groups, with the stream node id as the key
     */
    public static Map<Integer, StreamNodeForwardGroup> computeStreamNodeForwardGroup(
            final Iterable<StreamNode> topologicallySortedStreamNodes,
            final Function<StreamNode, Set<StreamNode>> forwardProducersRetriever) {
        // In the forwardProducersRetriever, only the upstream nodes connected to the given start
        // node by the forward edge are saved. We need to calculate the chain groups that can be
        // accessed with consecutive forward edges and put them in the same forward group.
        final Map<StreamNode, Set<StreamNode>> nodeToGroup =
                computeVertexToGroup(topologicallySortedStreamNodes, forwardProducersRetriever);
        final Map<Integer, StreamNodeForwardGroup> ret = new HashMap<>();
        for (Set<StreamNode> nodeGroup : VertexGroupComputeUtil.uniqueVertexGroups(nodeToGroup)) {
            StreamNodeForwardGroup forwardGroup = new StreamNodeForwardGroup(nodeGroup);
            for (Integer vertexId : forwardGroup.getVertexIds()) {
                ret.put(vertexId, forwardGroup);
            }
        }
        return ret;
    }

    private static <T> Map<T, Set<T>> computeVertexToGroup(
            final Iterable<T> topologicallySortedVertices,
            final Function<T, Set<T>> forwardProducersRetriever) {
        final Map<T, Set<T>> vertexToGroup = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (T vertex : topologicallySortedVertices) {
            Set<T> currentGroup = new HashSet<>();
            currentGroup.add(vertex);
            vertexToGroup.put(vertex, currentGroup);

            for (T producerVertex : forwardProducersRetriever.apply(vertex)) {
                final Set<T> producerGroup = vertexToGroup.get(producerVertex);

                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + vertex
                                    + ". This should be a forward group building bug.");
                }

                if (currentGroup != producerGroup) {
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, vertexToGroup);
                }
            }
        }
        return vertexToGroup;
    }

    /**
     * Determines whether the target forward group can be merged into the source forward group.
     *
     * @param sourceForwardGroup The source forward group.
     * @param forwardGroupToMerge The forward group needs to be merged.
     * @return whether the merge is valid.
     */
    public static boolean canTargetMergeIntoSourceForwardGroup(
            ForwardGroup<?> sourceForwardGroup, ForwardGroup<?> forwardGroupToMerge) {
        if (sourceForwardGroup == null || forwardGroupToMerge == null) {
            return false;
        }

        if (sourceForwardGroup == forwardGroupToMerge) {
            return true;
        }

        if (sourceForwardGroup.isParallelismDecided()
                && forwardGroupToMerge.isParallelismDecided()
                && sourceForwardGroup.getParallelism() != forwardGroupToMerge.getParallelism()) {
            return false;
        }

        // When the parallelism of source forward groups is determined, the maximum
        // parallelism of the forwardGroupToMerge should not be less than the parallelism of the
        // sourceForwardGroup to ensure the forwardGroupToMerge can also achieve the same
        // parallelism.
        if (sourceForwardGroup.isParallelismDecided()
                && forwardGroupToMerge.isMaxParallelismDecided()
                && sourceForwardGroup.getParallelism() > forwardGroupToMerge.getMaxParallelism()) {
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
