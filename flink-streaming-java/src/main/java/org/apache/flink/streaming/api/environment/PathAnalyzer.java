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
package org.apache.flink.streaming.api.environment;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;

public class PathAnalyzer {

    public static Integer computePathNum(StreamExecutionEnvironment env) throws Exception {
        StreamGraph streamGraph = env.getStreamGraph(false);
        verifyStreamGraph(streamGraph);

        JobGraph jobGraph = env.getStreamGraph(false).getJobGraph();

        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                SchedulerBase.computeVertexParallelismStore(jobGraph))
                        .build(Executors.newSingleThreadScheduledExecutor());
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        try {
            eg.attachJobGraph(
                    jobVertices,
                    UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        } catch (JobException e) {
            throw new Exception("Building ExecutionGraph failed: " + e.getMessage());
        }

        JobVertex sourceVertex = jobVertices.get(0);
        ExecutionJobVertex srcExecutionJobVertex = eg.getJobVertex(sourceVertex.getID());
        assert srcExecutionJobVertex != null;
        ExecutionVertex[] srcExecutionVertices = srcExecutionJobVertex.getTaskVertices();

        ExecutionVertex srcExecutionVertex = srcExecutionVertices[0];
        return dfs(srcExecutionVertex);
    }

    private static int dfs(ExecutionVertex v) {
        // reached sink vertex
        if (v.getProducedPartitions().isEmpty()) {
            return 1;
        }

        int pathNum = 0;
        for (IntermediateResultPartitionID pid : v.getProducedPartitions().keySet()) {
            List<ConsumerVertexGroup> vertexGroups =
                    v.getExecutionGraphAccessor()
                            .getEdgeManager()
                            .getConsumerVertexGroupsForPartition(pid);
            for (ConsumerVertexGroup group : vertexGroups) {
                for (ExecutionVertexID vertexID : group) {
                    ExecutionVertex vertex =
                            v.getExecutionGraphAccessor().getExecutionVertexOrThrow(vertexID);
                    pathNum += dfs(vertex);
                }
            }
        }

        return pathNum;
    }

    // verify that the stream graph has one source parallel instance and does not diverge
    private static void verifyStreamGraph(StreamGraph streamGraph) throws Exception {
        Collection<Integer> sourceNodeIds = streamGraph.getSourceIDs();
        if (sourceNodeIds.isEmpty()) {
            throw new Exception("Stream graph contains no source");
        }
        StreamNode curNode = streamGraph.getStreamNode((Integer) sourceNodeIds.toArray()[0]);
        if (sourceNodeIds.size() > 1 || curNode.getParallelism() > 1) {
            throw new Exception("Stream graph contains more than one source instance");
        }

        //        Collection<Integer> sinkIDs = streamGraph.getSinkIDs();
        //        if (sinkIDs.isEmpty()) {
        //            throw new Exception("Stream graph contains no sink");
        //        }
        //        StreamNode sinkNode = streamGraph.getStreamNode((Integer) sinkIDs.toArray()[0]);
        //        if (sinkIDs.size() > 1 || sinkNode.getParallelism() > 1) {
        //            throw new Exception("Stream graph contains more than one sink instance");
        //        }

        while (!curNode.getOutEdges().isEmpty()) {
            if (curNode.getOutEdges().size() > 1) {
                throw new Exception(
                        String.format("Stream topology diverges at %s", curNode.getOperatorName()));
            }
            StreamEdge edge = curNode.getOutEdges().get(0);
            curNode = streamGraph.getStreamNode(edge.getTargetId());
        }
    }
}
