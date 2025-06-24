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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.streaming.api.graph.AdaptiveGraphManager;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link DefaultAdaptiveExecutionHandler} implements the {@link AdaptiveExecutionHandler}
 * interface to provide an incrementally generated job graph.
 *
 * <p>This handler can modify the execution plan before downstream job vertices are created,
 * allowing for flexibility and adaptability during runtime.
 */
public class DefaultAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(DefaultAdaptiveExecutionHandler.class);

    private final List<JobGraphUpdateListener> jobGraphUpdateListeners = new ArrayList<>();

    private final AdaptiveGraphManager adaptiveGraphManager;

    private final StreamGraphOptimizer streamGraphOptimizer;

    public DefaultAdaptiveExecutionHandler(
            ClassLoader userClassloader, StreamGraph streamGraph, Executor serializationExecutor)
            throws DynamicCodeLoadingException {
        this.adaptiveGraphManager =
                new AdaptiveGraphManager(userClassloader, streamGraph, serializationExecutor);

        this.streamGraphOptimizer =
                new StreamGraphOptimizer(streamGraph.getJobConfiguration(), userClassloader);
        this.streamGraphOptimizer.initializeStrategies(
                adaptiveGraphManager.getStreamGraphContext());
    }

    @Override
    public JobGraph getJobGraph() {
        return adaptiveGraphManager.getJobGraph();
    }

    @Override
    public void handleJobEvent(JobEvent jobEvent) {
        try {
            tryOptimizeStreamGraph(jobEvent);
            tryUpdateJobGraph(jobEvent);
        } catch (Exception e) {
            log.error("Failed to handle job event {}.", jobEvent, e);
            throw new RuntimeException(e);
        }
    }

    private void tryOptimizeStreamGraph(JobEvent jobEvent) throws Exception {
        if (jobEvent instanceof ExecutionJobVertexFinishedEvent) {
            ExecutionJobVertexFinishedEvent event = (ExecutionJobVertexFinishedEvent) jobEvent;

            JobVertexID vertexId = event.getVertexId();
            Map<IntermediateDataSetID, BlockingResultInfo> resultInfo = event.getResultInfo();
            Map<Integer, List<BlockingResultInfo>> resultInfoMap =
                    resultInfo.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            entry ->
                                                    adaptiveGraphManager.getProducerStreamNodeId(
                                                            entry.getKey()),
                                            entry ->
                                                    new ArrayList<>(
                                                            Collections.singletonList(
                                                                    entry.getValue())),
                                            (existing, replacement) -> {
                                                existing.addAll(replacement);
                                                return existing;
                                            }));

            List<Integer> finishedStreamNodeIds =
                    adaptiveGraphManager.getStreamNodeIdsByJobVertexId(vertexId);
            OperatorsFinished operatorsFinished =
                    new OperatorsFinished(finishedStreamNodeIds, resultInfoMap);
            adaptiveGraphManager.addFinishedStreamNodeIds(finishedStreamNodeIds);

            streamGraphOptimizer.onOperatorsFinished(
                    operatorsFinished, adaptiveGraphManager.getStreamGraphContext());
        } else {
            throw new IllegalArgumentException("Unsupported job event " + jobEvent);
        }
    }

    private void tryUpdateJobGraph(JobEvent jobEvent) throws Exception {
        if (jobEvent instanceof ExecutionJobVertexFinishedEvent) {
            ExecutionJobVertexFinishedEvent event = (ExecutionJobVertexFinishedEvent) jobEvent;

            List<JobVertex> newlyCreatedJobVertices =
                    adaptiveGraphManager.onJobVertexFinished(event.getVertexId());

            if (!newlyCreatedJobVertices.isEmpty()) {
                notifyJobGraphUpdated(
                        newlyCreatedJobVertices, adaptiveGraphManager.getPendingOperatorsCount());
            }
        }
    }

    private void notifyJobGraphUpdated(List<JobVertex> jobVertices, int pendingOperatorsCount)
            throws Exception {
        for (JobGraphUpdateListener listener : jobGraphUpdateListeners) {
            listener.onNewJobVerticesAdded(jobVertices, pendingOperatorsCount);
        }
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        jobGraphUpdateListeners.add(listener);
    }

    @Override
    public int getInitialParallelism(JobVertexID jobVertexId) {
        JobVertex jobVertex = adaptiveGraphManager.getJobGraph().findVertexByID(jobVertexId);
        int vertexInitialParallelism = jobVertex.getParallelism();
        StreamNodeForwardGroup forwardGroup =
                adaptiveGraphManager.getStreamNodeForwardGroupByVertexId(jobVertexId);

        if (jobVertex.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT
                && forwardGroup != null
                && forwardGroup.isParallelismDecided()) {
            vertexInitialParallelism = forwardGroup.getParallelism();
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                    jobVertex.getName(),
                    jobVertexId,
                    vertexInitialParallelism);
        }

        return vertexInitialParallelism;
    }

    @Override
    public void notifyJobVertexParallelismDecided(JobVertexID jobVertexId, int parallelism) {
        StreamNodeForwardGroup forwardGroup =
                adaptiveGraphManager.getStreamNodeForwardGroupByVertexId(jobVertexId);
        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(parallelism);
        } else if (forwardGroup != null) {
            checkArgument(
                    forwardGroup.getParallelism() == parallelism,
                    "Incompatible parallelism for forward group.");
        }
    }

    @Override
    public ExecutionPlanSchedulingContext createExecutionPlanSchedulingContext(
            int defaultMaxParallelism) {
        return new AdaptiveExecutionPlanSchedulingContext(
                adaptiveGraphManager, defaultMaxParallelism);
    }
}
