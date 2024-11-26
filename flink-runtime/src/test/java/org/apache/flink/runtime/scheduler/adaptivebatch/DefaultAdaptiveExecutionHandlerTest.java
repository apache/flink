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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexFinishedEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultAdaptiveExecutionHandler}. */
class DefaultAdaptiveExecutionHandlerTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testGetJobGraph() {
        JobGraph jobGraph = createAdaptiveExecutionHandler().getJobGraph();

        assertThat(jobGraph).isNotNull();
        assertThat(jobGraph.getNumberOfVertices()).isOne();
        assertThat(jobGraph.getVertices().iterator().next().getName()).contains("Source");
    }

    @Test
    void testHandleJobEvent() {
        List<JobVertex> newAddedJobVertices = new ArrayList<>();
        AtomicInteger pendingOperators = new AtomicInteger();

        DefaultAdaptiveExecutionHandler handler =
                createAdaptiveExecutionHandler(
                        (newVertices, pendingOperatorsCount) -> {
                            newAddedJobVertices.addAll(newVertices);
                            pendingOperators.set(pendingOperatorsCount);
                        },
                        createStreamGraph());

        JobGraph jobGraph = handler.getJobGraph();

        JobVertex source =
                jobGraph.getVerticesSortedTopologicallyFromSources().stream()
                        .filter(jobVertex -> jobVertex.getName().contains("Source"))
                        .findFirst()
                        .get();

        // notify Source node is finished
        ExecutionJobVertexFinishedEvent event1 =
                new ExecutionJobVertexFinishedEvent(source.getID(), Collections.emptyMap());
        handler.handleJobEvent(event1);
        assertThat(newAddedJobVertices).hasSize(1);
        assertThat(newAddedJobVertices.get(0).getName()).contains("Map");
        assertThat(pendingOperators.get()).isOne();

        // notify Map node is finished
        ExecutionJobVertexFinishedEvent event2 =
                new ExecutionJobVertexFinishedEvent(
                        newAddedJobVertices.get(0).getID(), Collections.emptyMap());
        handler.handleJobEvent(event2);
        assertThat(newAddedJobVertices).hasSize(2);
        assertThat(newAddedJobVertices.get(1).getName()).contains("Sink");
        assertThat(pendingOperators.get()).isZero();
    }

    @Test
    void testGetInitialParallelismAndNotifyJobVertexParallelismDecided() {
        StreamGraph streamGraph = createStreamGraph();
        DefaultAdaptiveExecutionHandler handler =
                createAdaptiveExecutionHandler(
                        (newVertices, pendingOperatorsCount) -> {}, streamGraph);
        JobGraph jobGraph = handler.getJobGraph();
        JobVertex source =
                jobGraph.getVerticesSortedTopologicallyFromSources().stream()
                        .filter(jobVertex -> jobVertex.getName().contains("Source"))
                        .findFirst()
                        .get();

        assertThat(handler.getInitialParallelism(source.getID()))
                .isEqualTo(source.getParallelism());

        Random random = new Random();
        int parallelism = 1 + random.nextInt(8);
        handler.notifyJobVertexParallelismDecided(source.getID(), parallelism);
        handler.handleJobEvent(
                new ExecutionJobVertexFinishedEvent(source.getID(), Collections.emptyMap()));
        JobVertex map =
                jobGraph.getVerticesSortedTopologicallyFromSources().stream()
                        .filter(jobVertex -> jobVertex.getName().contains("Map"))
                        .findFirst()
                        .get();
        assertThat(handler.getInitialParallelism(map.getID())).isEqualTo(parallelism);
    }

    private DefaultAdaptiveExecutionHandler createAdaptiveExecutionHandler() {
        return createAdaptiveExecutionHandler(
                (newVertices, pendingOperatorsCount) -> {}, createStreamGraph());
    }

    /**
     * Create a stream graph with the following topology.
     *
     * <pre>
     *     Source -- forward --> Map -- rescale --> Sink
     * </pre>
     */
    private StreamGraph createStreamGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.fromSequence(0, 1)
                .name("Source")
                .forward()
                .map(i -> i)
                .name("Map")
                .rescale()
                .print()
                .name("Sink")
                .disableChaining();
        env.setParallelism(1);

        return env.getStreamGraph();
    }

    /**
     * Create an {@link DefaultAdaptiveExecutionHandler} with a given {@link JobGraphUpdateListener}
     * and a given {@link StreamGraph}.
     */
    private DefaultAdaptiveExecutionHandler createAdaptiveExecutionHandler(
            JobGraphUpdateListener listener, StreamGraph streamGraph) {
        DefaultAdaptiveExecutionHandler handler =
                new DefaultAdaptiveExecutionHandler(
                        getClass().getClassLoader(), streamGraph, EXECUTOR_RESOURCE.getExecutor());
        handler.registerJobGraphUpdateListener(listener);

        return handler;
    }
}
