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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexFinishedEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.SchedulerBase.getDefaultMaxParallelism;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultStreamGraphSchedulingContext}. */
class DefaultStreamGraphSchedulingContextTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testGetParallelismAndMaxParallelism() throws DynamicCodeLoadingException {
        int sourceParallelism = 3;
        int sinkParallelism = 4;
        int sourceMaxParallelism = 5;
        int sinkMaxParallelism = 5;

        StreamGraph streamGraph =
                getStreamGraph(
                        sourceParallelism,
                        sourceMaxParallelism,
                        sinkParallelism,
                        sinkMaxParallelism);

        StreamGraphSchedulingContext streamGraphSchedulingContext =
                getStreamGraphSchedulingContext(streamGraph, 100);

        Iterator<StreamNode> iterator = streamGraph.getStreamNodes().iterator();
        StreamNode source = iterator.next();
        assertThat(streamGraphSchedulingContext.getParallelism(source.getId()))
                .isEqualTo(sourceParallelism);
        assertThat(streamGraphSchedulingContext.getMaxParallelismOrDefault(source.getId()))
                .isEqualTo(sourceMaxParallelism);

        StreamNode sink = iterator.next();
        assertThat(streamGraphSchedulingContext.getParallelism(sink.getId()))
                .isEqualTo(sinkParallelism);
        assertThat(streamGraphSchedulingContext.getMaxParallelismOrDefault(sink.getId()))
                .isEqualTo(sinkMaxParallelism);
    }

    @Test
    void testGetDefaultMaxParallelism() throws DynamicCodeLoadingException {
        int sourceParallelism = 3;
        int sinkParallelism = -1;
        int sourceMaxParallelism = -1;
        int sinkMaxParallelism = -1;
        int defaultMaxParallelism = 100;

        StreamGraph streamGraph =
                getStreamGraph(
                        sourceParallelism,
                        sourceMaxParallelism,
                        sinkParallelism,
                        sinkMaxParallelism);

        StreamGraphSchedulingContext streamGraphSchedulingContext =
                getStreamGraphSchedulingContext(streamGraph, defaultMaxParallelism);

        Iterator<StreamNode> iterator = streamGraph.getStreamNodes().iterator();
        StreamNode source = iterator.next();
        // the source max parallelism will not fall back to the default max parallelism because its
        // parallelism is greater than zero.
        assertThat(streamGraphSchedulingContext.getMaxParallelismOrDefault(source.getId()))
                .isEqualTo(getDefaultMaxParallelism(sourceParallelism));

        StreamNode sink = iterator.next();
        // the sink max parallelism will fall back to default max parallelism
        assertThat(streamGraphSchedulingContext.getMaxParallelismOrDefault(sink.getId()))
                .isEqualTo(defaultMaxParallelism);
    }

    @Test
    public void testGetPendingOperatorCount() throws DynamicCodeLoadingException {
        StreamGraph streamGraph = getStreamGraph();
        DefaultAdaptiveExecutionHandler executionHandler =
                getDefaultAdaptiveExecutionHandler(streamGraph);
        StreamGraphSchedulingContext schedulingContext =
                getStreamGraphSchedulingContext(executionHandler, 1);

        assertThat(schedulingContext.getPendingOperatorCount()).isEqualTo(1);

        JobGraph jobGraph = executionHandler.getJobGraph();
        JobVertex source = jobGraph.getVertices().iterator().next();
        executionHandler.handleJobEvent(
                new ExecutionJobVertexFinishedEvent(source.getID(), Collections.emptyMap()));

        assertThat(schedulingContext.getPendingOperatorCount()).isEqualTo(0);
    }

    private static StreamGraph getStreamGraph() {
        return getStreamGraph(1, 1, 2, 2);
    }

    private static StreamGraph getStreamGraph(
            int sourceParallelism,
            int sourceMaxParallelism,
            int sinkParallelism,
            int sinkMaxParallelism) {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.fromSequence(0L, 1L).disableChaining().print();
        StreamGraph streamGraph = env.getStreamGraph();

        Iterator<StreamNode> iterator = streamGraph.getStreamNodes().iterator();

        StreamNode source = iterator.next();
        StreamNode sink = iterator.next();

        source.setParallelism(sourceParallelism);
        sink.setParallelism(sinkParallelism);

        if (sourceMaxParallelism > 0) {
            source.setMaxParallelism(sourceMaxParallelism);
        }

        if (sinkMaxParallelism > 0) {
            sink.setMaxParallelism(sinkMaxParallelism);
        }
        return streamGraph;
    }

    private static DefaultAdaptiveExecutionHandler getDefaultAdaptiveExecutionHandler(
            StreamGraph streamGraph) throws DynamicCodeLoadingException {
        return new DefaultAdaptiveExecutionHandler(
                Thread.currentThread().getContextClassLoader(),
                streamGraph,
                EXECUTOR_RESOURCE.getExecutor());
    }

    private static StreamGraphSchedulingContext getStreamGraphSchedulingContext(
            StreamGraph streamGraph, int defaultMaxParallelism) throws DynamicCodeLoadingException {
        return getDefaultAdaptiveExecutionHandler(streamGraph)
                .createStreamGraphSchedulingContext(defaultMaxParallelism);
    }

    private static StreamGraphSchedulingContext getStreamGraphSchedulingContext(
            AdaptiveExecutionHandler adaptiveExecutionHandler, int defaultMaxParallelism) {
        return adaptiveExecutionHandler.createStreamGraphSchedulingContext(defaultMaxParallelism);
    }
}
