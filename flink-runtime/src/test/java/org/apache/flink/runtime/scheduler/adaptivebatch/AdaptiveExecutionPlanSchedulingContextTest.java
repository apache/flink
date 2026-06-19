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
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.SchedulerBase.getDefaultMaxParallelism;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AdaptiveExecutionPlanSchedulingContext}. */
class AdaptiveExecutionPlanSchedulingContextTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testGetParallelismAndMaxParallelism() throws DynamicCodeLoadingException {
        int sinkParallelism = 4;
        int sinkMaxParallelism = 5;

        DefaultAdaptiveExecutionHandler adaptiveExecutionHandler =
                getDefaultAdaptiveExecutionHandler(sinkParallelism, sinkMaxParallelism);
        ExecutionPlanSchedulingContext schedulingContext =
                adaptiveExecutionHandler.createExecutionPlanSchedulingContext(100);
        JobGraph jobGraph = adaptiveExecutionHandler.getJobGraph();

        JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        IntermediateDataSet sourceDataSet = sourceVertex.getProducedDataSets().get(0);

        assertThat(schedulingContext.getConsumersParallelism(id -> 123, sourceDataSet))
                .isEqualTo(sinkParallelism);
        assertThat(schedulingContext.getConsumersMaxParallelism(id -> 456, sourceDataSet))
                .isEqualTo(sinkMaxParallelism);

        // notify source finished, then the sink vertex will be created
        adaptiveExecutionHandler.handleJobEvent(
                new ExecutionJobVertexFinishedEvent(sourceVertex.getID(), Collections.emptyMap()));
        assertThat(schedulingContext.getConsumersParallelism(id -> 123, sourceDataSet))
                .isEqualTo(123);
        assertThat(schedulingContext.getConsumersMaxParallelism(id -> 456, sourceDataSet))
                .isEqualTo(456);
    }

    @Test
    void testGetDefaultMaxParallelismWhenParallelismGreaterThanZero()
            throws DynamicCodeLoadingException {
        int sinkParallelism = 4;
        int sinkMaxParallelism = -1;
        int defaultMaxParallelism = 100;

        DefaultAdaptiveExecutionHandler adaptiveExecutionHandler =
                getDefaultAdaptiveExecutionHandler(sinkParallelism, sinkMaxParallelism);
        ExecutionPlanSchedulingContext schedulingContext =
                adaptiveExecutionHandler.createExecutionPlanSchedulingContext(
                        defaultMaxParallelism);
        JobGraph jobGraph = adaptiveExecutionHandler.getJobGraph();
        JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        IntermediateDataSet dataSet = sourceVertex.getProducedDataSets().get(0);

        // the max parallelism will not fall back to the default max parallelism because its
        // parallelism is greater than zero.
        assertThat(schedulingContext.getConsumersMaxParallelism(id -> 123, dataSet))
                .isEqualTo(getDefaultMaxParallelism(sinkParallelism));
    }

    @Test
    void testGetDefaultMaxParallelismWhenParallelismLessThanZero()
            throws DynamicCodeLoadingException {
        int sinkParallelism = -1;
        int sinkMaxParallelism = -1;
        int defaultMaxParallelism = 100;

        DefaultAdaptiveExecutionHandler adaptiveExecutionHandler =
                getDefaultAdaptiveExecutionHandler(sinkParallelism, sinkMaxParallelism);
        ExecutionPlanSchedulingContext schedulingContext =
                adaptiveExecutionHandler.createExecutionPlanSchedulingContext(
                        defaultMaxParallelism);
        JobGraph jobGraph = adaptiveExecutionHandler.getJobGraph();
        JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        IntermediateDataSet dataSet = sourceVertex.getProducedDataSets().get(0);

        // the max parallelism will fall back to the default max parallelism because its
        // parallelism is less than zero.
        assertThat(schedulingContext.getConsumersMaxParallelism(id -> 123, dataSet))
                .isEqualTo(defaultMaxParallelism);
    }

    @Test
    public void testGetPendingOperatorCount() throws DynamicCodeLoadingException {
        DefaultAdaptiveExecutionHandler adaptiveExecutionHandler =
                getDefaultAdaptiveExecutionHandler();
        ExecutionPlanSchedulingContext schedulingContext =
                adaptiveExecutionHandler.createExecutionPlanSchedulingContext(1);

        assertThat(schedulingContext.getPendingOperatorCount()).isEqualTo(1);

        JobGraph jobGraph = adaptiveExecutionHandler.getJobGraph();
        JobVertex source = jobGraph.getVertices().iterator().next();
        adaptiveExecutionHandler.handleJobEvent(
                new ExecutionJobVertexFinishedEvent(source.getID(), Collections.emptyMap()));

        assertThat(schedulingContext.getPendingOperatorCount()).isEqualTo(0);
    }

    private static DefaultAdaptiveExecutionHandler getDefaultAdaptiveExecutionHandler()
            throws DynamicCodeLoadingException {
        return getDefaultAdaptiveExecutionHandler(2, 2);
    }

    private static DefaultAdaptiveExecutionHandler getDefaultAdaptiveExecutionHandler(
            int sinkParallelism, int sinkMaxParallelism) throws DynamicCodeLoadingException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0L, 1L).disableChaining().print();
        StreamGraph streamGraph = env.getStreamGraph();

        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            if (streamNode.getOperatorName().contains("Sink")) {
                streamNode.setParallelism(sinkParallelism);

                if (sinkMaxParallelism > 0) {
                    streamNode.setMaxParallelism(sinkMaxParallelism);
                }
            }
        }

        return new DefaultAdaptiveExecutionHandler(
                Thread.currentThread().getContextClassLoader(),
                streamGraph,
                EXECUTOR_RESOURCE.getExecutor());
    }
}
