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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AdaptiveExecutionHandlerFactory}. */
class AdaptiveExecutionHandlerFactoryTest {

    @RegisterExtension
    private final LoggerAuditingExtension factoryLogs =
            new LoggerAuditingExtension(AdaptiveExecutionHandlerFactory.class, Level.WARN);

    @Test
    void testStreamGraphIsGeneratedIncrementally() throws Exception {
        assertThat(create(streamGraph(), false))
                .isInstanceOf(DefaultAdaptiveExecutionHandler.class);
        assertThat(factoryLogs.getMessages()).isEmpty();
    }

    @Test
    void testParallelismOverridesAreIgnoredForAnIncrementallyGeneratedStreamGraph()
            throws Exception {
        final AdaptiveExecutionHandler handler = create(withMapOverriddenTo(3), false);

        assertThat(handler).isInstanceOf(DefaultAdaptiveExecutionHandler.class);
        assertThat(factoryLogs.getMessages())
                .singleElement(InstanceOfAssertFactories.STRING)
                .contains(PipelineOptions.PARALLELISM_OVERRIDES.key());
    }

    @Test
    void testParallelismOverridesApplyWithBatchJobRecovery() throws Exception {
        final AdaptiveExecutionHandler handler = create(withMapOverriddenTo(3), true);

        assertThat(handler).isInstanceOf(NonAdaptiveExecutionHandler.class);
        assertThat(handler.getJobGraph().getVerticesSortedTopologicallyFromSources().get(1))
                .extracting(JobVertex::getParallelism)
                .isEqualTo(3);
    }

    private static StreamGraph withMapOverriddenTo(int parallelism) {
        final String mapVertexId =
                streamGraph()
                        .getJobGraph(AdaptiveExecutionHandlerFactoryTest.class.getClassLoader())
                        .getVerticesSortedTopologicallyFromSources()
                        .get(1)
                        .getID()
                        .toHexString();
        final StreamGraph overridden = streamGraph();
        overridden
                .getJobConfiguration()
                .set(
                        PipelineOptions.PARALLELISM_OVERRIDES,
                        Collections.singletonMap(mapVertexId, String.valueOf(parallelism)));
        return overridden;
    }

    private static AdaptiveExecutionHandler create(
            StreamGraph streamGraph, boolean enableBatchJobRecovery) throws Exception {
        return AdaptiveExecutionHandlerFactory.create(
                streamGraph,
                enableBatchJobRecovery,
                AdaptiveExecutionHandlerFactoryTest.class.getClassLoader(),
                Runnable::run);
    }

    private static StreamGraph streamGraph() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.disableOperatorChaining();
        env.fromSequence(0, 9).uid("source").map(v -> v).uid("map").print();
        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobId(new JobID());
        return streamGraph;
    }
}
