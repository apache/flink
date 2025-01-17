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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.graph.util.StreamNodeUpdateRequestInfo;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StreamGraphOptimizer}. */
class StreamGraphOptimizerTest {
    private Configuration jobConfiguration;
    private ClassLoader userClassLoader;

    @BeforeEach
    void setUp() {
        jobConfiguration = new Configuration();
        userClassLoader = Thread.currentThread().getContextClassLoader();
    }

    @Test
    void testOnOperatorsFinished() throws Exception {
        List<String> strategyClassNames =
                List.of(TestingStreamGraphOptimizerStrategy.class.getName());
        jobConfiguration.set(
                StreamGraphOptimizationStrategy.STREAM_GRAPH_OPTIMIZATION_STRATEGY,
                strategyClassNames);

        StreamGraphOptimizer optimizer =
                new StreamGraphOptimizer(jobConfiguration, userClassLoader);

        OperatorsFinished operatorsFinished =
                new OperatorsFinished(new ArrayList<>(), new HashMap<>());
        StreamGraphContext context =
                new StreamGraphContext() {
                    @Override
                    public ImmutableStreamGraph getStreamGraph() {
                        return null;
                    }

                    @Override
                    public @Nullable StreamOperatorFactory<?> getOperatorFactory(
                            Integer streamNodeId) {
                        return null;
                    }

                    @Override
                    public boolean modifyStreamEdge(
                            List<StreamEdgeUpdateRequestInfo> requestInfos) {
                        return false;
                    }

                    @Override
                    public boolean modifyStreamNode(
                            List<StreamNodeUpdateRequestInfo> requestInfos) {
                        return false;
                    }

                    @Override
                    public boolean areAllUpstreamNodesFinished(ImmutableStreamNode streamNode) {
                        return false;
                    }

                    @Override
                    public IntermediateDataSetID getConsumedIntermediateDataSetId(String edgeId) {
                        return null;
                    }

                    @Override
                    public @Nullable StreamPartitioner<?> getOutputPartitioner(
                            String edgeId, Integer sourceId, Integer targetId) {
                        return null;
                    }
                };

        optimizer.onOperatorsFinished(operatorsFinished, context);

        assertThat(TestingStreamGraphOptimizerStrategy.collectedOperatorsFinished)
                .isEqualTo(operatorsFinished);
        assertThat(TestingStreamGraphOptimizerStrategy.collectedStreamGraphContext)
                .isEqualTo(context);
    }

    protected static final class TestingStreamGraphOptimizerStrategy
            implements StreamGraphOptimizationStrategy {

        private static OperatorsFinished collectedOperatorsFinished;
        private static StreamGraphContext collectedStreamGraphContext;

        @Override
        public boolean onOperatorsFinished(
                OperatorsFinished operatorsFinished, StreamGraphContext context) {
            collectedOperatorsFinished = operatorsFinished;
            collectedStreamGraphContext = context;
            return true;
        }
    }
}
