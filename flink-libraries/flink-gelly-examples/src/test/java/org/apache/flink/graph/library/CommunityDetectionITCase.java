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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.CommunityDetectionData;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Tests for {@link CommunityDetection}. */
@RunWith(Parameterized.class)
public class CommunityDetectionITCase extends MultipleProgramsTestBase {

    public CommunityDetectionITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expected;

    @Test
    public void testSingleIteration() throws Exception {
        /*
         * Test one iteration of the Simple Community Detection Example
         */

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, Long, Double> inputGraph =
                Graph.fromDataSet(
                        CommunityDetectionData.getSimpleEdgeDataSet(env), new InitLabels(), env);

        List<Vertex<Long, Long>> result =
                inputGraph
                        .run(new CommunityDetection<>(1, CommunityDetectionData.DELTA))
                        .getVertices()
                        .collect();

        expected = CommunityDetectionData.COMMUNITIES_SINGLE_ITERATION;
        compareResultAsTuples(result, expected);
    }

    @Test
    public void testTieBreaker() throws Exception {
        /*
         * Test one iteration of the Simple Community Detection Example where a tie must be broken
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, Long, Double> inputGraph =
                Graph.fromDataSet(
                        CommunityDetectionData.getTieEdgeDataSet(env), new InitLabels(), env);

        List<Vertex<Long, Long>> result =
                inputGraph
                        .run(new CommunityDetection<>(1, CommunityDetectionData.DELTA))
                        .getVertices()
                        .collect();
        expected = CommunityDetectionData.COMMUNITIES_WITH_TIE;
        compareResultAsTuples(result, expected);
    }

    @SuppressWarnings("serial")
    private static final class InitLabels implements MapFunction<Long, Long> {

        public Long map(Long id) {
            return id;
        }
    }
}
