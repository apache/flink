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

package org.apache.flink.graph.generator;

import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RMatGraph}. */
public class RMatGraphTest extends GraphGeneratorTestBase {

    @Test
    public void testGraphMetrics() throws Exception {
        long vertexCount = 100;

        long edgeCount = 1000;

        RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

        Graph<LongValue, NullValue, NullValue> graph =
                new RMatGraph<>(env, rnd, vertexCount, edgeCount).generate();

        assertTrue(vertexCount >= graph.numberOfVertices());
        assertEquals(edgeCount, graph.numberOfEdges());
    }

    @Test
    public void testParallelism() throws Exception {
        int parallelism = 2;

        RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

        Graph<LongValue, NullValue, NullValue> graph =
                new RMatGraph<>(env, rnd, 100, 1000).setParallelism(parallelism).generate();

        graph.getVertices().output(new DiscardingOutputFormat<>());
        graph.getEdges().output(new DiscardingOutputFormat<>());

        TestUtils.verifyParallelism(env, parallelism);
    }
}
