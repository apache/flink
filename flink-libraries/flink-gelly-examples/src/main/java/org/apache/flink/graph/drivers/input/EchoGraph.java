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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import static org.apache.flink.graph.generator.EchoGraph.MINIMUM_VERTEX_COUNT;
import static org.apache.flink.graph.generator.EchoGraph.MINIMUM_VERTEX_DEGREE;

/** Generate an {@link org.apache.flink.graph.generator.EchoGraph}. */
public class EchoGraph extends GeneratedGraph<LongValue> {

    private LongParameter vertexCount =
            new LongParameter(this, "vertex_count").setMinimumValue(MINIMUM_VERTEX_COUNT);

    private LongParameter vertexDegree =
            new LongParameter(this, "vertex_degree").setMinimumValue(MINIMUM_VERTEX_DEGREE);

    @Override
    public String getIdentity() {
        return getName() + " (" + vertexCount.getValue() + ":" + vertexDegree.getValue() + ")";
    }

    @Override
    protected long vertexCount() {
        return vertexCount.getValue();
    }

    @Override
    public Graph<LongValue, NullValue, NullValue> create(ExecutionEnvironment env)
            throws Exception {
        return new org.apache.flink.graph.generator.EchoGraph(
                        env, vertexCount.getValue(), vertexDegree.getValue())
                .setParallelism(parallelism.getValue().intValue())
                .generate();
    }
}
