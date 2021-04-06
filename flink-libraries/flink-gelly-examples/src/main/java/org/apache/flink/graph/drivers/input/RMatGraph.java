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
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.DoubleParameter;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import org.apache.commons.math3.random.JDKRandomGenerator;

/**
 * Generate an {@code RMatGraph} with {@link IntValue}, {@link LongValue}, or {@link StringValue}
 * keys.
 *
 * @see org.apache.flink.graph.generator.RMatGraph
 */
public class RMatGraph extends GeneratedMultiGraph {

    // generate graph with 2^scale vertices
    private LongParameter scale =
            new LongParameter(this, "scale").setDefaultValue(10).setMinimumValue(1);

    // generate graph with edgeFactor * 2^scale edges
    private LongParameter edgeFactor =
            new LongParameter(this, "edge_factor").setDefaultValue(16).setMinimumValue(1);

    // matrix parameters "a", "b", "c", and implicitly "d = 1 - a - b - c"
    // describe the skew in the recursive matrix
    private DoubleParameter a =
            new DoubleParameter(this, "a")
                    .setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_A)
                    .setMinimumValue(0.0, false);

    private DoubleParameter b =
            new DoubleParameter(this, "b")
                    .setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_B)
                    .setMinimumValue(0.0, false);

    private DoubleParameter c =
            new DoubleParameter(this, "c")
                    .setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_C)
                    .setMinimumValue(0.0, false);

    // noise randomly pertubates the matrix parameters for each successive bit
    // for each generated edge
    private BooleanParameter noiseEnabled = new BooleanParameter(this, "noise_enabled");

    private DoubleParameter noise =
            new DoubleParameter(this, "noise")
                    .setDefaultValue(org.apache.flink.graph.generator.RMatGraph.DEFAULT_NOISE)
                    .setMinimumValue(0.0, true)
                    .setMaximumValue(2.0, true);

    private LongParameter seed =
            new LongParameter(this, "seed").setDefaultValue(JDKRandomGeneratorFactory.DEFAULT_SEED);

    @Override
    public String getIdentity() {
        return getName() + " (s" + scale + "e" + edgeFactor + getSimplifyShortString() + ")";
    }

    @Override
    protected long vertexCount() {
        return 1L << scale.getValue();
    }

    @Override
    public Graph<LongValue, NullValue, NullValue> generate(ExecutionEnvironment env)
            throws Exception {
        RandomGenerableFactory<JDKRandomGenerator> rnd =
                new JDKRandomGeneratorFactory(seed.getValue());

        long vertexCount = 1L << scale.getValue();
        long edgeCount = vertexCount * edgeFactor.getValue();

        return new org.apache.flink.graph.generator.RMatGraph<>(env, rnd, vertexCount, edgeCount)
                .setConstants(
                        a.getValue().floatValue(),
                        b.getValue().floatValue(),
                        c.getValue().floatValue())
                .setNoise(noiseEnabled.getValue(), noise.getValue().floatValue())
                .setParallelism(parallelism.getValue().intValue())
                .generate();
    }
}
