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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.drivers.parameter.IterationConvergence.Value;

/**
 * Iterative algorithms which converge can be terminated with a maximum number of iterations or a
 * convergence threshold which stops computation when the total change in scores is below a given
 * delta.
 *
 * <p>If the command-line configuration specifies neither a number of iterations nor a convergence
 * threshold then a default number of iterations is used with an infinite convergence threshold.
 * Otherwise, when either value is configured then an unset value is set to infinity.
 */
public class IterationConvergence implements Parameter<Value> {

    private final int defaultIterations;

    private final Value value = new Value();

    /**
     * Add this parameter to the list of parameters stored by owner.
     *
     * @param owner the {@link Parameterized} using this {@link Parameter}
     * @param defaultIterations the default number of iterations if neither the number of iterations
     *     nor the convergence threshold are specified
     */
    public IterationConvergence(ParameterizedBase owner, int defaultIterations) {
        owner.addParameter(this);
        this.defaultIterations = defaultIterations;
    }

    @Override
    public String getUsage() {
        return "[--iterations ITERATIONS] [--convergence_threshold CONVERGENCE_THRESHOLD] ";
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public void configure(ParameterTool parameterTool) {
        if (!parameterTool.has("iterations") && !parameterTool.has("convergence_threshold")) {
            // no configuration so use default iterations and maximum threshold
            value.iterations = defaultIterations;
            value.convergenceThreshold = Double.MAX_VALUE;
        } else {
            // use configured values and maximum default for unset values
            value.iterations = parameterTool.getInt("iterations", Integer.MAX_VALUE);
            Util.checkParameter(value.iterations > 0, "iterations must be greater than zero");

            value.convergenceThreshold =
                    parameterTool.getDouble("convergence_threshold", Double.MAX_VALUE);
            Util.checkParameter(
                    value.convergenceThreshold > 0,
                    "convergence threshold must be greater than zero");
        }
    }

    @Override
    public Value getValue() {
        return value;
    }

    /** Encapsulate the number of iterations and the convergence threshold. */
    public static class Value {
        public int iterations;
        public double convergenceThreshold;
    }
}
