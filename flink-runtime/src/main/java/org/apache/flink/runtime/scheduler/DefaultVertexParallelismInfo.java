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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import java.util.Optional;
import java.util.function.Function;

/** A {@link VertexParallelismInformation} implementation that provides common validation. */
public class DefaultVertexParallelismInfo implements VertexParallelismInformation {
    private final int parallelism;
    private int maxParallelism;
    private final Function<Integer, Optional<String>> rescaleMaxValidator;

    /**
     * Create {@link VertexParallelismInformation} with max parallelism rescaling validation for a
     * vertex.
     *
     * @param parallelism the vertex's parallelism
     * @param maxParallelism the vertex's max parallelism
     * @param rescaleMaxValidator the validation function to provide an error message if a max
     *     parallelism rescale is not allowed
     */
    public DefaultVertexParallelismInfo(
            int parallelism,
            int maxParallelism,
            Function<Integer, Optional<String>> rescaleMaxValidator) {
        this.parallelism = checkParallelism(parallelism);
        this.maxParallelism = normalizeAndCheckMaxParallelism(maxParallelism);
        this.rescaleMaxValidator = Preconditions.checkNotNull(rescaleMaxValidator);
    }

    private static int normalizeAndCheckMaxParallelism(int maxParallelism) {
        if (maxParallelism == ExecutionConfig.PARALLELISM_AUTO_MAX) {
            maxParallelism = KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;
        }

        return checkBounds("max parallelism", maxParallelism);
    }

    private static int checkParallelism(int parallelism) {
        return checkBounds("parallelism", parallelism);
    }

    private static int checkBounds(String name, int parallelism) {
        Preconditions.checkArgument(
                parallelism > 0
                        && parallelism <= KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM,
                "Setting %s is not in valid bounds (1..%s), found: %s",
                name,
                KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM,
                parallelism);
        return parallelism;
    }

    @Override
    public int getParallelism() {
        return this.parallelism;
    }

    @Override
    public int getMaxParallelism() {
        return this.maxParallelism;
    }

    @Override
    public void setMaxParallelism(int maxParallelism) {
        maxParallelism = normalizeAndCheckMaxParallelism(maxParallelism);

        Optional<String> validationResult = rescaleMaxValidator.apply(maxParallelism);
        if (validationResult.isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Rescaling max parallelism from %s to %s is not allowed: %s",
                            this.maxParallelism, maxParallelism, validationResult.get()));
        }

        this.maxParallelism = maxParallelism;
    }

    @Override
    public boolean canRescaleMaxParallelism(int desiredMaxParallelism) {
        // Technically a valid parallelism value, but one that cannot be rescaled to
        if (desiredMaxParallelism == JobVertex.MAX_PARALLELISM_DEFAULT) {
            return false;
        }

        return !rescaleMaxValidator
                .apply(normalizeAndCheckMaxParallelism(desiredMaxParallelism))
                .isPresent();
    }
}
