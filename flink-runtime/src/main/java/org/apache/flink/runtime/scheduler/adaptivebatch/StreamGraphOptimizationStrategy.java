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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.graph.StreamGraphContext;

import java.util.List;

/**
 * Defines an optimization strategy for StreamGraph. Implementors of this interface provide methods
 * to modify and optimize a StreamGraph based on contexts provided at runtime.
 */
@FunctionalInterface
public interface StreamGraphOptimizationStrategy {

    @Internal
    ConfigOption<List<String>> STREAM_GRAPH_OPTIMIZATION_STRATEGY =
            ConfigOptions.key("execution.batch.adaptive.stream-graph-optimization.strategies")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a comma-separated list of fully qualified class names "
                                    + "implementing the StreamGraphOptimizationStrategy interface.");

    /**
     * Initializes the StreamGraphOptimizationStrategy with the provided {@link StreamGraphContext}.
     *
     * @param context the StreamGraphContext with a read-only view of a StreamGraph, providing
     *     methods to modify StreamEdges and StreamNodes within the StreamGraph.
     */
    default void initialize(StreamGraphContext context) {}

    /**
     * Tries to optimize the StreamGraph using the provided {@link OperatorsFinished} and {@link
     * StreamGraphContext}. The method returns a boolean indicating whether the StreamGraph was
     * successfully optimized.
     *
     * @param operatorsFinished the OperatorsFinished object containing information about completed
     *     operators and their produced data size and distribution information.
     * @param context the StreamGraphContext with a read-only view of a StreamGraph, providing
     *     methods to modify StreamEdges and StreamNodes within the StreamGraph.
     * @return {@code true} if the StreamGraph was successfully optimized; {@code false} otherwise.
     */
    boolean onOperatorsFinished(OperatorsFinished operatorsFinished, StreamGraphContext context)
            throws Exception;
}
