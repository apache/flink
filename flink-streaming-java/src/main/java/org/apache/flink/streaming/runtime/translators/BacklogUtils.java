/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.TransformationTranslator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG;

/** A utility class for applying sorting inputs during backlog processing. */
public class BacklogUtils {
    public static boolean isCheckpointDisableDuringBacklog(
            TransformationTranslator.Context context) {
        return context.getGraphGeneratorConfig().get(CHECKPOINTING_INTERVAL_DURING_BACKLOG) != null
                && context.getGraphGeneratorConfig()
                        .get(CHECKPOINTING_INTERVAL_DURING_BACKLOG)
                        .isZero();
    }

    public static void applyBacklogProcessingSettings(
            Transformation<?> transformation,
            TransformationTranslator.Context context,
            StreamNode node) {
        node.addInputRequirement(0, StreamConfig.InputRequirement.SORTED_DURING_BACKLOG);
        Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights = new HashMap<>();
        Integer operatorMemoryWeights =
                transformation
                        .getManagedMemoryOperatorScopeUseCaseWeights()
                        .get(ManagedMemoryUseCase.OPERATOR);
        operatorScopeUseCaseWeights.put(
                ManagedMemoryUseCase.OPERATOR,
                operatorMemoryWeights == null
                        ? deriveMemoryWeight(context.getGraphGeneratorConfig())
                        : operatorMemoryWeights);
        node.setManagedMemoryUseCaseWeights(operatorScopeUseCaseWeights, Collections.emptySet());
    }

    private static int deriveMemoryWeight(ReadableConfig configuration) {
        return Math.max(1, configuration.get(ExecutionOptions.SORTED_INPUTS_MEMORY).getMebiBytes());
    }
}
