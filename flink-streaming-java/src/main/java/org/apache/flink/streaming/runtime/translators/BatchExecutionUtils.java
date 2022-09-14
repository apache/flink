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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** A utility class for applying sorting inputs. */
class BatchExecutionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BatchExecutionUtils.class);

    static void applyBatchExecutionSettings(
            int transformationId,
            TransformationTranslator.Context context,
            StreamConfig.InputRequirement... inputRequirements) {
        StreamNode node = context.getStreamGraph().getStreamNode(transformationId);
        boolean sortInputs = context.getGraphGeneratorConfig().get(ExecutionOptions.SORT_INPUTS);
        boolean isInputSelectable = isInputSelectable(node);

        adjustChainingStrategy(node);

        checkState(
                !isInputSelectable || !sortInputs,
                "Batch state backend and sorting inputs are not supported in graphs with an InputSelectable operator.");

        if (sortInputs) {
            LOG.debug("Applying sorting/pass-through input requirements for operator {}.", node);
            for (int i = 0; i < inputRequirements.length; i++) {
                node.addInputRequirement(i, inputRequirements[i]);
            }
            Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights = new HashMap<>();
            operatorScopeUseCaseWeights.put(
                    ManagedMemoryUseCase.OPERATOR,
                    deriveMemoryWeight(context.getGraphGeneratorConfig()));
            node.setManagedMemoryUseCaseWeights(
                    operatorScopeUseCaseWeights, Collections.emptySet());
        }
    }

    private static int deriveMemoryWeight(ReadableConfig configuration) {
        return Math.max(1, configuration.get(ExecutionOptions.SORTED_INPUTS_MEMORY).getMebiBytes());
    }

    @SuppressWarnings("rawtypes")
    private static boolean isInputSelectable(StreamNode node) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<? extends StreamOperator> operatorClass =
                node.getOperatorFactory().getStreamOperatorClass(classLoader);
        return InputSelectable.class.isAssignableFrom(operatorClass);
    }

    private static void adjustChainingStrategy(StreamNode node) {
        StreamOperatorFactory<?> operatorFactory = node.getOperatorFactory();
        ChainingStrategy currentChainingStrategy = operatorFactory.getChainingStrategy();
        switch (currentChainingStrategy) {
            case ALWAYS:
            case HEAD_WITH_SOURCES:
                LOG.debug(
                        "Setting chaining strategy to HEAD for operator {}, because of the BATCH execution mode.",
                        node);
                operatorFactory.setChainingStrategy(ChainingStrategy.HEAD);
                break;
            case NEVER:
            case HEAD:
                break;
        }
    }

    private BatchExecutionUtils() {}
}
