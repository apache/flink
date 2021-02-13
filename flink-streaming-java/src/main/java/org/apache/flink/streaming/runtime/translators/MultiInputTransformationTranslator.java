/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link MultipleInputTransformation} and the {@link
 * KeyedMultipleInputTransformation}.
 *
 * @param <OUT> The type of the elements that result from this {@code MultipleInputTransformation}
 */
@Internal
public class MultiInputTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, AbstractMultipleInputTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final AbstractMultipleInputTransformation<OUT> transformation, final Context context) {
        Collection<Integer> ids = translateInternal(transformation, context);
        if (transformation instanceof KeyedMultipleInputTransformation) {
            KeyedMultipleInputTransformation<OUT> keyedTransformation =
                    (KeyedMultipleInputTransformation<OUT>) transformation;
            List<Transformation<?>> inputs = transformation.getInputs();
            List<KeySelector<?, ?>> keySelectors = keyedTransformation.getStateKeySelectors();

            StreamConfig.InputRequirement[] inputRequirements =
                    IntStream.range(0, inputs.size())
                            .mapToObj(
                                    idx -> {
                                        if (keySelectors.get(idx) != null) {
                                            return StreamConfig.InputRequirement.SORTED;
                                        } else {
                                            return StreamConfig.InputRequirement.PASS_THROUGH;
                                        }
                                    })
                            .toArray(StreamConfig.InputRequirement[]::new);

            BatchExecutionUtils.applyBatchExecutionSettings(
                    transformation.getId(), context, inputRequirements);
        }
        return ids;
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final AbstractMultipleInputTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final AbstractMultipleInputTransformation<OUT> transformation, final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        final List<Transformation<?>> inputTransformations = transformation.getInputs();
        checkArgument(
                !inputTransformations.isEmpty(),
                "Empty inputs for MultipleInputTransformation. Did you forget to add inputs?");
        MultipleInputSelectionHandler.checkSupportedInputCount(inputTransformations.size());

        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        final int transformationId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        streamGraph.addMultipleInputOperator(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                transformation.getOperatorFactory(),
                transformation.getInputTypes(),
                transformation.getOutputType(),
                transformation.getName());

        final int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        streamGraph.setParallelism(transformationId, parallelism);
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

        if (transformation instanceof KeyedMultipleInputTransformation) {
            KeyedMultipleInputTransformation<OUT> keyedTransform =
                    (KeyedMultipleInputTransformation<OUT>) transformation;
            TypeSerializer<?> keySerializer =
                    keyedTransform.getStateKeyType().createSerializer(executionConfig);
            streamGraph.setMultipleInputStateKey(
                    transformationId, keyedTransform.getStateKeySelectors(), keySerializer);
        }

        for (int i = 0; i < inputTransformations.size(); i++) {
            final Transformation<?> inputTransformation = inputTransformations.get(i);
            final Collection<Integer> inputIds = context.getStreamNodeIds(inputTransformation);
            for (Integer inputId : inputIds) {
                streamGraph.addEdge(inputId, transformationId, i + 1);
            }
        }

        return Collections.singleton(transformationId);
    }
}
