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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A base class with functionality used during translating {@link Transformation transformations}
 * with two inputs.
 */
@Internal
public abstract class AbstractTwoInputTransformationTranslator<
                IN1, IN2, OUT, OP extends Transformation<OUT>>
        extends SimpleTransformationTranslator<OUT, OP> {

    protected Collection<Integer> translateInternal(
            final Transformation<OUT> transformation,
            final Transformation<IN1> firstInputTransformation,
            final Transformation<IN2> secondInputTransformation,
            final StreamOperatorFactory<OUT> operatorFactory,
            @Nullable final TypeInformation<?> keyTypeInfo,
            @Nullable final KeySelector<IN1, ?> firstKeySelector,
            @Nullable final KeySelector<IN2, ?> secondKeySelector,
            final Context context) {
        checkNotNull(transformation);
        checkNotNull(firstInputTransformation);
        checkNotNull(secondInputTransformation);
        checkNotNull(operatorFactory);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        final int transformationId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        streamGraph.addCoOperator(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                operatorFactory,
                firstInputTransformation.getOutputType(),
                secondInputTransformation.getOutputType(),
                transformation.getOutputType(),
                transformation.getName());

        if (firstKeySelector != null || secondKeySelector != null) {
            checkState(
                    keyTypeInfo != null,
                    "Keyed Transformation without provided key type information.");

            final TypeSerializer<?> keySerializer = keyTypeInfo.createSerializer(executionConfig);
            streamGraph.setTwoInputStateKey(
                    transformationId, firstKeySelector, secondKeySelector, keySerializer);
        }

        final int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        streamGraph.setParallelism(
                transformationId, parallelism, transformation.isParallelismConfigured());
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

        for (Integer inputId : context.getStreamNodeIds(firstInputTransformation)) {
            streamGraph.addEdge(inputId, transformationId, 1);
        }

        for (Integer inputId : context.getStreamNodeIds(secondInputTransformation)) {
            streamGraph.addEdge(inputId, transformationId, 2);
        }

        if (transformation instanceof PhysicalTransformation) {
            streamGraph.setSupportsConcurrentExecutionAttempts(
                    transformationId,
                    ((PhysicalTransformation<OUT>) transformation)
                            .isSupportsConcurrentExecutionAttempts());
        }

        return Collections.singleton(transformationId);
    }
}
