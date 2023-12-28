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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link TransformationTranslator} for the {@link OneInputTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to
 *     translate.
 * @param <OUT> The type of the elements that result from the provided {@code
 *     OneInputTransformation}.
 */
@Internal
public final class OneInputTransformationTranslator<IN, OUT>
        extends AbstractOneInputTransformationTranslator<IN, OUT, OneInputTransformation<IN, OUT>> {

    @Override
    public Collection<Integer> translateForBatchInternal(
            final OneInputTransformation<IN, OUT> transformation, final Context context) {
        KeySelector<IN, ?> keySelector = transformation.getStateKeySelector();
        Collection<Integer> ids =
                translateInternal(
                        transformation,
                        transformation.getOperatorFactory(),
                        transformation.getInputType(),
                        keySelector,
                        transformation.getStateKeyType(),
                        context);

        maybeApplyBatchExecutionSettings(transformation, context);

        return ids;
    }

    @Override
    public Collection<Integer> translateForStreamingInternal(
            final OneInputTransformation<IN, OUT> transformation, final Context context) {
        Collection<Integer> ids =
                translateInternal(
                        transformation,
                        transformation.getOperatorFactory(),
                        transformation.getInputType(),
                        transformation.getStateKeySelector(),
                        transformation.getStateKeyType(),
                        context);

        if (transformation.isOutputOnEOF()) {
            // Try to apply batch execution settings for streaming mode transformation.
            maybeApplyBatchExecutionSettings(transformation, context);
        }

        return ids;
    }

    private void maybeApplyBatchExecutionSettings(
            final OneInputTransformation<IN, OUT> transformation, final Context context) {
        KeySelector<IN, ?> keySelector = transformation.getStateKeySelector();
        boolean isKeyed = keySelector != null;
        if (isKeyed) {
            List<Transformation<?>> inputs = transformation.getInputs();
            List<KeySelector<?, ?>> keySelectors = Collections.singletonList(keySelector);
            StreamConfig.InputRequirement[] inputRequirements =
                    BatchExecutionUtils.getInputRequirements(
                            inputs, keySelectors, transformation.isInternalSorterSupported());
            BatchExecutionUtils.applyBatchExecutionSettings(
                    transformation.getId(), context, inputRequirements);
        }
    }
}
