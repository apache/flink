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
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link TwoInputTransformation}.
 *
 * @param <IN1> The type of the elements in the first input {@code Transformation}
 * @param <IN2> The type of the elements in the second input {@code Transformation}
 * @param <OUT> The type of the elements that result from this {@link TwoInputTransformation}
 */
@Internal
public class TwoInputTransformationTranslator<IN1, IN2, OUT>
        extends AbstractTwoInputTransformationTranslator<
                IN1, IN2, OUT, TwoInputTransformation<IN1, IN2, OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final TwoInputTransformation<IN1, IN2, OUT> transformation, final Context context) {
        Collection<Integer> ids = translateInternal(transformation, context);

        StreamConfig.InputRequirement input1Requirement =
                transformation.getStateKeySelector1() != null
                        ? StreamConfig.InputRequirement.SORTED
                        : StreamConfig.InputRequirement.PASS_THROUGH;

        StreamConfig.InputRequirement input2Requirement =
                transformation.getStateKeySelector2() != null
                        ? StreamConfig.InputRequirement.SORTED
                        : StreamConfig.InputRequirement.PASS_THROUGH;

        if (input1Requirement == StreamConfig.InputRequirement.SORTED
                || input2Requirement == StreamConfig.InputRequirement.SORTED) {
            BatchExecutionUtils.applyBatchExecutionSettings(
                    transformation.getId(), context, input1Requirement, input2Requirement);
        }
        return ids;
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final TwoInputTransformation<IN1, IN2, OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final TwoInputTransformation<IN1, IN2, OUT> transformation, final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        return translateInternal(
                transformation,
                transformation.getInput1(),
                transformation.getInput2(),
                transformation.getOperatorFactory(),
                transformation.getStateKeyType(),
                transformation.getStateKeySelector1(),
                transformation.getStateKeySelector2(),
                context);
    }
}
