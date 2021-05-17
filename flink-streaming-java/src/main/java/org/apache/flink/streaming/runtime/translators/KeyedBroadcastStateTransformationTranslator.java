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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.co.BatchCoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.transformations.KeyedBroadcastStateTransformation;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link KeyedBroadcastStateTransformation}.
 *
 * @param <IN1> The type of the elements in the non-broadcasted input of the {@link
 *     KeyedBroadcastStateTransformation}.
 * @param <IN2> The type of the elements in the broadcasted input of the {@link
 *     KeyedBroadcastStateTransformation}.
 * @param <OUT> The type of the elements that result from the {@link
 *     KeyedBroadcastStateTransformation}.
 */
@Internal
public class KeyedBroadcastStateTransformationTranslator<KEY, IN1, IN2, OUT>
        extends AbstractTwoInputTransformationTranslator<
                IN1, IN2, OUT, KeyedBroadcastStateTransformation<KEY, IN1, IN2, OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final KeyedBroadcastStateTransformation<KEY, IN1, IN2, OUT> transformation,
            final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        BatchCoBroadcastWithKeyedOperator<KEY, IN1, IN2, OUT> operator =
                new BatchCoBroadcastWithKeyedOperator<>(
                        transformation.getUserFunction(),
                        transformation.getBroadcastStateDescriptors());

        Collection<Integer> result =
                translateInternal(
                        transformation,
                        transformation.getRegularInput(),
                        transformation.getBroadcastInput(),
                        SimpleOperatorFactory.of(operator),
                        transformation.getStateKeyType(),
                        transformation.getKeySelector(),
                        null /* no key selector on broadcast input */,
                        context);

        BatchExecutionUtils.applyBatchExecutionSettings(
                transformation.getId(),
                context,
                StreamConfig.InputRequirement.SORTED,
                StreamConfig.InputRequirement.PASS_THROUGH);

        return result;
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final KeyedBroadcastStateTransformation<KEY, IN1, IN2, OUT> transformation,
            final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        CoBroadcastWithKeyedOperator<KEY, IN1, IN2, OUT> operator =
                new CoBroadcastWithKeyedOperator<>(
                        transformation.getUserFunction(),
                        transformation.getBroadcastStateDescriptors());

        return translateInternal(
                transformation,
                transformation.getRegularInput(),
                transformation.getBroadcastInput(),
                SimpleOperatorFactory.of(operator),
                transformation.getStateKeyType(),
                transformation.getKeySelector(),
                null /* no key selector on broadcast input */,
                context);
    }
}
