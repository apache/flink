/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link TimestampsAndWatermarksTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to
 *     translate.
 */
@Internal
public class TimestampsAndWatermarksTransformationTranslator<IN>
        extends AbstractOneInputTransformationTranslator<
                IN, IN, TimestampsAndWatermarksTransformation<IN>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final TimestampsAndWatermarksTransformation<IN> transformation, final Context context) {
        return translateInternal(
                transformation, context, false /* don't emit progressive watermarks */);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final TimestampsAndWatermarksTransformation<IN> transformation, final Context context) {
        return translateInternal(transformation, context, true /* emit progressive watermarks */);
    }

    private Collection<Integer> translateInternal(
            final TimestampsAndWatermarksTransformation<IN> transformation,
            final Context context,
            boolean emitProgressiveWatermarks) {
        checkNotNull(transformation);
        checkNotNull(context);

        TimestampsAndWatermarksOperator<IN> operator =
                new TimestampsAndWatermarksOperator<>(
                        transformation.getWatermarkStrategy(), emitProgressiveWatermarks);
        SimpleOperatorFactory<IN> operatorFactory = SimpleOperatorFactory.of(operator);
        operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
        return translateInternal(
                transformation,
                operatorFactory,
                transformation.getInputType(),
                null,
                null,
                context);
    }
}
