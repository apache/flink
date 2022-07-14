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

package org.apache.flink.streaming.runtime.translators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonBatchCoBroadcastProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonCoProcessOperator;
import org.apache.flink.streaming.api.transformations.python.PythonBroadcastStateTransformation;
import org.apache.flink.streaming.runtime.translators.AbstractTwoInputTransformationTranslator;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * A {@link org.apache.flink.streaming.api.graph.TransformationTranslator} that translates {@link
 * PythonBroadcastStateTransformation} into {@link ExternalPythonCoProcessOperator} or {@link
 * ExternalPythonBatchCoBroadcastProcessOperator}.
 */
@Internal
public class PythonBroadcastStateTransformationTranslator<IN1, IN2, OUT>
        extends AbstractTwoInputTransformationTranslator<
                IN1, IN2, OUT, PythonBroadcastStateTransformation<IN1, IN2, OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            PythonBroadcastStateTransformation<IN1, IN2, OUT> transformation, Context context) {
        Preconditions.checkNotNull(transformation);
        Preconditions.checkNotNull(context);

        ExternalPythonBatchCoBroadcastProcessOperator operator =
                new ExternalPythonBatchCoBroadcastProcessOperator(
                        transformation.getConfiguration(),
                        transformation.getDataStreamPythonFunctionInfo(),
                        transformation.getRegularInput().getOutputType(),
                        transformation.getBroadcastInput().getOutputType(),
                        transformation.getOutputType());

        return translateInternal(
                transformation,
                transformation.getRegularInput(),
                transformation.getBroadcastInput(),
                SimpleOperatorFactory.of(operator),
                null,
                null,
                null,
                context);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            PythonBroadcastStateTransformation<IN1, IN2, OUT> transformation, Context context) {
        Preconditions.checkNotNull(transformation);
        Preconditions.checkNotNull(context);

        ExternalPythonCoProcessOperator<IN1, IN2, OUT> operator =
                new ExternalPythonCoProcessOperator<>(
                        transformation.getConfiguration(),
                        transformation.getDataStreamPythonFunctionInfo(),
                        transformation.getRegularInput().getOutputType(),
                        transformation.getBroadcastInput().getOutputType(),
                        transformation.getOutputType());

        return translateInternal(
                transformation,
                transformation.getRegularInput(),
                transformation.getBroadcastInput(),
                SimpleOperatorFactory.of(operator),
                null,
                null,
                null,
                context);
    }
}
