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
import org.apache.flink.streaming.api.operators.python.PythonBatchKeyedCoBroadcastProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.transformations.python.PythonKeyedBroadcastStateTransformation;
import org.apache.flink.streaming.runtime.translators.AbstractTwoInputTransformationTranslator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * A {@link org.apache.flink.streaming.api.graph.TransformationTranslator} that translates {@link
 * PythonKeyedBroadcastStateTransformation} into {@link PythonKeyedCoProcessOperator} or {@link
 * PythonBatchKeyedCoBroadcastProcessOperator}.
 */
@Internal
public class PythonKeyedBroadcastStateTransformationTranslator<OUT>
        extends AbstractTwoInputTransformationTranslator<
                Row, Row, OUT, PythonKeyedBroadcastStateTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            PythonKeyedBroadcastStateTransformation<OUT> transformation, Context context) {
        Preconditions.checkNotNull(transformation);
        Preconditions.checkNotNull(context);

        PythonKeyedCoProcessOperator<OUT> operator =
                new PythonBatchKeyedCoBroadcastProcessOperator<>(
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
                transformation.getStateKeyType(),
                transformation.getKeySelector(),
                null,
                context);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            PythonKeyedBroadcastStateTransformation<OUT> transformation, Context context) {
        Preconditions.checkNotNull(transformation);
        Preconditions.checkNotNull(context);

        PythonKeyedCoProcessOperator<OUT> operator =
                new PythonKeyedCoProcessOperator<>(
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
                transformation.getStateKeyType(),
                transformation.getKeySelector(),
                null,
                context);
    }
}
