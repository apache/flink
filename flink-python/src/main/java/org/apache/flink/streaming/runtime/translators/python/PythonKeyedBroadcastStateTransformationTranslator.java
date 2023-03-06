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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonBatchKeyedCoBroadcastProcessOperator;
import org.apache.flink.streaming.api.operators.python.embedded.EmbeddedPythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonBatchKeyedCoBroadcastProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.transformations.python.DelegateOperatorTransformation;
import org.apache.flink.streaming.api.transformations.python.PythonKeyedBroadcastStateTransformation;
import org.apache.flink.streaming.runtime.translators.AbstractTwoInputTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.BatchExecutionUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * A {@link org.apache.flink.streaming.api.graph.TransformationTranslator} that translates {@link
 * PythonKeyedBroadcastStateTransformation} into {@link ExternalPythonKeyedCoProcessOperator}/{@link
 * EmbeddedPythonKeyedCoProcessOperator} in streaming mode or {@link
 * ExternalPythonBatchKeyedCoBroadcastProcessOperator}/{@link
 * EmbeddedPythonBatchKeyedCoBroadcastProcessOperator} in batch mode.
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

        Configuration config = transformation.getConfiguration();

        AbstractPythonFunctionOperator<OUT> operator;

        if (config.get(PythonOptions.PYTHON_EXECUTION_MODE).equals("thread")) {
            operator =
                    new EmbeddedPythonBatchKeyedCoBroadcastProcessOperator<>(
                            transformation.getConfiguration(),
                            transformation.getDataStreamPythonFunctionInfo(),
                            transformation.getRegularInput().getOutputType(),
                            transformation.getBroadcastInput().getOutputType(),
                            transformation.getOutputType());
        } else {
            operator =
                    new ExternalPythonBatchKeyedCoBroadcastProcessOperator<>(
                            transformation.getConfiguration(),
                            transformation.getDataStreamPythonFunctionInfo(),
                            transformation.getRegularInput().getOutputType(),
                            transformation.getBroadcastInput().getOutputType(),
                            transformation.getOutputType());
        }

        DelegateOperatorTransformation.configureOperator(transformation, operator);

        Collection<Integer> result =
                translateInternal(
                        transformation,
                        transformation.getRegularInput(),
                        transformation.getBroadcastInput(),
                        SimpleOperatorFactory.of(operator),
                        transformation.getStateKeyType(),
                        transformation.getKeySelector(),
                        null,
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
            PythonKeyedBroadcastStateTransformation<OUT> transformation, Context context) {
        Preconditions.checkNotNull(transformation);
        Preconditions.checkNotNull(context);

        Configuration config = transformation.getConfiguration();

        AbstractPythonFunctionOperator<OUT> operator;

        if (config.get(PythonOptions.PYTHON_EXECUTION_MODE).equals("thread")) {
            operator =
                    new EmbeddedPythonKeyedCoProcessOperator<>(
                            transformation.getConfiguration(),
                            transformation.getDataStreamPythonFunctionInfo(),
                            transformation.getRegularInput().getOutputType(),
                            transformation.getBroadcastInput().getOutputType(),
                            transformation.getOutputType());

        } else {
            operator =
                    new ExternalPythonKeyedCoProcessOperator<>(
                            transformation.getConfiguration(),
                            transformation.getDataStreamPythonFunctionInfo(),
                            transformation.getRegularInput().getOutputType(),
                            transformation.getBroadcastInput().getOutputType(),
                            transformation.getOutputType());
        }

        DelegateOperatorTransformation.configureOperator(transformation, operator);

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
