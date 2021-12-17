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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.python.Constants.STATELESS_FUNCTION_URN;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/**
 * {@link PythonProcessOperator} is responsible for launching beam runner which will start a python
 * harness to execute user defined python ProcessFunction.
 */
@Internal
public class PythonProcessOperator<IN, OUT>
        extends AbstractOneInputPythonFunctionOperator<IN, OUT> {

    private static final long serialVersionUID = 2L;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private transient long currentWatermark;

    public PythonProcessOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, inputTypeInfo, outputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        super.open();
        currentWatermark = Long.MIN_VALUE;
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                STATELESS_FUNCTION_URN,
                ProtoUtils.createUserDefinedDataStreamFunctionProtos(
                        getPythonFunctionInfo(),
                        getRuntimeContext(),
                        getInternalParameters(),
                        inBatchExecutionMode(getKeyedStateBackend())),
                jobOptions,
                getFlinkMetricContainer(),
                null,
                null,
                null,
                null,
                getContainingTask().getEnvironment().getMemoryManager(),
                getOperatorConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                getContainingTask()
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getUserCodeClassLoader()
                                        .asClassLoader()),
                createInputCoderInfoDescriptor(),
                createOutputCoderInfoDescriptor(),
                null);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        processElement(element.getTimestamp(), currentWatermark, element.getValue());
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    @Override
    public <T> AbstractDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new PythonProcessOperator<>(
                config, pythonFunctionInfo, getInputTypeInfo(), outputTypeInfo);
    }
}
