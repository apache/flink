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

package org.apache.flink.streaming.api.operators.python.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.python.Constants.STATELESS_FUNCTION_URN;
import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/**
 * The {@link ExternalPythonCoProcessOperator} is responsible for executing the Python CoProcess
 * Function.
 *
 * @param <IN1> The input type of the first stream
 * @param <IN2> The input type of the second stream
 * @param <OUT> The output type of the CoProcess function
 */
@Internal
public class ExternalPythonCoProcessOperator<IN1, IN2, OUT>
        extends AbstractExternalTwoInputPythonFunctionOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 2L;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private transient long currentWatermark;

    public ExternalPythonCoProcessOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN1> inputTypeInfo1,
            TypeInformation<IN2> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, inputTypeInfo1, inputTypeInfo2, outputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.currentWatermark = Long.MIN_VALUE;
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
                        inBatchExecutionMode(getKeyedStateBackend()),
                        config.get(PYTHON_METRIC_ENABLED),
                        config.get(PYTHON_PROFILE_ENABLED),
                        getSideOutputTags().size() > 0,
                        config.get(STATE_CACHE_SIZE),
                        config.get(MAP_STATE_READ_CACHE_SIZE),
                        config.get(MAP_STATE_WRITE_CACHE_SIZE)),
                getFlinkMetricContainer(),
                null,
                getOperatorStateBackend(),
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
                null,
                createSideOutputCoderDescriptors());
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        processElement(true, element.getTimestamp(), currentWatermark, element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        processElement(false, element.getTimestamp(), currentWatermark, element.getValue());
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    @Override
    public <T> AbstractExternalDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new ExternalPythonCoProcessOperator<>(
                config,
                pythonFunctionInfo,
                getLeftInputType(),
                getRightInputType(),
                outputTypeInfo);
    }
}
