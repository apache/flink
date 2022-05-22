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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.python.timer.TimerHandler;
import org.apache.flink.streaming.api.operators.python.timer.TimerRegistration;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import static org.apache.flink.python.Constants.STATEFUL_FUNCTION_URN;
import static org.apache.flink.streaming.api.operators.python.timer.TimerUtils.createTimerDataCoderInfoDescriptorProto;
import static org.apache.flink.streaming.api.operators.python.timer.TimerUtils.createTimerDataTypeInfo;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/** KeyedCoProcessOperator. */
@Internal
public class PythonKeyedCoProcessOperator<OUT>
        extends AbstractTwoInputPythonFunctionOperator<Row, Row, OUT>
        implements Triggerable<Row, VoidNamespace> {

    private static final long serialVersionUID = 2L;

    /** TimerService for current operator to register or fire timer. */
    private transient InternalTimerService<VoidNamespace> internalTimerService;

    /** The TypeInformation of the key. */
    private transient TypeInformation<Row> keyTypeInfo;

    /** The TypeSerializer of the key. */
    private transient TypeSerializer<Row> keyTypeSerializer;

    /** The TypeInformation of timer data. */
    private transient TypeInformation<Row> timerDataTypeInfo;

    /** The TypeSerializer of timer data. */
    private transient TypeSerializer<Row> timerDataSerializer;

    private transient TimerHandler timerHandler;

    private transient Object keyForTimerService;

    public PythonKeyedCoProcessOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<Row> inputTypeInfo1,
            TypeInformation<Row> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, inputTypeInfo1, inputTypeInfo2, outputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        keyTypeInfo = new RowTypeInfo(((RowTypeInfo) getLeftInputType()).getTypeAt(0));
        keyTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        keyTypeInfo);

        timerDataTypeInfo = createTimerDataTypeInfo(keyTypeInfo);
        timerDataSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        timerDataTypeInfo);

        timerHandler = new TimerHandler();

        super.open();
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                STATEFUL_FUNCTION_URN,
                ProtoUtils.createUserDefinedDataStreamStatefulFunctionProtos(
                        getPythonFunctionInfo(),
                        getRuntimeContext(),
                        getInternalParameters(),
                        keyTypeInfo,
                        inBatchExecutionMode(getKeyedStateBackend())),
                jobOptions,
                getFlinkMetricContainer(),
                getKeyedStateBackend(),
                keyTypeSerializer,
                null,
                new TimerRegistration(
                        getKeyedStateBackend(),
                        internalTimerService,
                        this,
                        VoidNamespaceSerializer.INSTANCE,
                        timerDataSerializer),
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
                createTimerDataCoderInfoDescriptorProto(timerDataTypeInfo));
    }

    @Override
    public void processElement1(StreamRecord<Row> element) throws Exception {
        processElement(
                true,
                element.getTimestamp(),
                internalTimerService.currentWatermark(),
                element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<Row> element) throws Exception {
        processElement(
                false,
                element.getTimestamp(),
                internalTimerService.currentWatermark(),
                element.getValue());
    }

    @Override
    public void onEventTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
        processTimer(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
        processTimer(TimeDomain.PROCESSING_TIME, timer);
    }

    /**
     * It is responsible to send timer data to python worker when a registered timer is fired. The
     * input data is a Row containing 4 fields: TimerFlag 0 for proc time, 1 for event time;
     * Timestamp of the fired timer; Current watermark and the key of the timer.
     *
     * @param timeDomain The type of the timer.
     * @param timer The fired timer.
     * @throws Exception The runnerInputSerializer might throw exception.
     */
    private void processTimer(TimeDomain timeDomain, InternalTimer<Row, VoidNamespace> timer)
            throws Exception {
        Row timerData =
                timerHandler.buildTimerData(
                        timeDomain,
                        internalTimerService.currentWatermark(),
                        timer.getTimestamp(),
                        timer.getKey(),
                        null);
        timerDataSerializer.serialize(timerData, baosWrapper);
        pythonFunctionRunner.processTimer(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    /**
     * As the beam state gRPC service will access the KeyedStateBackend in parallel with this
     * operator, we must override this method to prevent changing the current key of the
     * KeyedStateBackend while the beam service is handling requests.
     */
    @Override
    public void setCurrentKey(Object key) {
        if (inBatchExecutionMode(getKeyedStateBackend())) {
            super.setCurrentKey(key);
        }

        keyForTimerService = key;
    }

    @Override
    public Object getCurrentKey() {
        return keyForTimerService;
    }

    @Override
    public <T> AbstractDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new PythonKeyedCoProcessOperator<>(
                config,
                pythonFunctionInfo,
                getLeftInputType(),
                getRightInputType(),
                outputTypeInfo);
    }
}
