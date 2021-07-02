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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.python.collector.RunnerOutputCollector;
import org.apache.flink.streaming.api.operators.python.timer.TimerHandler;
import org.apache.flink.streaming.api.operators.python.timer.TimerRegistration;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.python.Constants.STATEFUL_FUNCTION_URN;
import static org.apache.flink.streaming.api.operators.python.timer.TimerUtils.createTimerDataCoderInfoDescriptorProto;
import static org.apache.flink.streaming.api.operators.python.timer.TimerUtils.createTimerDataTypeInfo;
import static org.apache.flink.streaming.api.utils.ProtoUtils.createRawTypeCoderInfoDescriptorProto;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/**
 * {@link PythonKeyedProcessOperator} is responsible for launching beam runner which will start a
 * python harness to execute user defined python function. It is also able to handle the timer and
 * state request from the python stateful user defined function.
 */
@Internal
public class PythonKeyedProcessOperator<OUT>
        extends AbstractOneInputPythonFunctionOperator<Row, OUT>
        implements ResultTypeQueryable<OUT>, Triggerable<Row, Object> {

    private static final long serialVersionUID = 1L;

    /** The options used to configure the Python worker process. */
    private final Map<String, String> jobOptions;

    /** The TypeInformation of input data. */
    private final TypeInformation<Row> inputTypeInfo;

    /** The TypeInformation of output data. */
    private final TypeInformation<OUT> outputTypeInfo;

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    private final TypeSerializer namespaceSerializer;

    /** The TypeInformation of current key. */
    private transient TypeInformation<Row> keyTypeInfo;

    /** The TypeInformation of runner input data. */
    private transient TypeInformation<Row> runnerInputTypeInfo;

    /** The TypeInformation of runner output data. */
    private transient TypeInformation<Row> runnerOutputTypeInfo;

    /** The TypeInformation of timer data. */
    private transient TypeInformation<Row> timerDataTypeInfo;

    /** Serializer to serialize input data for python worker. */
    private transient TypeSerializer<Row> runnerInputSerializer;

    /** Serializer to deserialize output data from python worker. */
    private transient TypeSerializer<Row> runnerOutputSerializer;

    private transient TypeSerializer<Row> timerDataSerializer;

    /** Serializer for current key. */
    private transient TypeSerializer<Row> keyTypeSerializer;

    /** TimerService for current operator to register or fire timer. */
    private transient InternalTimerService internalTimerService;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    private transient ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private transient DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    private transient ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    private transient DataOutputViewStreamWrapper baosWrapper;

    private transient RunnerInputHandler runnerInputHandler;
    private transient RunnerOutputCollector<OUT> runnerOutputCollector;
    private transient TimerHandler timerHandler;

    private transient Object keyForTimerService;

    public PythonKeyedProcessOperator(
            Configuration config,
            RowTypeInfo inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo) {
        this(
                config,
                inputTypeInfo,
                outputTypeInfo,
                pythonFunctionInfo,
                VoidNamespaceSerializer.INSTANCE);
    }

    public PythonKeyedProcessOperator(
            Configuration config,
            RowTypeInfo inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeSerializer namespaceSerializer) {
        super(config);
        this.jobOptions = config.toMap();
        this.inputTypeInfo = inputTypeInfo;
        this.outputTypeInfo = outputTypeInfo;
        this.pythonFunctionInfo = pythonFunctionInfo;
        this.namespaceSerializer = namespaceSerializer;
    }

    @Override
    public void open() throws Exception {
        keyTypeInfo = new RowTypeInfo(((RowTypeInfo) this.inputTypeInfo).getTypeAt(0));
        runnerInputTypeInfo = RunnerInputHandler.getRunnerInputTypeInfo(inputTypeInfo);
        runnerOutputTypeInfo = RunnerOutputCollector.getRunnerOutputTypeInfo(outputTypeInfo);
        timerDataTypeInfo = createTimerDataTypeInfo(keyTypeInfo);
        keyTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        keyTypeInfo);
        runnerInputSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerInputTypeInfo);
        runnerOutputSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerOutputTypeInfo);
        timerDataSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        timerDataTypeInfo);

        internalTimerService = getInternalTimerService("user-timers", namespaceSerializer, this);

        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        runnerInputHandler = new RunnerInputHandler();
        runnerOutputCollector = new RunnerOutputCollector<>(new TimestampedCollector<>(output));
        timerHandler = new TimerHandler();

        super.open();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return this.outputTypeInfo;
    }

    @Override
    public void onEventTime(InternalTimer<Row, Object> timer) throws Exception {
        processTimer(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<Row, Object> timer) throws Exception {
        processTimer(TimeDomain.PROCESSING_TIME, timer);
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                STATEFUL_FUNCTION_URN,
                ProtoUtils.getUserDefinedDataStreamStatefulFunctionProto(
                        pythonFunctionInfo,
                        getRuntimeContext(),
                        Collections.emptyMap(),
                        keyTypeInfo,
                        inBatchExecutionMode(getKeyedStateBackend())),
                jobOptions,
                getFlinkMetricContainer(),
                getKeyedStateBackend(),
                keyTypeSerializer,
                namespaceSerializer,
                new TimerRegistration(
                        getKeyedStateBackend(),
                        internalTimerService,
                        this,
                        namespaceSerializer,
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
                createRawTypeCoderInfoDescriptorProto(
                        runnerInputTypeInfo, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false),
                createRawTypeCoderInfoDescriptorProto(
                        runnerOutputTypeInfo, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false),
                createTimerDataCoderInfoDescriptorProto(timerDataTypeInfo));
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonFunctionInfo.getPythonFunction().getPythonEnv();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(rawResult, 0, length);
        Row runnerOutput = runnerOutputSerializer.deserialize(baisWrapper);
        runnerOutputCollector.collect(runnerOutput);
    }

    @Override
    public void processElement(StreamRecord<Row> element) throws Exception {
        Row row =
                runnerInputHandler.buildRunnerInputData(
                        element.getTimestamp(),
                        internalTimerService.currentWatermark(),
                        element.getValue());
        runnerInputSerializer.serialize(row, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    /**
     * It is responsible to send timer data to python worker when a registered timer is fired. The
     * input data is a Row containing 4 fields: TimerFlag 0 for proc time, 1 for event time;
     * Timestamp of the fired timer; Current watermark and the key of the timer.
     *
     * @param timeDomain The type of the timer.
     * @param timer The internal timer.
     * @throws Exception The runnerInputSerializer might throw exception.
     */
    private void processTimer(TimeDomain timeDomain, InternalTimer<Row, Object> timer)
            throws Exception {
        Object namespace = timer.getNamespace();
        byte[] encodedNamespace;
        if (VoidNamespace.INSTANCE.equals(namespace)) {
            encodedNamespace = null;
        } else {
            namespaceSerializer.serialize(namespace, baosWrapper);
            encodedNamespace = baos.toByteArray();
            baos.reset();
        }
        Row timerData =
                timerHandler.buildTimerData(
                        timeDomain,
                        internalTimerService.currentWatermark(),
                        timer.getTimestamp(),
                        timer.getKey(),
                        encodedNamespace);
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

    private static final class RunnerInputHandler {

        /** Reusable row for element data. */
        private final Row reusableRunnerInput;

        public RunnerInputHandler() {
            this.reusableRunnerInput = new Row(3);
        }

        public Row buildRunnerInputData(long timestamp, long watermark, Row elementData) {
            reusableRunnerInput.setField(0, timestamp);
            reusableRunnerInput.setField(1, watermark);
            reusableRunnerInput.setField(2, elementData);
            return reusableRunnerInput;
        }

        public static TypeInformation<Row> getRunnerInputTypeInfo(
                TypeInformation<Row> elementDataTypeInfo) {
            // structure: [timestamp, watermark, data]
            return Types.ROW(Types.LONG, Types.LONG, elementDataTypeInfo);
        }
    }
}
