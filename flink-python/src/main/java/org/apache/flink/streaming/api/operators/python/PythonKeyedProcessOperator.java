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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.api.utils.input.KeyedInputWithTimerRowFactory;
import org.apache.flink.streaming.api.utils.output.OutputWithTimerRowHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

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

    private static final String KEYED_PROCESS_FUNCTION_URN =
            "flink:transform:keyed_process_function:v1";

    private static final String FLAT_MAP_CODER_URN = "flink:coder:flat_map:v1";

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

    /** Serializer to serialize input data for python worker. */
    private transient TypeSerializer runnerInputSerializer;

    /** Serializer to deserialize output data from python worker. */
    private transient TypeSerializer runnerOutputSerializer;

    /** Serializer for current key. */
    private transient TypeSerializer keyTypeSerializer;

    /** TimerService for current operator to register or fire timer. */
    private transient InternalTimerService internalTimerService;

    private transient LinkedList<Long> bufferedTimestamp;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    private transient ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private transient DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    private transient ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    private transient DataOutputViewStreamWrapper baosWrapper;

    private transient KeyedInputWithTimerRowFactory runnerInputFactory;
    private transient OutputWithTimerRowHandler runnerOutputHandler;

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
        runnerInputTypeInfo =
                KeyedInputWithTimerRowFactory.getRunnerInputTypeInfo(inputTypeInfo, keyTypeInfo);
        runnerOutputTypeInfo =
                OutputWithTimerRowHandler.getRunnerOutputTypeInfo(outputTypeInfo, keyTypeInfo);
        keyTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        keyTypeInfo);
        runnerInputSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerInputTypeInfo);
        runnerOutputSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerOutputTypeInfo);

        internalTimerService = getInternalTimerService("user-timers", namespaceSerializer, this);
        this.bufferedTimestamp = new LinkedList<>();

        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);
        runnerInputFactory = new KeyedInputWithTimerRowFactory();
        runnerOutputHandler =
                new OutputWithTimerRowHandler(
                        getKeyedStateBackend(),
                        internalTimerService,
                        new TimestampedCollector<>(output),
                        this,
                        this.namespaceSerializer);

        super.open();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return this.outputTypeInfo;
    }

    @Override
    public void onEventTime(InternalTimer<Row, Object> timer) throws Exception {
        bufferedTimestamp.offer(timer.getTimestamp());
        processTimer(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<Row, Object> timer) throws Exception {
        bufferedTimestamp.offer(Long.MIN_VALUE);
        processTimer(TimeDomain.PROCESSING_TIME, timer);
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                runnerInputTypeInfo,
                runnerOutputTypeInfo,
                KEYED_PROCESS_FUNCTION_URN,
                PythonOperatorUtils.getUserDefinedDataStreamStatefulFunctionProto(
                        pythonFunctionInfo,
                        getRuntimeContext(),
                        Collections.EMPTY_MAP,
                        keyTypeInfo,
                        inBatchExecutionMode(getKeyedStateBackend())),
                FLAT_MAP_CODER_URN,
                jobOptions,
                getFlinkMetricContainer(),
                getKeyedStateBackend(),
                keyTypeSerializer,
                namespaceSerializer,
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
                                        .asClassLoader()));
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonFunctionInfo.getPythonFunction().getPythonEnv();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
            bufferedTimestamp.poll();
        } else {
            bais.setBuffer(rawResult, 0, length);
            Row runnerOutput = (Row) runnerOutputSerializer.deserialize(baisWrapper);
            runnerOutputHandler.accept(runnerOutput, bufferedTimestamp.peek());
        }
    }

    @Override
    public void processElement(StreamRecord<Row> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        Row row =
                runnerInputFactory.fromNormalData(
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
        long time = timer.getTimestamp();
        Row timerKey = timer.getKey();
        Object namespace = timer.getNamespace();
        byte[] encodedNamespace;
        if (VoidNamespace.INSTANCE.equals(namespace)) {
            encodedNamespace = null;
        } else {
            namespaceSerializer.serialize(namespace, baosWrapper);
            encodedNamespace = baos.toByteArray();
            baos.reset();
        }
        Row row =
                runnerInputFactory.fromTimer(
                        timeDomain,
                        time,
                        internalTimerService.currentWatermark(),
                        timerKey,
                        encodedNamespace);
        runnerInputSerializer.serialize(row, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
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
}
