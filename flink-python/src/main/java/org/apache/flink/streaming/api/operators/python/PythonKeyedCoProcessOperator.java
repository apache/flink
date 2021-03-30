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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
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
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.api.utils.input.KeyedTwoInputWithTimerRowFactory;
import org.apache.flink.streaming.api.utils.output.OutputWithTimerRowHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.util.Collections;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/** KeyedCoProcessOperator. */
public class PythonKeyedCoProcessOperator<OUT>
        extends TwoInputPythonFunctionOperator<Row, Row, Row, OUT>
        implements ResultTypeQueryable<OUT>, Triggerable<Row, VoidNamespace> {

    private static final String KEYED_CO_PROCESS_FUNCTION_URN =
            "flink:transform:keyed_process_function:v1";

    private static final String FLAT_MAP_CODER_URN = "flink:coder:flat_map:v1";

    /** The TypeInformation of current key. */
    private final TypeInformation<Row> keyTypeInfo;

    /** Serializer for current key. */
    private final TypeSerializer keyTypeSerializer;

    private final TypeInformation<OUT> outputTypeInfo;

    /** TimerService for current operator to register or fire timer. */
    private transient InternalTimerService<VoidNamespace> internalTimerService;

    private transient KeyedTwoInputWithTimerRowFactory runnerInputFactory;
    private transient OutputWithTimerRowHandler runnerOutputHandler;

    private transient Object keyForTimerService;

    public PythonKeyedCoProcessOperator(
            Configuration config,
            TypeInformation<Row> inputTypeInfo1,
            TypeInformation<Row> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo) {
        super(
                config,
                pythonFunctionInfo,
                FLAT_MAP_CODER_URN,
                KeyedTwoInputWithTimerRowFactory.getRunnerInputTypeInfo(
                        inputTypeInfo1, inputTypeInfo2, constructKeyTypeInfo(inputTypeInfo1)),
                OutputWithTimerRowHandler.getRunnerOutputTypeInfo(
                        outputTypeInfo, constructKeyTypeInfo(inputTypeInfo1)));
        this.keyTypeInfo = constructKeyTypeInfo(inputTypeInfo1);
        this.keyTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        keyTypeInfo);
        this.outputTypeInfo = outputTypeInfo;
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                getRunnerInputTypeInfo(),
                getRunnerOutputTypeInfo(),
                KEYED_CO_PROCESS_FUNCTION_URN,
                PythonOperatorUtils.getUserDefinedDataStreamStatefulFunctionProto(
                        getPythonFunctionInfo(),
                        getRuntimeContext(),
                        Collections.EMPTY_MAP,
                        keyTypeInfo,
                        inBatchExecutionMode(getKeyedStateBackend())),
                getCoderUrn(),
                getJobOptions(),
                getFlinkMetricContainer(),
                getKeyedStateBackend(),
                keyTypeSerializer,
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
                                        .asClassLoader()));
    }

    @Override
    public void open() throws Exception {
        this.internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
        this.runnerInputFactory = new KeyedTwoInputWithTimerRowFactory();
        this.runnerOutputHandler =
                new OutputWithTimerRowHandler(
                        getKeyedStateBackend(),
                        internalTimerService,
                        new TimestampedCollector<>(output),
                        this,
                        VoidNamespaceSerializer.INSTANCE);
        super.open();
    }

    @Override
    public void processElement1(StreamRecord<Row> element) throws Exception {
        processElement(true, element);
    }

    @Override
    public void processElement2(StreamRecord<Row> element) throws Exception {
        processElement(false, element);
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
            bufferedTimestamp.poll();
        } else {
            bais.setBuffer(rawResult, 0, length);
            Row runnerOutput = getRunnerOutputTypeSerializer().deserialize(baisWrapper);
            runnerOutputHandler.accept(runnerOutput, bufferedTimestamp.peek());
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return outputTypeInfo;
    }

    @Override
    public void onEventTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
        bufferedTimestamp.offer(timer.getTimestamp());
        processTimer(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
        bufferedTimestamp.offer(Long.MIN_VALUE);
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
        Row row =
                runnerInputFactory.fromTimer(
                        timeDomain,
                        timer.getTimestamp(),
                        internalTimerService.currentWatermark(),
                        timer.getKey(),
                        null);
        getRunnerInputTypeSerializer().serialize(row, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    private void processElement(boolean isLeft, StreamRecord<Row> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        Row row =
                runnerInputFactory.fromNormalData(
                        isLeft,
                        element.getTimestamp(),
                        internalTimerService.currentWatermark(),
                        element.getValue());
        getRunnerInputTypeSerializer().serialize(row, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    private static TypeInformation<Row> constructKeyTypeInfo(TypeInformation<Row> inputTypeInfo) {
        return new RowTypeInfo(((RowTypeInfo) inputTypeInfo).getTypeAt(0));
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
