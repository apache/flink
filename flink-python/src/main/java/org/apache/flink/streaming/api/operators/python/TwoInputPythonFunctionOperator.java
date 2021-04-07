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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.getUserDefinedDataStreamFunctionProto;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;
import static org.apache.flink.streaming.api.utils.PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter;

/**
 * {@link TwoInputPythonFunctionOperator} is responsible for launching beam runner which will start
 * a python harness to execute two-input user defined python function.
 */
@Internal
public abstract class TwoInputPythonFunctionOperator<IN1, IN2, RUNNER_OUT, OUT>
        extends AbstractTwoInputPythonFunctionOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;

    private static final String DATASTREAM_STATELESS_FUNCTION_URN =
            "flink:transform:datastream_stateless_function:v1";

    /** The options used to configure the Python worker process. */
    private final Map<String, String> jobOptions;

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    /** The coder urn. */
    private final String coderUrn;

    /** The TypeInformation of python worker input data. */
    private final TypeInformation<Row> runnerInputTypeInfo;

    private final TypeInformation<RUNNER_OUT> runnerOutputTypeInfo;

    /** The TypeSerializer of python worker input data. */
    private final TypeSerializer<Row> runnerInputTypeSerializer;

    /** The TypeSerializer of the runner output. */
    private final TypeSerializer<RUNNER_OUT> runnerOutputTypeSerializer;

    protected transient ByteArrayInputStreamWithPos bais;

    protected transient DataInputViewStreamWrapper baisWrapper;

    protected transient ByteArrayOutputStreamWithPos baos;

    protected transient DataOutputViewStreamWrapper baosWrapper;

    protected transient TimestampedCollector collector;

    protected transient Row reuseRow;

    transient LinkedList<Long> bufferedTimestamp;

    public TwoInputPythonFunctionOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            String coderUrn,
            TypeInformation<Row> runnerInputTypeInfo,
            TypeInformation<RUNNER_OUT> runnerOutputTypeInfo) {
        super(config);
        this.jobOptions = config.toMap();
        this.pythonFunctionInfo = pythonFunctionInfo;
        this.coderUrn = coderUrn;
        this.runnerInputTypeInfo = runnerInputTypeInfo;
        this.runnerOutputTypeInfo = runnerOutputTypeInfo;
        this.runnerInputTypeSerializer = typeInfoSerializerConverter(runnerInputTypeInfo);
        this.runnerOutputTypeSerializer = typeInfoSerializerConverter(runnerOutputTypeInfo);
    }

    public TwoInputPythonFunctionOperator(
            Configuration config,
            TypeInformation<IN1> inputTypeInfo1,
            TypeInformation<IN2> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            String coderUrn) {
        this(
                config,
                pythonFunctionInfo,
                coderUrn,
                new RowTypeInfo(Types.BOOLEAN, inputTypeInfo1, inputTypeInfo2),
                (TypeInformation<RUNNER_OUT>) outputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        bufferedTimestamp = new LinkedList<>();

        collector = new TimestampedCollector(output);
        reuseRow = new Row(3);

        super.open();
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                runnerInputTypeInfo,
                runnerOutputTypeInfo,
                DATASTREAM_STATELESS_FUNCTION_URN,
                getUserDefinedDataStreamFunctionProto(
                        pythonFunctionInfo,
                        getRuntimeContext(),
                        Collections.EMPTY_MAP,
                        inBatchExecutionMode(getKeyedStateBackend())),
                coderUrn,
                jobOptions,
                getFlinkMetricContainer(),
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
                                        .asClassLoader()));
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonFunctionInfo.getPythonFunction().getPythonEnv();
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        // construct combined row.
        reuseRow.setField(0, true);
        reuseRow.setField(1, element.getValue());
        reuseRow.setField(2, null); // need to set null since it is a reuse row.
        processElementInternal();
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        // construct combined row.
        reuseRow.setField(0, false);
        reuseRow.setField(1, null); // need to set null since it is a reuse row.
        reuseRow.setField(2, element.getValue());
        processElementInternal();
    }

    protected Map<String, String> getJobOptions() {
        return jobOptions;
    }

    protected String getCoderUrn() {
        return coderUrn;
    }

    protected TypeInformation<Row> getRunnerInputTypeInfo() {
        return runnerInputTypeInfo;
    }

    protected TypeInformation<RUNNER_OUT> getRunnerOutputTypeInfo() {
        return runnerOutputTypeInfo;
    }

    protected DataStreamPythonFunctionInfo getPythonFunctionInfo() {
        return pythonFunctionInfo;
    }

    protected TypeSerializer<Row> getRunnerInputTypeSerializer() {
        return runnerInputTypeSerializer;
    }

    protected TypeSerializer<RUNNER_OUT> getRunnerOutputTypeSerializer() {
        return runnerOutputTypeSerializer;
    }

    private void processElementInternal() throws Exception {
        runnerInputTypeSerializer.serialize(reuseRow, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }
}
