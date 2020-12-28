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
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.getUserDefinedDataStreamFunctionProto;

/**
 * {@link TwoInputPythonFunctionOperator} is responsible for launching beam runner which will start
 * a python harness to execute two-input user defined python function.
 */
@Internal
public abstract class TwoInputPythonFunctionOperator<IN1, IN2, OUT>
        extends AbstractTwoInputPythonFunctionOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;

    private static final String DATASTREAM_STATELESS_FUNCTION_URN =
            "flink:transform:datastream_stateless_function:v1";

    /** The options used to configure the Python worker process. */
    private final Map<String, String> jobOptions;

    /** The TypeInformation of the first input. */
    private final TypeInformation<IN1> inputTypeInfo1;

    /** The TypeInformation of the second input. */
    private final TypeInformation<IN2> inputTypeInfo2;

    /** The TypeInformation of the output. */
    private final TypeInformation<OUT> outputTypeInfo;

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    // True if input1 and input2 are KeyedStream
    private final boolean isKeyedStream;

    /** The TypeInformation of python worker input data. */
    private transient TypeInformation<Row> runnerInputTypeInfo;

    private transient TypeInformation<Row> runnerOutputTypeInfo;

    /** The TypeSerializer of python worker input data. */
    private transient TypeSerializer<Row> runnerInputTypeSerializer;

    /** The TypeSerializer of the runner output. */
    transient TypeSerializer<Row> runnerOutputTypeSerializer;

    protected transient ByteArrayInputStreamWithPos bais;

    protected transient DataInputViewStreamWrapper baisWrapper;

    private transient ByteArrayOutputStreamWithPos baos;

    private transient DataOutputViewStreamWrapper baosWrapper;

    protected transient TimestampedCollector collector;

    private transient Row reuseRow;

    transient LinkedList<Long> bufferedTimestamp1;

    transient LinkedList<Long> bufferedTimestamp2;

    public TwoInputPythonFunctionOperator(
            Configuration config,
            TypeInformation<IN1> inputTypeInfo1,
            TypeInformation<IN2> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            boolean isKeyedStream) {
        super(config);
        this.jobOptions = config.toMap();
        this.inputTypeInfo1 = inputTypeInfo1;
        this.inputTypeInfo2 = inputTypeInfo2;
        this.outputTypeInfo = outputTypeInfo;
        this.pythonFunctionInfo = pythonFunctionInfo;
        this.isKeyedStream = isKeyedStream;
    }

    @Override
    public void open() throws Exception {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        bufferedTimestamp1 = new LinkedList<>();
        bufferedTimestamp2 = new LinkedList<>();
        // The row contains three field. The first field indicate left input or right input
        // The second field contains left input and the third field contains right input.
        runnerInputTypeInfo = getRunnerInputTypeInfo();
        runnerInputTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerInputTypeInfo);

        runnerOutputTypeInfo = Types.ROW(Types.BYTE, outputTypeInfo);
        runnerOutputTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerOutputTypeInfo);

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
                        pythonFunctionInfo, getRuntimeContext(), Collections.EMPTY_MAP),
                getFunctionUrn(),
                jobOptions,
                getFlinkMetricContainer(),
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
        bufferedTimestamp1.offer(element.getTimestamp());
        // construct combined row.
        reuseRow.setField(0, true);
        reuseRow.setField(1, getValue(element));
        reuseRow.setField(2, null); // need to set null since it is a reuse row.
        processElementInternal();
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        bufferedTimestamp2.offer(element.getTimestamp());
        // construct combined row.
        reuseRow.setField(0, false);
        reuseRow.setField(1, null); // need to set null since it is a reuse row.
        reuseRow.setField(2, getValue(element));
        processElementInternal();
    }

    public abstract String getFunctionUrn();

    private TypeInformation<Row> getRunnerInputTypeInfo() {
        if (isKeyedStream) {
            // since we wrap a keyed field for python KeyedStream, we need to extract the
            // corresponding data input type.
            return new RowTypeInfo(
                    Types.BOOLEAN,
                    ((RowTypeInfo) inputTypeInfo1).getTypeAt(1),
                    ((RowTypeInfo) inputTypeInfo2).getTypeAt(1));
        } else {
            return new RowTypeInfo(Types.BOOLEAN, inputTypeInfo1, inputTypeInfo2);
        }
    }

    private Object getValue(StreamRecord<?> element) {
        if (isKeyedStream) {
            // since we wrap a keyed field for python KeyedStream, we need to extract the
            // corresponding data input.
            return ((Row) element.getValue()).getField(1);
        } else {
            return element.getValue();
        }
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
