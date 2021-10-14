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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.collector.RunnerOutputCollector;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.streaming.api.utils.ProtoUtils.createRawTypeCoderInfoDescriptorProto;
import static org.apache.flink.streaming.api.utils.PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter;

/**
 * {@link AbstractTwoInputPythonFunctionOperator} is responsible for launching beam runner which
 * will start a python harness to execute two-input user defined python function.
 */
@Internal
public abstract class AbstractTwoInputPythonFunctionOperator<IN1, IN2, OUT>
        extends AbstractDataStreamPythonFunctionOperator<OUT>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {

    private static final long serialVersionUID = 1L;

    /** The left input type. */
    private final TypeInformation<IN1> inputTypeInfo1;

    /** The right input type. */
    private final TypeInformation<IN2> inputTypeInfo2;

    /** The TypeInformation of python worker input data. */
    private transient TypeInformation<Row> runnerInputTypeInfo;

    private transient TypeInformation<Row> runnerOutputTypeInfo;

    /** The TypeSerializer of python worker input data. */
    private transient TypeSerializer<Row> runnerInputTypeSerializer;

    /** The TypeSerializer of the runner output. */
    private transient TypeSerializer<Row> runnerOutputTypeSerializer;

    private transient ByteArrayInputStreamWithPos bais;
    private transient DataInputViewStreamWrapper baisWrapper;
    protected transient ByteArrayOutputStreamWithPos baos;
    protected transient DataOutputViewStreamWrapper baosWrapper;

    private transient RunnerInputHandler runnerInputHandler;
    private transient RunnerOutputCollector<OUT> runnerOutputCollector;

    public AbstractTwoInputPythonFunctionOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN1> inputTypeInfo1,
            TypeInformation<IN2> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, outputTypeInfo);
        this.inputTypeInfo1 = Preconditions.checkNotNull(inputTypeInfo1);
        this.inputTypeInfo2 = Preconditions.checkNotNull(inputTypeInfo2);
    }

    @Override
    public void open() throws Exception {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        runnerInputTypeInfo =
                RunnerInputHandler.getRunnerInputTypeInfo(inputTypeInfo1, inputTypeInfo2);
        runnerOutputTypeInfo = RunnerOutputCollector.getRunnerOutputTypeInfo(getProducedType());
        runnerInputTypeSerializer = typeInfoSerializerConverter(runnerInputTypeInfo);
        runnerOutputTypeSerializer = typeInfoSerializerConverter(runnerOutputTypeInfo);

        runnerInputHandler = new RunnerInputHandler();
        runnerOutputCollector = new RunnerOutputCollector<>(new TimestampedCollector<>(output));

        super.open();
    }

    @Override
    public void endInput(int inputId) throws Exception {
        invokeFinishBundle();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(rawResult, 0, length);
        Row runnerOutput = runnerOutputTypeSerializer.deserialize(baisWrapper);
        runnerOutputCollector.collect(runnerOutput);
    }

    public void processElement(boolean isLeft, long timestamp, long watermark, Object element)
            throws Exception {
        Row row = runnerInputHandler.buildRunnerInputData(isLeft, timestamp, watermark, element);
        runnerInputTypeSerializer.serialize(row, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor() {
        return createRawTypeCoderInfoDescriptorProto(
                runnerInputTypeInfo, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor() {
        return createRawTypeCoderInfoDescriptorProto(
                runnerOutputTypeInfo, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    // ----------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------

    protected TypeInformation<IN1> getLeftInputType() {
        return inputTypeInfo1;
    }

    protected TypeInformation<IN2> getRightInputType() {
        return inputTypeInfo2;
    }

    private static final class RunnerInputHandler {

        private final Row reusableElementData;
        private final Row reusableRunnerInput;

        public RunnerInputHandler() {
            this.reusableElementData = new Row(3);
            this.reusableRunnerInput = new Row(3);
            this.reusableRunnerInput.setField(2, reusableElementData);
        }

        public Row buildRunnerInputData(
                boolean isLeft, long timestamp, long watermark, Object elementData) {
            reusableElementData.setField(0, isLeft);
            if (isLeft) {
                // The input row is a tuple of key and value.
                reusableElementData.setField(1, elementData);
                // need to set null since it is a reuse row.
                reusableElementData.setField(2, null);
            } else {
                // need to set null since it is a reuse row.
                reusableElementData.setField(1, null);
                // The input row is a tuple of key and value.
                reusableElementData.setField(2, elementData);
            }

            reusableRunnerInput.setField(0, timestamp);
            reusableRunnerInput.setField(1, watermark);
            return reusableRunnerInput;
        }

        public static TypeInformation<Row> getRunnerInputTypeInfo(
                TypeInformation<?> leftInputType, TypeInformation<?> rightInputType) {
            // structure: [timestamp, watermark, [isLeft, leftInput, rightInput]]
            return Types.ROW(
                    Types.LONG,
                    Types.LONG,
                    new RowTypeInfo(Types.BOOLEAN, leftInputType, rightInputType));
        }
    }
}
