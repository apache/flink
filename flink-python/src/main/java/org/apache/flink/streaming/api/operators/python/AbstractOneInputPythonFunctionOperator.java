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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.python.collector.RunnerOutputCollector;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.streaming.api.utils.ProtoUtils.createRawTypeCoderInfoDescriptorProto;

/**
 * {@link AbstractOneInputPythonFunctionOperator} is responsible for launching beam runner which
 * will start a python harness to execute user defined python function.
 *
 * <p>The operator will buffer the timestamp of input elements in a queue, and set into the produced
 * output element.
 */
@Internal
public abstract class AbstractOneInputPythonFunctionOperator<IN, OUT>
        extends AbstractDataStreamPythonFunctionOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** The TypeInformation of input data. */
    private final TypeInformation<IN> inputTypeInfo;

    /** The TypeInformation of runner input data. */
    private transient TypeInformation<Row> runnerInputTypeInfo;

    /** The TypeInformation of runner output data. */
    private transient TypeInformation<Row> runnerOutputTypeInfo;

    /** The TypeSerializer of python worker input data. */
    private transient TypeSerializer<Row> runnerInputTypeSerializer;

    /** The TypeSerializer of python worker output data. */
    private transient TypeSerializer<Row> runnerOutputTypeSerializer;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    private transient ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private transient DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    protected transient ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    protected transient DataOutputViewStreamWrapper baosWrapper;

    private transient RunnerInputHandler runnerInputHandler;

    private transient RunnerOutputCollector<OUT> runnerOutputCollector;

    public AbstractOneInputPythonFunctionOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, outputTypeInfo);
        this.inputTypeInfo = Preconditions.checkNotNull(inputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        runnerInputTypeInfo = RunnerInputHandler.getRunnerInputTypeInfo(inputTypeInfo);
        runnerOutputTypeInfo = RunnerOutputCollector.getRunnerOutputTypeInfo(getProducedType());
        runnerInputTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerInputTypeInfo);
        runnerOutputTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        runnerOutputTypeInfo);

        runnerInputHandler = new RunnerInputHandler();
        runnerOutputCollector = new RunnerOutputCollector<>(new TimestampedCollector<>(output));

        super.open();
    }

    @Override
    public void endInput() throws Exception {
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

    public void processElement(long timestamp, long watermark, Object element) throws Exception {
        Row row = runnerInputHandler.buildRunnerInputData(timestamp, watermark, element);
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

    public TypeInformation<IN> getInputTypeInfo() {
        return inputTypeInfo;
    }

    private static final class RunnerInputHandler {

        /** Reusable row for element data. */
        private final Row reusableRunnerInput;

        public RunnerInputHandler() {
            this.reusableRunnerInput = new Row(3);
        }

        public Row buildRunnerInputData(long timestamp, long watermark, Object elementData) {
            reusableRunnerInput.setField(0, timestamp);
            reusableRunnerInput.setField(1, watermark);
            reusableRunnerInput.setField(2, elementData);
            return reusableRunnerInput;
        }

        public static TypeInformation<Row> getRunnerInputTypeInfo(
                TypeInformation<?> elementDataTypeInfo) {
            // structure: [timestamp, watermark, data]
            return Types.ROW(Types.LONG, Types.LONG, elementDataTypeInfo);
        }
    }
}
