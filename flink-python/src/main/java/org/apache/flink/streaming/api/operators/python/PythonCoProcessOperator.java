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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.util.LinkedList;

import static org.apache.flink.streaming.api.utils.ProtoUtils.createRawTypeCoderInfoDescriptorProto;

/**
 * The {@link PythonCoProcessOperator} is responsible for executing the Python CoProcess Function.
 *
 * @param <IN1> The input type of the first stream
 * @param <IN2> The input type of the second stream
 * @param <OUT> The output type of the CoProcess function
 */
@Internal
public class PythonCoProcessOperator<IN1, IN2, OUT>
        extends TwoInputPythonFunctionOperator<IN1, IN2, OUT, OUT> {

    private static final long serialVersionUID = 1L;

    private transient LinkedList<Long> bufferedTimestamp;

    private transient RunnerInputHandler runnerInputHandler;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private transient long currentWatermark;

    public PythonCoProcessOperator(
            Configuration config,
            TypeInformation<IN1> inputTypeInfo1,
            TypeInformation<IN2> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo) {
        super(
                config,
                pythonFunctionInfo,
                RunnerInputHandler.getRunnerInputTypeInfo(inputTypeInfo1, inputTypeInfo2),
                outputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.bufferedTimestamp = new LinkedList<>();
        this.runnerInputHandler = new RunnerInputHandler();
        this.currentWatermark = Long.MIN_VALUE;
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        processElement(true, element);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        processElement(false, element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    private void processElement(boolean isLeft, StreamRecord<?> element) throws Exception {
        Row row =
                runnerInputHandler.buildRunnerInputData(
                        isLeft, element.getTimestamp(), currentWatermark, element.getValue());
        getRunnerInputTypeSerializer().serialize(row, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
            bufferedTimestamp.poll();
        } else {
            bais.setBuffer(rawResult, 0, length);
            collector.setAbsoluteTimestamp(bufferedTimestamp.peek());
            OUT outputRow = getRunnerOutputTypeSerializer().deserialize(baisWrapper);
            collector.collect(outputRow);
        }
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(
            TypeInformation<?> runnerInputType) {
        return createRawTypeCoderInfoDescriptorProto(
                runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(
            TypeInformation<?> runnerOutType) {
        return createRawTypeCoderInfoDescriptorProto(
                runnerOutType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }
}
