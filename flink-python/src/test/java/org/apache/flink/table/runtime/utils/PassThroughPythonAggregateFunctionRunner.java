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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.process.FlinkMetricContainer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.runners.python.beam.BeamTablePythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.Struct;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.python.Constants.OUTPUT_COLLECTION_ID;
import static org.apache.flink.python.util.ProtoUtils.createArrowTypeCoderInfoDescriptorProto;

/**
 * A {@link PassThroughPythonAggregateFunctionRunner} runner that just return the first input
 * element with the same key as the execution results.
 */
public class PassThroughPythonAggregateFunctionRunner extends BeamTablePythonFunctionRunner {

    private static final IntSerializer windowBoundarySerializer = IntSerializer.INSTANCE;

    private final List<byte[]> buffer;

    private final ArrowSerializer arrowSerializer;

    /** Whether it is batch over window. */
    private final boolean isBatchOverWindow;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    private transient ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private transient DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    private transient ByteArrayOutputStreamWithPos baos;

    public PassThroughPythonAggregateFunctionRunner(
            String taskName,
            ProcessPythonEnvironmentManager environmentManager,
            RowType inputType,
            RowType outputType,
            String functionUrn,
            FlinkFnApi.UserDefinedFunctions userDefinedFunctions,
            FlinkMetricContainer flinkMetricContainer,
            boolean isBatchOverWindow) {
        super(
                taskName,
                environmentManager,
                functionUrn,
                userDefinedFunctions,
                flinkMetricContainer,
                null,
                null,
                null,
                null,
                0.0,
                createArrowTypeCoderInfoDescriptorProto(
                        inputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false),
                createArrowTypeCoderInfoDescriptorProto(
                        outputType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false));
        this.buffer = new LinkedList<>();
        this.isBatchOverWindow = isBatchOverWindow;
        arrowSerializer = new ArrowSerializer(inputType, outputType);
    }

    @Override
    public void open(ReadableConfig config) throws Exception {
        super.open(config);
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        arrowSerializer.open(bais, baos);
    }

    @Override
    protected void startBundle() {
        super.startBundle();
        this.mainInputReceiver =
                input -> {
                    byte[] data = input.getValue();
                    bais.setBuffer(data, 0, data.length);
                    if (isBatchOverWindow) {
                        int windowSize = windowBoundarySerializer.deserialize(baisWrapper);
                        List<Integer> lowerBoundarys = new ArrayList<>();
                        for (int i = 0; i < windowSize; i++) {
                            int windowLength = windowBoundarySerializer.deserialize(baisWrapper);
                            for (int j = 0; j < windowLength; j++) {
                                if (j % 2 == 0) {
                                    lowerBoundarys.add(
                                            windowBoundarySerializer.deserialize(baisWrapper));
                                } else {
                                    windowBoundarySerializer.deserialize(baisWrapper);
                                }
                            }
                        }
                        arrowSerializer.load();
                        for (Integer lowerBoundary : lowerBoundarys) {
                            RowData firstData = arrowSerializer.read(lowerBoundary);
                            arrowSerializer.write(firstData);
                        }
                        arrowSerializer.resetReader();
                    } else {
                        arrowSerializer.load();
                        arrowSerializer.write(arrowSerializer.read(0));
                        arrowSerializer.resetReader();
                    }
                    arrowSerializer.finishCurrentBatch();
                    buffer.add(baos.toByteArray());
                    baos.reset();
                    arrowSerializer.resetWriter();
                };
    }

    @Override
    public void flush() throws Exception {
        super.flush();
        resultBuffer.addAll(
                buffer.stream()
                        .map(b -> Tuple2.of(OUTPUT_COLLECTION_ID, b))
                        .collect(Collectors.toList()));
        buffer.clear();
    }

    @Override
    public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) {
        return PythonTestUtils.createMockJobBundleFactory();
    }
}
