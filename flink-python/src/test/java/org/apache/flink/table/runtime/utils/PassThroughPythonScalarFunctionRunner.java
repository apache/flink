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

import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.runtime.runners.python.beam.BeamTableStatelessPythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A {@link PassThroughPythonScalarFunctionRunner} runner that just return the input elements as the
 * execution results.
 */
public class PassThroughPythonScalarFunctionRunner extends BeamTableStatelessPythonFunctionRunner {

    private final List<byte[]> buffer;

    public PassThroughPythonScalarFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            RowType inputType,
            RowType outputType,
            String functionUrn,
            FlinkFnApi.UserDefinedFunctions userDefinedFunctions,
            String coderUrn,
            Map<String, String> jobOptions,
            FlinkMetricContainer flinkMetricContainer) {
        super(
                taskName,
                environmentManager,
                inputType,
                outputType,
                functionUrn,
                userDefinedFunctions,
                coderUrn,
                jobOptions,
                flinkMetricContainer,
                null,
                0.0,
                FlinkFnApi.CoderParam.OutputMode.SINGLE);
        this.buffer = new LinkedList<>();
    }

    @Override
    protected void startBundle() {
        super.startBundle();
        this.mainInputReceiver = input -> buffer.add(input.getValue());
    }

    @Override
    public void flush() throws Exception {
        super.flush();
        resultBuffer.addAll(buffer);
        buffer.clear();
    }

    @Override
    public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) {
        return PythonTestUtils.createMockJobBundleFactory();
    }
}
