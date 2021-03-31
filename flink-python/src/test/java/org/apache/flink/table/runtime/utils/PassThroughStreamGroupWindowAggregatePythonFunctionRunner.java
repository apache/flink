/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.runtime.operators.python.aggregate.PassThroughPythonStreamGroupWindowAggregateOperator;
import org.apache.flink.table.runtime.runners.python.beam.BeamTableStatefulPythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;

import java.util.Map;

/**
 * A {@link PassThroughStreamGroupWindowAggregatePythonFunctionRunner} runner that help to test the
 * Python stream group window aggregate operators.
 */
public class PassThroughStreamGroupWindowAggregatePythonFunctionRunner
        extends BeamTableStatefulPythonFunctionRunner {

    private final PassThroughPythonStreamGroupWindowAggregateOperator operator;

    public PassThroughStreamGroupWindowAggregatePythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            RowType inputType,
            RowType outputType,
            String functionUrn,
            FlinkFnApi.UserDefinedAggregateFunctions userDefinedFunctions,
            String coderUrn,
            Map<String, String> jobOptions,
            FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend keyedStateBackend,
            TypeSerializer keySerializer,
            PassThroughPythonStreamGroupWindowAggregateOperator operator) {
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
                keyedStateBackend,
                keySerializer,
                null,
                null,
                0.0,
                FlinkFnApi.CoderParam.OutputMode.MULTIPLE);
        this.operator = operator;
    }

    @Override
    protected void startBundle() {
        super.startBundle();
        this.operator.setResultBuffer(resultBuffer);
        this.mainInputReceiver = input -> operator.processPythonElement(input.getValue());
    }

    @Override
    public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) {
        return PythonTestUtils.createMockJobBundleFactory();
    }
}
