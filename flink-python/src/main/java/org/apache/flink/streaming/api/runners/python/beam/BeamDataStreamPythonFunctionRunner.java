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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * {@link BeamDataStreamPythonFunctionRunner} is responsible for starting a beam python harness to
 * execute user defined python function.
 */
@Internal
public class BeamDataStreamPythonFunctionRunner extends BeamPythonFunctionRunner {

    private final FlinkFnApi.UserDefinedDataStreamFunction userDefinedDataStreamFunction;

    public BeamDataStreamPythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            String functionUrn,
            FlinkFnApi.UserDefinedDataStreamFunction userDefinedDataStreamFunction,
            Map<String, String> jobOptions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend stateBackend,
            TypeSerializer keySerializer,
            TypeSerializer namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor) {
        super(
                taskName,
                environmentManager,
                functionUrn,
                jobOptions,
                flinkMetricContainer,
                stateBackend,
                keySerializer,
                namespaceSerializer,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor);
        this.userDefinedDataStreamFunction = userDefinedDataStreamFunction;
    }

    @Override
    protected byte[] getUserDefinedFunctionsProtoBytes() {
        return this.userDefinedDataStreamFunction.toByteArray();
    }
}
