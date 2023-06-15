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

package org.apache.flink.table.runtime.operators.fusion;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;

/**
 * Base {@link MultipleInputStreamOperator} to handle multiple operator fusion codegen in table
 * module.
 */
public abstract class FusionStreamOperatorBase extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData>, InputSelectable, BoundedMultiInput {

    protected final StreamOperatorParameters parameters;

    public FusionStreamOperatorBase(
            StreamOperatorParameters<RowData> parameters, int numberOfInputs) {
        super(parameters, numberOfInputs);
        this.parameters = parameters;
    }

    public StreamTask<?, ?> getContainingTask() {
        return parameters.getContainingTask();
    }

    public long computeMemorySize(double operatorFraction) {
        final double memFraction =
                parameters
                        .getStreamConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR,
                                getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration(),
                                getRuntimeContext().getUserCodeClassLoader());
        return getContainingTask()
                .getEnvironment()
                .getMemoryManager()
                .computeMemorySize(memFraction * operatorFraction);
    }
}
