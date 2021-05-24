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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** A {@link MultipleInputStreamOperatorBase} to handle batch operators. */
public class BatchMultipleInputStreamOperator extends MultipleInputStreamOperatorBase
        implements BoundedMultiInput, InputSelectable {
    private static final long serialVersionUID = 1L;

    private final InputSelectionHandler inputSelectionHandler;

    public BatchMultipleInputStreamOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InputSpec> inputSpecs,
            List<TableOperatorWrapper<?>> headWrapper,
            TableOperatorWrapper<?> tailWrapper) {
        super(parameters, inputSpecs, headWrapper, tailWrapper);
        inputSelectionHandler = new InputSelectionHandler(inputSpecs);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputSelectionHandler.endInput(inputId);
        InputSpec inputSpec = inputSpecMap.get(inputId);
        inputSpec.getOutput().endOperatorInput(inputSpec.getOutputOpInputId());
    }

    @Override
    public InputSelection nextSelection() {
        return inputSelectionHandler.getInputSelection();
    }

    protected StreamConfig createStreamConfig(
            StreamOperatorParameters<RowData> multipleInputOperatorParameters,
            TableOperatorWrapper<?> wrapper) {
        StreamConfig streamConfig =
                super.createStreamConfig(multipleInputOperatorParameters, wrapper);
        checkState(wrapper.getManagedMemoryFraction() >= 0);
        Configuration taskManagerConfig =
                getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration();
        double managedMemoryFraction =
                multipleInputOperatorParameters
                                .getStreamConfig()
                                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        taskManagerConfig,
                                        getRuntimeContext().getUserCodeClassLoader())
                        * wrapper.getManagedMemoryFraction();
        streamConfig.setManagedMemoryFractionOperatorOfUseCase(
                ManagedMemoryUseCase.OPERATOR, managedMemoryFraction);
        return streamConfig;
    }
}
