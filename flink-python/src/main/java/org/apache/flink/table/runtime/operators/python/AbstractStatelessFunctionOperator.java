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

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.runners.python.beam.BeamTablePythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Base class for all stream operators to execute Python Stateless Functions.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 */
@Internal
public abstract class AbstractStatelessFunctionOperator<IN, OUT, UDFIN>
        extends AbstractOneInputPythonFunctionOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    /** The input logical type. */
    protected final RowType inputType;

    /** The user-defined function input logical type. */
    protected final RowType udfInputType;

    /** The user-defined function output logical type. */
    protected final RowType udfOutputType;

    /**
     * The queue holding the input elements for which the execution results have not been received.
     */
    protected transient LinkedList<IN> forwardedInputQueue;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    protected transient ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    protected transient DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    protected transient ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    protected transient DataOutputViewStreamWrapper baosWrapper;

    public AbstractStatelessFunctionOperator(
            Configuration config, RowType inputType, RowType udfInputType, RowType udfOutputType) {
        super(config);
        this.inputType = Preconditions.checkNotNull(inputType);
        this.udfInputType = Preconditions.checkNotNull(udfInputType);
        this.udfOutputType = Preconditions.checkNotNull(udfOutputType);
    }

    @Override
    public void open() throws Exception {
        forwardedInputQueue = new LinkedList<>();
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);
        super.open();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        IN value = element.getValue();
        bufferInput(value);
        processElementInternal(value);
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws IOException {
        return BeamTablePythonFunctionRunner.stateless(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                getFunctionUrn(),
                createUserDefinedFunctionsProto(),
                getFlinkMetricContainer(),
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
                                        .asClassLoader()),
                createInputCoderInfoDescriptor(udfInputType),
                createOutputCoderInfoDescriptor(udfOutputType));
    }

    /**
     * Buffers the specified input, it will be used to construct the operator result together with
     * the user-defined function execution result.
     */
    public abstract void bufferInput(IN input) throws Exception;

    public abstract UDFIN getFunctionInput(IN element);

    /** Gets the proto representation of the Python user-defined functions to be executed. */
    public abstract FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto();

    public abstract String getFunctionUrn();

    public abstract FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(
            RowType runnerInputType);

    public abstract FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(
            RowType runnerOutType);

    public abstract void processElementInternal(IN value) throws Exception;
}
