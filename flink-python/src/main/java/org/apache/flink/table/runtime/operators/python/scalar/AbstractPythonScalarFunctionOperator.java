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

package org.apache.flink.table.runtime.operators.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;

/**
 * Base class for all stream operators to execute Python {@link ScalarFunction}s. It executes the
 * Python {@link ScalarFunction}s in separate Python execution environment.
 *
 * <p>The inputs are assumed as the following format: {{{ +------------------+--------------+ |
 * forwarded fields | extra fields | +------------------+--------------+ }}}.
 *
 * <p>The Python UDFs may take input columns directly from the input row or the execution result of
 * Java UDFs: 1) The input columns from the input row can be referred from the 'forwarded fields';
 * 2) The Java UDFs will be computed and the execution results can be referred from the 'extra
 * fields'.
 *
 * <p>The outputs will be as the following format: {{{
 * +------------------+-------------------------+ | forwarded fields | scalar function results |
 * +------------------+-------------------------+ }}}.
 */
@Internal
public abstract class AbstractPythonScalarFunctionOperator
        extends AbstractStatelessFunctionOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final String SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1";

    /** The Python {@link ScalarFunction}s to be executed. */
    protected final PythonFunctionInfo[] scalarFunctions;

    private final GeneratedProjection udfInputGeneratedProjection;
    private final GeneratedProjection forwardedFieldGeneratedProjection;

    /** The collector used to collect records. */
    protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** The Projection which projects the forwarded fields from the input row. */
    private transient Projection<RowData, BinaryRowData> forwardedFieldProjection;

    /** The Projection which projects the udf input fields from the input row. */
    private transient Projection<RowData, BinaryRowData> udfInputProjection;

    /** The JoinedRowData reused holding the execution result. */
    protected transient JoinedRowData reuseJoinedRow;

    public AbstractPythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection udfInputGeneratedProjection,
            GeneratedProjection forwardedFieldGeneratedProjection) {
        super(config, inputType, udfInputType, udfOutputType);
        this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
        this.udfInputGeneratedProjection = Preconditions.checkNotNull(udfInputGeneratedProjection);
        this.forwardedFieldGeneratedProjection =
                Preconditions.checkNotNull(forwardedFieldGeneratedProjection);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open() throws Exception {
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseJoinedRow = new JoinedRowData();

        udfInputProjection =
                udfInputGeneratedProjection.newInstance(
                        Thread.currentThread().getContextClassLoader());
        forwardedFieldProjection =
                forwardedFieldGeneratedProjection.newInstance(
                        Thread.currentThread().getContextClassLoader());
    }

    @Override
    public PythonEnv getPythonEnv() {
        return scalarFunctions[0].getPythonFunction().getPythonEnv();
    }

    /** Gets the proto representation of the Python user-defined functions to be executed. */
    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        return ProtoUtils.createUserDefinedFunctionsProto(
                getRuntimeContext(),
                scalarFunctions,
                config.get(PYTHON_METRIC_ENABLED),
                config.get(PYTHON_PROFILE_ENABLED));
    }

    @Override
    public String getFunctionUrn() {
        return SCALAR_FUNCTION_URN;
    }

    @Override
    public void bufferInput(RowData input) {
        // always copy the projection result as the generated Projection reuses the projection
        // result
        RowData forwardedFields = forwardedFieldProjection.apply(input).copy();
        forwardedFields.setRowKind(input.getRowKind());
        forwardedInputQueue.add(forwardedFields);
    }

    @Override
    public RowData getFunctionInput(RowData element) {
        return udfInputProjection.apply(element);
    }
}
