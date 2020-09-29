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
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.PythonOperatorUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * Base class for all stream operators to execute Python {@link ScalarFunction}s. It executes the Python
 * {@link ScalarFunction}s in separate Python execution environment.
 *
 * <p>The inputs are assumed as the following format:
 * {{{
 *   +------------------+--------------+
 *   | forwarded fields | extra fields |
 *   +------------------+--------------+
 * }}}.
 *
 * <p>The Python UDFs may take input columns directly from the input row or the execution result of Java UDFs:
 * 1) The input columns from the input row can be referred from the 'forwarded fields';
 * 2) The Java UDFs will be computed and the execution results can be referred from the 'extra fields'.
 *
 * <p>The outputs will be as the following format:
 * {{{
 *   +------------------+-------------------------+
 *   | forwarded fields | scalar function results |
 *   +------------------+-------------------------+
 * }}}.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 */
@Internal
public abstract class AbstractPythonScalarFunctionOperator<IN, OUT, UDFIN>
		extends AbstractStatelessFunctionOperator<IN, OUT, UDFIN> {

	private static final long serialVersionUID = 1L;

	private static final String SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1";

	private static final String SCALAR_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:scalar_function:v1";

	/**
	 * The Python {@link ScalarFunction}s to be executed.
	 */
	protected final PythonFunctionInfo[] scalarFunctions;

	/**
	 * The offset of the fields which should be forwarded.
	 */
	protected final int[] forwardedFields;

	AbstractPythonScalarFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, inputType, outputType, udfInputOffsets);
		this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
		this.forwardedFields = Preconditions.checkNotNull(forwardedFields);
	}

	@Override
	public void open() throws Exception {
		userDefinedFunctionOutputType = new RowType(
			outputType.getFields().subList(forwardedFields.length, outputType.getFieldCount()));
		super.open();
	}

	@Override
	public PythonEnv getPythonEnv() {
		return scalarFunctions[0].getPythonFunction().getPythonEnv();
	}

	/**
	 * Gets the proto representation of the Python user-defined functions to be executed.
	 */
	@Override
	public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		// add udf proto
		for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
			builder.addUdfs(PythonOperatorUtils.getUserDefinedFunctionProto(pythonFunctionInfo));
		}
		builder.setMetricEnabled(getPythonConfig().isMetricEnabled());
		return builder.build();
	}

	@Override
	public String getFunctionUrn() {
		return SCALAR_FUNCTION_URN;
	}

	@Override
	public String getInputOutputCoderUrn() {
		return SCALAR_FUNCTION_SCHEMA_CODER_URN;
	}
}
