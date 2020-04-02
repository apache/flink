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

package org.apache.flink.table.runtime.runners.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.AbstractPythonStatelessFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.Map;

/**
 * Abstract {@link PythonFunctionRunner} used to execute Python {@link TableFunction}.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractPythonTableFunctionRunner<IN> extends AbstractPythonStatelessFunctionRunner<IN> {

	private static final String TABLE_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:table_function:v1";
	private static final String TABLE_FUNCTION_URN = "flink:transform:table_function:v1";

	private final PythonFunctionInfo tableFunction;

	/**
	 * The TypeSerializer for input elements.
	 */
	private transient TypeSerializer<IN> inputTypeSerializer;

	public AbstractPythonTableFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo tableFunction,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, environmentManager, inputType, outputType, TABLE_FUNCTION_URN, jobOptions, flinkMetricContainer);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
	}

	@Override
	public void open() throws Exception {
		super.open();
		inputTypeSerializer = getInputTypeSerializer();
	}

	/**
	 * Gets the proto representation of the Python user-defined functions to be executed.
	 */
	@VisibleForTesting
	public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		builder.addUdfs(getUserDefinedFunctionProto(tableFunction));
		builder.setMetricEnabled(flinkMetricContainer != null);
		return builder.build();
	}

	@Override
	public void processElement(IN element) {
		try {
			baos.reset();
			inputTypeSerializer.serialize(element, baosWrapper);
			mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(baos.toByteArray()));
		} catch (Throwable t) {
			throw new RuntimeException("Failed to process element when sending data to Python SDK harness.", t);
		}
	}

	@Override
	public OutputReceiverFactory createOutputReceiverFactory() {
		return new OutputReceiverFactory() {

			// the input value type is always byte array
			@SuppressWarnings("unchecked")
			@Override
			public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
				return input -> resultReceiver.accept(input.getValue());
			}
		};
	}

	/**
	 * Returns the TypeSerializer for input elements.
	 */
	public abstract TypeSerializer<IN> getInputTypeSerializer();

	@Override
	public String getInputOutputCoderUrn() {
		return TABLE_FUNCTION_SCHEMA_CODER_URN;
	}
}
