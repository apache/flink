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
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.AbstractPythonStatelessFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Abstract {@link PythonFunctionRunner} used to execute Python {@link TableFunction}.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractPythonTableFunctionRunner<IN> extends AbstractPythonStatelessFunctionRunner<IN> {

	private static final String SCHEMA_CODER_URN = "flink:coder:schema:v1";
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
		RowType outputType) {
		super(taskName, resultReceiver, environmentManager, inputType, outputType, TABLE_FUNCTION_URN);
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
		return builder.build();
	}

	/**
	 * Gets the proto representation of the input coder.
	 */
	@Override
	public RunnerApi.Coder getInputCoderProto() {
		return getTableCoderProto(getInputType());
	}

	/**
	 * Gets the proto representation of the output coder.
	 */
	@Override
	public RunnerApi.Coder getOutputCoderProto() {
		return getTableCoderProto(getOutputType());
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

	private RunnerApi.Coder getTableCoderProto(RowType rowType) {
		return RunnerApi.Coder.newBuilder()
			.setSpec(
				RunnerApi.FunctionSpec.newBuilder()
					.setUrn(SCHEMA_CODER_URN)
					.setPayload(org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString.copyFrom(
						toTableFunctionProtoType(rowType).toByteArray()))
					.build())
			.build();
	}

	private FlinkFnApi.Schema.FieldType toTableFunctionProtoType(RowType rowType) {
		FlinkFnApi.Schema.FieldType.Builder builder =
			FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.TABLE_FUNCTION_ROW)
				.setNullable(rowType.isNullable());

		LogicalTypeDefaultVisitor<FlinkFnApi.Schema.FieldType> converter =
			new PythonTypeUtils.LogicalTypeToProtoTypeConverter();
		FlinkFnApi.Schema.Builder schemaBuilder = FlinkFnApi.Schema.newBuilder();
		for (RowType.RowField field : rowType.getFields()) {
			schemaBuilder.addFields(
				FlinkFnApi.Schema.Field.newBuilder()
					.setName(field.getName())
					.setDescription(field.getDescription().orElse(""))
					.setType(field.getType().accept(converter))
					.build());
		}
		builder.setRowSchema(schemaBuilder.build());
		return builder.build();
	}
}
