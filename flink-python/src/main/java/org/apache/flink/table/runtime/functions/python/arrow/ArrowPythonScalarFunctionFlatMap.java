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

package org.apache.flink.table.runtime.functions.python.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.functions.python.AbstractPythonStatelessFunctionFlatMap;
import org.apache.flink.table.runtime.runners.python.scalar.arrow.ArrowPythonScalarFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Arrays;

/**
 * The {@link RichFlatMapFunction} used to invoke Arrow Python {@link ScalarFunction} functions for
 * the old planner.
 */
@Internal
public final class ArrowPythonScalarFunctionFlatMap extends AbstractPythonStatelessFunctionFlatMap {

	private static final long serialVersionUID = 1L;

	/**
	 * The Python {@link ScalarFunction}s to be executed.
	 */
	private final PythonFunctionInfo[] scalarFunctions;

	/**
	 * The offset of the fields which should be forwarded.
	 */
	private final int[] forwardedFields;

	/**
	 * Allocator which is used for byte buffer allocation.
	 */
	private transient BufferAllocator allocator;

	/**
	 * Reader which is responsible for deserialize the Arrow format data to the Flink rows.
	 */
	private transient ArrowReader<Row> arrowReader;

	/**
	 * Reader which is responsible for convert the execution result from
	 * byte array to arrow format.
	 */
	private transient ArrowStreamReader reader;

	public ArrowPythonScalarFunctionFlatMap(
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
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		RowTypeInfo forwardedInputTypeInfo = new RowTypeInfo(
			Arrays.stream(forwardedFields)
				.mapToObj(i -> inputType.getFields().get(i))
				.map(RowType.RowField::getType)
				.map(TypeConversions::fromLogicalToDataType)
				.map(TypeConversions::fromDataTypeToLegacyInfo)
				.toArray(TypeInformation[]::new));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
		allocator = ArrowUtils.ROOT_ALLOCATOR.newChildAllocator("reader", 0, Long.MAX_VALUE);
		reader = new ArrowStreamReader(bais, allocator);
	}

	@Override
	public void close() throws Exception {
		try {
			super.close();
		} finally {
			reader.close();
			allocator.close();
		}
	}

	@Override
	public PythonEnv getPythonEnv() {
		return scalarFunctions[0].getPythonFunction().getPythonEnv();
	}

	@Override
	public PythonFunctionRunner<Row> createPythonFunctionRunner() throws IOException {
		final FnDataReceiver<byte[]> userDefinedFunctionResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			userDefinedFunctionResultQueue.put(input);
		};

		return new ArrowPythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			userDefinedFunctionResultReceiver,
			scalarFunctions,
			createPythonEnvironmentManager(),
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			getPythonConfig().getMaxArrowBatchSize(),
			jobOptions,
			getFlinkMetricContainer());
	}

	@Override
	public void bufferInput(Row input) {
		Row forwardedFieldsRow = Row.project(input, forwardedFields);
		if (getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()) {
			forwardedFieldsRow = forwardedInputSerializer.copy(forwardedFieldsRow);
		}
		forwardedInputQueue.add(forwardedFieldsRow);
	}

	@Override
	public void emitResults() throws IOException {
		byte[] udfResult;
		while ((udfResult = userDefinedFunctionResultQueue.poll()) != null) {
			bais.setBuffer(udfResult, 0, udfResult.length);
			reader.loadNextBatch();
			VectorSchemaRoot root = reader.getVectorSchemaRoot();
			if (arrowReader == null) {
				arrowReader = ArrowUtils.createRowArrowReader(root, userDefinedFunctionOutputType);
			}
			for (int i = 0; i < root.getRowCount(); i++) {
				resultCollector.collect(Row.join(forwardedInputQueue.poll(), arrowReader.read(i)));
			}
		}
	}

	@Override
	public int getForwardedFieldsCount() {
		return forwardedFields.length;
	}
}
