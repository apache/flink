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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.functions.python.AbstractPythonScalarFunctionFlatMap;
import org.apache.flink.table.runtime.runners.python.scalar.arrow.ArrowPythonScalarFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;

/**
 * The {@link RichFlatMapFunction} used to invoke Arrow Python {@link ScalarFunction} functions for
 * the old planner.
 */
@Internal
public final class ArrowPythonScalarFunctionFlatMap extends AbstractPythonScalarFunctionFlatMap {

	private static final long serialVersionUID = 1L;

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
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		allocator = ArrowUtils.getRootAllocator().newChildAllocator("reader", 0, Long.MAX_VALUE);
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
}
