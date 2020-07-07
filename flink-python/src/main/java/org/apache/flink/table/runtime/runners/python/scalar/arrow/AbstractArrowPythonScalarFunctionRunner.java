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

package org.apache.flink.table.runtime.runners.python.scalar.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.runtime.runners.python.scalar.AbstractPythonScalarFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.Map;

/**
 * Abstract {@link PythonFunctionRunner} used to execute Arrow Python {@link ScalarFunction}s.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractArrowPythonScalarFunctionRunner<IN> extends AbstractPythonScalarFunctionRunner<IN> {

	private static final String SCHEMA_ARROW_CODER_URN = "flink:coder:schema:scalar_function:arrow:v1";

	static {
		ArrowUtils.checkArrowUsable();
	}

	/**
	 * Max number of elements to include in an arrow batch.
	 */
	private final int maxArrowBatchSize;

	/**
	 * Container that holds a set of vectors for the input elements
	 * to be sent to the Python worker.
	 */
	protected transient VectorSchemaRoot root;

	/**
	 * Allocator which is used by {@link #root} for byte buffer allocation.
	 */
	private transient BufferAllocator allocator;

	/**
	 * Writer which is responsible for serialize the input elements to arrow format.
	 */
	@VisibleForTesting
	transient ArrowWriter<IN> arrowWriter;

	/**
	 * Writer which is responsible for convert the arrow format data into byte array.
	 */
	private transient ArrowStreamWriter arrowStreamWriter;

	/**
	 * The current number of elements to be included in an arrow batch.
	 */
	private transient int currentBatchCount;

	public AbstractArrowPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		int maxArrowBatchSize,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, scalarFunctions, environmentManager, inputType, outputType, jobOptions, flinkMetricContainer);
		this.maxArrowBatchSize = maxArrowBatchSize;
	}

	@Override
	public void open() throws Exception {
		super.open();
		allocator = ArrowUtils.getRootAllocator().newChildAllocator("writer", 0, Long.MAX_VALUE);
		root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(getInputType()), allocator);
		arrowWriter = createArrowWriter();
		arrowStreamWriter = new ArrowStreamWriter(root, null, baos);
		arrowStreamWriter.start();
		currentBatchCount = 0;
	}

	@Override
	public void close() throws Exception {
		try {
			super.close();
			arrowStreamWriter.end();
		} finally {
			root.close();
			allocator.close();
		}
	}

	@Override
	public void processElement(IN element) {
		try {
			arrowWriter.write(element);

			currentBatchCount++;
			if (currentBatchCount >= maxArrowBatchSize) {
				finishCurrentBatch();
			}
		} catch (Throwable t) {
			throw new RuntimeException("Failed to process element.", t);
		}
	}

	@Override
	public void finishBundle() throws Exception {
		finishCurrentBatch();
		super.finishBundle();
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

	@Override
	public String getInputOutputCoderUrn() {
		return SCHEMA_ARROW_CODER_URN;
	}

	/**
	 * Creates an {@link ArrowWriter}.
	 */
	public abstract ArrowWriter<IN> createArrowWriter();

	/**
	 * Forces to finish the processing of the current batch of elements.
	 * It will serialize the batch of elements into one arrow batch.
	 */
	private void finishCurrentBatch() throws Exception {
		if (currentBatchCount > 0) {
			arrowWriter.finish();
			// the batch of elements sent out as one row should be serialized into one arrow batch
			arrowStreamWriter.writeBatch();
			arrowWriter.reset();

			mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(baos.toByteArray()));
			baos.reset();
		}
		currentBatchCount = 0;
	}
}
