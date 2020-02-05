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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * @param <IN>      Type of the input elements.
 * @param <OUT>     Type of the output elements.
 * @param <UDTFIN>  Type of the UDF input type.
 * @param <UDTFOUT> Type of the UDF input type.
 */
public abstract class AbstractPythonTableFunctionOperator<IN, OUT, UDTFIN, UDTFOUT>
	extends AbstractPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * The Python {@link TableFunction} to be executed.
	 */
	protected final PythonFunctionInfo tableFunction;

	/**
	 * The input logical type.
	 */
	protected final RowType inputType;

	/**
	 * The output logical type.
	 */
	protected final RowType outputType;

	/**
	 * The offsets of udtf inputs.
	 */
	protected final int[] udtfInputOffsets;

	/**
	 * The udtf input logical type.
	 */
	protected transient RowType udtfInputType;

	/**
	 * The udtf output logical type.
	 */
	protected transient RowType udtfOutputType;

	/**
	 * The queue holding the input elements for which the execution results have not been received.
	 */
	protected transient LinkedBlockingQueue<IN> forwardedInputQueue;

	/**
	 * The queue holding the user-defined table function execution results. The execution results
	 * are in the same order as the input elements.
	 */
	protected transient LinkedBlockingQueue<UDTFOUT> udtfResultQueue;

	public AbstractPythonTableFunctionOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets) {
		super(config);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.udtfInputOffsets = Preconditions.checkNotNull(udtfInputOffsets);
	}

	@Override
	public void open() throws Exception {
		forwardedInputQueue = new LinkedBlockingQueue<>();
		udtfResultQueue = new LinkedBlockingQueue<>();
		udtfInputType = new RowType(
			Arrays.stream(udtfInputOffsets)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));
		List<RowType.RowField> udtfOutputDataFields = new ArrayList<>(
			outputType.getFields().subList(inputType.getFieldCount(), outputType.getFieldCount()));
		udtfOutputType = new RowType(udtfOutputDataFields);
		super.open();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		bufferInput(element.getValue());
		super.processElement(element);
		emitResults();
	}

	@Override
	public PythonFunctionRunner<IN> createPythonFunctionRunner() throws Exception {
		final FnDataReceiver<UDTFOUT> udtfResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			udtfResultQueue.put(input);
		};

		return new ProjectUdfInputPythonTableFunctionRunner(
			createPythonFunctionRunner(
				udtfResultReceiver,
				createPythonEnvironmentManager()));
	}

	@Override
	public PythonEnv getPythonEnv() {
		return tableFunction.getPythonFunction().getPythonEnv();
	}

	/**
	 * Buffers the specified input, it will be used to construct
	 * the operator result together with the udtf execution result.
	 */
	public abstract void bufferInput(IN input);

	public abstract UDTFIN getUdtfInput(IN element);

	public abstract PythonFunctionRunner<UDTFIN> createPythonFunctionRunner(
		FnDataReceiver<UDTFOUT> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager);

	private class ProjectUdfInputPythonTableFunctionRunner implements PythonFunctionRunner<IN> {

		private final PythonFunctionRunner<UDTFIN> pythonFunctionRunner;

		ProjectUdfInputPythonTableFunctionRunner(PythonFunctionRunner<UDTFIN> pythonFunctionRunner) {
			this.pythonFunctionRunner = pythonFunctionRunner;
		}

		@Override
		public void open() throws Exception {
			pythonFunctionRunner.open();
		}

		@Override
		public void close() throws Exception {
			pythonFunctionRunner.close();
		}

		@Override
		public void startBundle() throws Exception {
			pythonFunctionRunner.startBundle();
		}

		@Override
		public void finishBundle() throws Exception {
			pythonFunctionRunner.finishBundle();
		}

		@Override
		public void processElement(IN element) throws Exception {
			pythonFunctionRunner.processElement(getUdtfInput(element));
		}
	}
}
