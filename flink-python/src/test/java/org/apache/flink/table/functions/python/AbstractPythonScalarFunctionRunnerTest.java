/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.functions.python;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.runners.python.AbstractPythonScalarFunctionRunner;
import org.apache.flink.table.types.DataType;

/**
 * Base class for PythonScalarFunctionRunner and BaseRowPythonScalarFunctionRunner test.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 */
public abstract class AbstractPythonScalarFunctionRunnerTest<IN, OUT>  {

	AbstractPythonScalarFunctionRunner<IN, OUT> createSingleUDFRunner() {
		PythonFunctionInfo[] pythonFunctionInfos = new PythonFunctionInfo[] {
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Integer[]{0})
		};

		DataType dataType = DataTypes.ROW(
			DataTypes.FIELD("f1", DataTypes.BIGINT())
		);
		return createPythonScalarFunctionRunner(pythonFunctionInfos, dataType, dataType);
	}

	AbstractPythonScalarFunctionRunner<IN, OUT> createMultipleUDFRunner() {
		PythonFunctionInfo[] pythonFunctionInfos = new PythonFunctionInfo[] {
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Integer[]{0, 1}),
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Integer[]{0, 2})
		};

		DataType inputType = DataTypes.ROW(
			DataTypes.FIELD("f1", DataTypes.BIGINT()),
			DataTypes.FIELD("f2", DataTypes.BIGINT()),
			DataTypes.FIELD("f3", DataTypes.BIGINT())
		);

		DataType outputType = DataTypes.ROW(
			DataTypes.FIELD("f1", DataTypes.BIGINT()),
			DataTypes.FIELD("f2", DataTypes.BIGINT())
		);
		return createPythonScalarFunctionRunner(pythonFunctionInfos, inputType, outputType);
	}

	AbstractPythonScalarFunctionRunner<IN, OUT> createChainedUDFRunner() {
		PythonFunctionInfo[] pythonFunctionInfos = new PythonFunctionInfo[] {
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Integer[]{0, 1}),
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Object[]{
					0,
					new PythonFunctionInfo(
						DummyPythonFunction.INSTANCE,
						new Integer[]{1, 2})
				}),
			new PythonFunctionInfo(
				DummyPythonFunction.INSTANCE,
				new Object[]{
					new PythonFunctionInfo(
						DummyPythonFunction.INSTANCE,
						new Integer[]{1, 3}),
					new PythonFunctionInfo(
						DummyPythonFunction.INSTANCE,
						new Integer[]{3, 4})
				})
		};

		DataType inputType = DataTypes.ROW(
			DataTypes.FIELD("f1", DataTypes.BIGINT()),
			DataTypes.FIELD("f2", DataTypes.BIGINT()),
			DataTypes.FIELD("f3", DataTypes.BIGINT()),
			DataTypes.FIELD("f4", DataTypes.BIGINT()),
			DataTypes.FIELD("f5", DataTypes.BIGINT())
		);

		DataType outputType = DataTypes.ROW(
			DataTypes.FIELD("f1", DataTypes.BIGINT()),
			DataTypes.FIELD("f2", DataTypes.BIGINT()),
			DataTypes.FIELD("f3", DataTypes.BIGINT())
		);
		return createPythonScalarFunctionRunner(pythonFunctionInfos, inputType, outputType);
	}

	public abstract AbstractPythonScalarFunctionRunner<IN, OUT> createPythonScalarFunctionRunner(
		PythonFunctionInfo[] pythonFunctionInfos, DataType inputType, DataType outputType);

	/**
	 * Dummy PythonFunction.
	 */
	public static class DummyPythonFunction implements PythonFunction {

		private static final long serialVersionUID = 1L;

		public static final PythonFunction INSTANCE = new DummyPythonFunction();

		@Override
		public byte[] getSerializedPythonFunction() {
			return new byte[0];
		}

		@Override
		public PythonEnv getPythonEnv() {
			return new PythonEnv(PythonEnv.ExecType.PROCESS);
		}
	}
}
