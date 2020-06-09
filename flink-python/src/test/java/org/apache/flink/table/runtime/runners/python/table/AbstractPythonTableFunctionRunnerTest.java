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

package org.apache.flink.table.runtime.runners.python.table;

import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.scalar.AbstractPythonScalarFunctionRunnerTest;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/**
 * Base class for PythonTableFunctionRunner and RowDataPythonTableFunctionRunner test.
 *
 * @param <IN>  Type of the input elements.
 */
public abstract class AbstractPythonTableFunctionRunnerTest<IN> {
	AbstractPythonTableFunctionRunner<IN> createUDTFRunner() throws Exception {
		PythonFunctionInfo pythonFunctionInfo = new PythonFunctionInfo(
			AbstractPythonScalarFunctionRunnerTest.DummyPythonFunction.INSTANCE,
			new Integer[]{0});

		RowType rowType = new RowType(Collections.singletonList(new RowType.RowField("f1", new BigIntType())));
		return createPythonTableFunctionRunner(pythonFunctionInfo, rowType, rowType);
	}

	public abstract AbstractPythonTableFunctionRunner<IN> createPythonTableFunctionRunner(
		PythonFunctionInfo pythonFunctionInfo, RowType inputType, RowType outputType) throws Exception;
}
