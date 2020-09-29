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

package org.apache.flink.streaming.api.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;

/**
 * {@link DataStreamPythonFunction} maintains the serialized python function and its function type, which will be used in
 * BeamDataStreamStatelessPythonFunctionRunner.
 */
@Internal
public class DataStreamPythonFunction implements PythonFunction {
	private static final long serialVersionUID = 1L;
	private final byte[] serializedPythonFunction;
	private final PythonEnv pythonEnv;

	public DataStreamPythonFunction(
		byte[] serializedPythonFunction,
		PythonEnv pythonEnv) {
		this.serializedPythonFunction = serializedPythonFunction;
		this.pythonEnv = pythonEnv;
	}

	@Override
	public byte[] getSerializedPythonFunction() {
		return this.serializedPythonFunction;
	}

	@Override
	public PythonEnv getPythonEnv() {
		return this.pythonEnv;
	}
}
