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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * PythonFunctionInfo contains the execution information of a Python function, such as:
 * the actual Python function, the input arguments, etc.
 */
@Internal
public class PythonFunctionInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The python function to be executed.
	 */
	private final PythonFunction pythonFunction;

	/**
	 * The input arguments, it could be an input offset of the input row or
	 * the execution result of another python function described as PythonFunctionInfo.
	 */
	private final Object[] inputs;

	public PythonFunctionInfo(
		PythonFunction pythonFunction,
		Object[] inputs) {
		this.pythonFunction = Preconditions.checkNotNull(pythonFunction);
		this.inputs = Preconditions.checkNotNull(inputs);
	}

	public PythonFunction getPythonFunction() {
		return this.pythonFunction;
	}

	public Object[] getInputs() {
		return this.inputs;
	}
}
