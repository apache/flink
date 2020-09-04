/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.datastream.runtime.functions.python;

import org.apache.flink.table.functions.python.PythonFunction;

import java.io.Serializable;

/**
 * {@link DataStreamPythonFunctionInfo} holds a PythonFunction and its function type.
 * */
public class DataStreamPythonFunctionInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	private final PythonFunction pythonFunction;
	private final int functionType;

	public DataStreamPythonFunctionInfo(PythonFunction pythonFunction, int functionType) {
		this.pythonFunction = pythonFunction;
		this.functionType = functionType;
	}

	public PythonFunction getPythonFunction() {
		return pythonFunction;
	}

	public int getFunctionType(){
		return this.functionType;
	}
}
