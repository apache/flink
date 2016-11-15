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
package org.apache.flink.streaming.python.api.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;
import org.python.core.PyObject;

import java.io.IOException;


/**
 * The {@code PythonFilterFunction} is a thin wrapper layer over a Python UDF {@code FilterFunction}.
 * It receives a {@code FilterFunction} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code FilterFunction}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonFilterFunction extends RichFilterFunction<PyObject> {
	private static final long serialVersionUID = 775688642701399472L;

	private final byte[] serFun;
	private transient FilterFunction<PyObject> fun;

	public PythonFilterFunction(FilterFunction<PyObject> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		this.fun = (FilterFunction<PyObject>) UtilityFunctions.smartFunctionDeserialization(
			getRuntimeContext(), this.serFun);
		if (this.fun instanceof RichFunction) {
			final RichFilterFunction filterFun = (RichFilterFunction)this.fun;
			filterFun.setRuntimeContext(getRuntimeContext());
			filterFun.open(parameters);
		}
	}

	@Override
	public void close() throws Exception {
		if (this.fun instanceof RichFunction) {
			((RichFilterFunction)this.fun).close();
		}
	}

	@Override
	public boolean filter(PyObject value) throws Exception {
		return this.fun.filter(value);
	}
}
