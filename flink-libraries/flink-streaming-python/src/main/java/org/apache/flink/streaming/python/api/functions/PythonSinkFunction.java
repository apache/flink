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

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.python.core.PyException;
import org.python.core.PyObject;

import java.io.IOException;

/**
 * The {@code PythonSinkFunction} is a thin wrapper layer over a Python UDF {@code SinkFunction}.
 * It receives a {@code SinkFunction} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of the job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code SinkFunction}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonSinkFunction extends AbstractPythonUDF<SinkFunction<PyObject>> implements SinkFunction<PyObject> {

	private static final long serialVersionUID = -9030596504893036458L;

	public PythonSinkFunction(SinkFunction<PyObject> fun) throws IOException {
		super(fun);
	}

	@Override
	public void invoke(PyObject value, Context context) throws Exception {
		try {
			this.fun.invoke(value, context);
		} catch (PyException pe) {
			throw createAndLogException(pe);
		}
	}
}
