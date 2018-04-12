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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.python.util.AdapterMap;

import org.python.core.PyException;
import org.python.core.PyObject;

import java.io.IOException;

/**
 * The {@code PythonMapFunction} is a thin wrapper layer over a Python UDF {@code MapFunction}.
 * It receives a {@code MapFunction} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of the job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code MapFunction}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonMapFunction extends AbstractPythonUDF<MapFunction<PyObject, PyObject>> implements MapFunction<PyObject, PyObject> {
	private static final long serialVersionUID = 3001212087036451818L;

	public PythonMapFunction(MapFunction<PyObject, PyObject> fun) throws IOException {
		super(fun);
	}

	@Override
	public PyObject map(PyObject value) throws Exception {
		try {
			return AdapterMap.adapt(fun.map(value));
		} catch (PyException pe) {
			throw createAndLogException(pe);
		}
	}
}
