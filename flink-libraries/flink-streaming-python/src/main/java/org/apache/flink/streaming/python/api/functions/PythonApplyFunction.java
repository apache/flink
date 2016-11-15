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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.python.util.PythonCollector;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;
import org.apache.flink.util.Collector;
import org.python.core.PyObject;

import java.io.IOException;

/**
 * The {@code PythonApplyFunction} is a thin wrapper layer over a Python UDF {@code WindowFunction}.
 * It receives an {@code WindowFunction} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code WindowFunction}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonApplyFunction<W extends Window> extends RichWindowFunction<PyObject, PyObject, PyKey, W> {
	private static final long serialVersionUID = 577032239468987781L;

	private final byte[] serFun;
	private transient WindowFunction<PyObject, PyObject, Object, Window> fun;
	private transient PythonCollector collector;

	public PythonApplyFunction(WindowFunction<PyObject, PyObject, Object, W> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		this.fun =
			(WindowFunction<PyObject, PyObject, Object, Window>) UtilityFunctions.smartFunctionDeserialization(
			getRuntimeContext(), this.serFun);
		if (this.fun instanceof RichWindowFunction) {
			final RichWindowFunction winFun = (RichWindowFunction)this.fun;
			winFun.setRuntimeContext(getRuntimeContext());
			winFun.open(parameters);
		}
		this.collector = new PythonCollector();
	}

	@Override
	public void close() throws Exception {
		if (this.fun instanceof RichWindowFunction) {
			((RichWindowFunction)this.fun).close();
		}
	}

	@Override
	public void apply(PyKey key, W window, Iterable<PyObject> values, Collector<PyObject> out) throws Exception {
		this.collector.setCollector(out);
		this.fun.apply(key.getData(), window, values, this.collector);
	}
}
