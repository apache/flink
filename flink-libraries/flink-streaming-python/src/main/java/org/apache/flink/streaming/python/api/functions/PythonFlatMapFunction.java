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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.python.util.PythonCollector;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;
import org.apache.flink.util.Collector;
import org.python.core.PyObject;

import java.io.IOException;


/**
 * The {@code PythonFlatMapFunction} is a thin wrapper layer over a Python UDF {@code FlatMapFunction}.
 * It receives a {@code FlatMapFunction} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of the job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code FlatMapFunction}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonFlatMapFunction extends RichFlatMapFunction<PyObject, PyObject> {
	private static final long serialVersionUID = -6098432222172956477L;

	private final byte[] serFun;
	private transient FlatMapFunction<PyObject, PyObject> fun;
	private transient PythonCollector collector;

	public PythonFlatMapFunction(FlatMapFunction<PyObject, PyObject> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration config) throws Exception {
		this.fun =
			(FlatMapFunction<PyObject, PyObject>) UtilityFunctions.smartFunctionDeserialization(
				getRuntimeContext(), serFun);
		if (this.fun instanceof RichFunction) {
			final RichFlatMapFunction flatMapFun = (RichFlatMapFunction)this.fun;
			flatMapFun.setRuntimeContext(getRuntimeContext());
			flatMapFun.open(config);
		}
		this.collector = new PythonCollector();
	}

	@Override
	public void close() throws Exception {
		if (this.fun instanceof RichFunction) {
			((RichFlatMapFunction)this.fun).close();
		}
	}

	@Override
	public void flatMap(PyObject value, Collector<PyObject> out) throws Exception {
		this.collector.setCollector(out);
		this.fun.flatMap(value, this.collector);
	}
}
