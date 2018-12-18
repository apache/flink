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

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.python.util.InterpreterUtils;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;

import org.python.core.PyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * The {@code PythonIteratorFunction} is a thin wrapper layer over a Python UDF {@code Iterator}.
 * It receives an {@code Iterator} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of the job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code Iterator}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonIteratorFunction extends RichSourceFunction<Object> {
	private static final long serialVersionUID = 6741748297048588334L;
	private static final Logger LOG = LoggerFactory.getLogger(PythonIteratorFunction.class);

	private final byte[] serFun;
	private transient Iterator<Object> fun;
	private transient volatile boolean isRunning;

	public PythonIteratorFunction(Iterator<Object> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.fun = InterpreterUtils.deserializeFunction(getRuntimeContext(), this.serFun);
		if (this.fun instanceof RichFunction) {
			try {
				final RichFunction winFun = (RichFunction) this.fun;
				winFun.setRuntimeContext(getRuntimeContext());
				winFun.open(parameters);
			} catch (PyException pe) {
				throw AbstractPythonUDF.createAndLogException(pe, LOG);
			}
		}
	}

	@Override
	public void run(SourceContext<Object> ctx) throws Exception {
		try {
			while (isRunning && this.fun.hasNext()) {
				ctx.collect(this.fun.next());
			}
		} catch (PyException pe) {
			throw AbstractPythonUDF.createAndLogException(pe, LOG);
		}
	}

	@Override
	public void close() throws Exception {
		if (this.fun instanceof RichFunction) {
			try {
				((RichFunction) this.fun).close();
			} catch (PyException pe) {
				throw AbstractPythonUDF.createAndLogException(pe, LOG);
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
