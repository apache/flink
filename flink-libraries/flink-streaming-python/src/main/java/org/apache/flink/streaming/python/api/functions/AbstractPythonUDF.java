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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.python.util.InterpreterUtils;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;
import org.apache.flink.util.FlinkException;

import org.python.core.PyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Generic base-class for wrappers of python functions implenting the {@link Function} interface.
 */
public class AbstractPythonUDF<F extends Function> extends AbstractRichFunction {
	protected Logger log = LoggerFactory.getLogger(AbstractPythonUDF.class);
	private final byte[] serFun;
	protected transient F fun;

	AbstractPythonUDF(F fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.fun = InterpreterUtils.deserializeFunction(getRuntimeContext(), this.serFun);
		if (this.fun instanceof RichFunction) {
			try {
				final RichFunction rf = (RichFunction) this.fun;
				rf.setRuntimeContext(getRuntimeContext());
				rf.open(parameters);
			} catch (PyException pe) {
				throw createAndLogException(pe);
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (this.fun instanceof RichFunction) {
			try {
				((RichFunction) this.fun).close();
			} catch (PyException pe) {
				throw createAndLogException(pe);
			}
		}
	}

	FlinkException createAndLogException(PyException pe) {
		return createAndLogException(pe, log);
	}

	static FlinkException createAndLogException(PyException pe, Logger log) {
		StringWriter sw = new StringWriter();
		try (PrintWriter pw = new PrintWriter(sw)) {
			pe.printStackTrace(pw);
		}
		String pythonStackTrace = sw.toString().trim();

		log.error("Python function failed: " + System.lineSeparator() + pythonStackTrace);
		return new FlinkException("Python function failed: " + pythonStackTrace);
	}
}
