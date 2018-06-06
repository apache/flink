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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;

import org.python.core.PyException;
import org.python.core.PyObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The {@code PythonKeySelector} is a thin wrapper layer over a Python UDF {@code KeySelector}.
 * It receives a {@code KeySelector} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of the job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code KeySelector}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonKeySelector implements KeySelector<PyObject, PyKey> {
	private static final long serialVersionUID = 7403775239671366607L;
	private static final Logger LOG = LoggerFactory.getLogger(PythonKeySelector.class);

	private final byte[] serFun;
	private transient KeySelector<PyObject, Object> fun;

	public PythonKeySelector(KeySelector<PyObject, PyKey> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	@SuppressWarnings("unchecked")
	public PyKey getKey(PyObject value) throws Exception {
		if (fun == null) {
			fun = SerializationUtils.deserializeObject(serFun);
		}
		try {
			Object key = fun.getKey(value);
			return new PyKey(key);
		} catch (PyException pe) {
			throw AbstractPythonUDF.createAndLogException(pe, LOG);
		}
	}
}
