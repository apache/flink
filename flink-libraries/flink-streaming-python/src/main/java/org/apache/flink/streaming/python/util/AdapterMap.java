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

package org.apache.flink.streaming.python.util;

import org.apache.flink.api.common.functions.MapFunction;

import org.python.core.Py;
import org.python.core.PyObject;

/**
 * A generic map operator that convert any java type to PyObject. It is mainly used to convert elements
 * collected from a source functions, to PyObject objects.
 *
 * @param <IN> Any given java object
 */
public class AdapterMap<IN> implements MapFunction<IN, PyObject> {
	private static final long serialVersionUID = 1582769662549499373L;

	/**
	 * Convert java object to its corresponding PyObject representation.
	 *
	 * @param o Java object
	 * @return PyObject
	 */
	public static PyObject adapt(Object o) {
		if (o instanceof PyObject) {
			return (PyObject) o;
		}
		return Py.java2py(o);
	}

	@Override
	public PyObject map(IN value) throws Exception {
		PyObject ret = adapt(value);
		return ret;
	}
}
