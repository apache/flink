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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

import org.python.core.PyObject;

/**
 * Collects a {@code PyObject} record and forwards it. It provides a safety layer with regards
 * to type conversions. In case the UDF is a java based code (as opposed to Python), it is not
 * guaranteed that the function handles PyObject types properly. In such case it'll fail immediately
 * with java.lang.ClassCastException, instead of a fail somewhere down the code.
 */
@Public
public class PythonCollector implements Collector<PyObject> {
	private Collector<PyObject> collector;

	public void setCollector(Collector<PyObject> collector) {
		this.collector = collector;
	}

	@Override
	public void collect(PyObject record) {
		this.collector.collect(record);
	}

	@Override
	public void close() {
		this.collector.close();
	}
}
