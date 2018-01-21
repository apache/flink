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
import org.apache.flink.streaming.python.api.functions.UtilityFunctions;
import org.apache.flink.util.Collector;
import org.python.core.PyObject;

/**
 * Collects a {@code PyObject} record and forwards it. It makes sure that the record is converted,
 * if necessary, to a {@code PyObject}.
 */
@Public
public class PythonCollector implements Collector<PyObject> {
	private Collector<PyObject> collector;

	public void setCollector(Collector<PyObject> collector) {
		this.collector = collector;
	}

	@Override
	public void collect(PyObject record) {
		PyObject po = UtilityFunctions.adapt(record);
		this.collector.collect(po);
	}

	@Override
	public void close() {
		this.collector.close();
	}
}
