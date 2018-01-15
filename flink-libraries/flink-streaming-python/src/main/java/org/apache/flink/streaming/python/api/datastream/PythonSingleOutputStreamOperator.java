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

package org.apache.flink.streaming.python.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.python.core.PyObject;

/**
 * A thin wrapper layer over {@link SingleOutputStreamOperator}
 *
 * <p>{@code PythonSingleOutputStreamOperator} represents a user defined transformation
 * applied on a {@link PythonDataStream} with one predefined output type.</p>
 */
@Public
public class PythonSingleOutputStreamOperator extends PythonDataStream<SingleOutputStreamOperator<PyObject>> {

	PythonSingleOutputStreamOperator(SingleOutputStreamOperator<PyObject> stream) {
		super(stream);
	}

	/**
	 * A thin wrapper layer over {@link SingleOutputStreamOperator#name(String)} .
	 *
	 * @param name operator name
	 * @return The named operator.
	 */
	public PythonSingleOutputStreamOperator name(String name) {
		this.stream.name(name);
		return this;
	}
}

