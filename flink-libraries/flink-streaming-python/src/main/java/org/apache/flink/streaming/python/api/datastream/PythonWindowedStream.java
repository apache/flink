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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.python.api.functions.PyKey;
import org.apache.flink.streaming.python.api.functions.PythonApplyFunction;
import org.apache.flink.streaming.python.api.functions.PythonReduceFunction;

import org.python.core.PyObject;

import java.io.IOException;

/**
 * A thin wrapper layer over {@link WindowedStream}.
 *
 * <p>A {@code PythonWindowedStream} represents a data stream where elements are grouped by
 * key, and for each key, the stream of elements is split into windows based on a
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. Window emission
 * is triggered based on a {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.</p>
 */
@Public
public class PythonWindowedStream<W extends Window> {
	private final WindowedStream<PyObject, PyKey, W> stream;

	PythonWindowedStream(WindowedStream<PyObject, PyKey, W> stream) {
		this.stream = stream;
	}

	/**
	 * A thin wrapper layer over {@link WindowedStream#reduce(org.apache.flink.api.common.functions.ReduceFunction)}.
	 *
	 * @param fun The reduce function.
	 * @return The data stream that is the result of applying the reduce function to the window.
	 */
	@SuppressWarnings("unchecked")
	public PythonSingleOutputStreamOperator reduce(ReduceFunction<PyObject> fun) throws IOException {
		return new PythonSingleOutputStreamOperator(stream.reduce(new PythonReduceFunction(fun)));
	}

	/**
	 * A thin wrapper layer over {@link WindowedStream#apply(WindowFunction)}.
	 *
	 * @param fun The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public PythonSingleOutputStreamOperator apply(
		WindowFunction<PyObject, Object, Object, W> fun) throws IOException {
		return new PythonSingleOutputStreamOperator(stream.apply(new PythonApplyFunction<>(fun)));
	}
}
