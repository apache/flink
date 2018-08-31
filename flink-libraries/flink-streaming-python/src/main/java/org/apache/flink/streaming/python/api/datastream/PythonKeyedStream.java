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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.python.api.functions.PyKey;
import org.apache.flink.streaming.python.api.functions.PythonReduceFunction;

import org.python.core.PyObject;

import java.io.IOException;

/**
 * A thin wrapper layer over {@link KeyedStream}.
 *
 * <p>A {@code PythonKeyedStream} represents a {@link PythonDataStream} on which operator state is
 * partitioned by key using a provided {@link org.apache.flink.api.java.functions.KeySelector}.
 */
@Public
public class PythonKeyedStream extends PythonDataStream<KeyedStream<PyObject, PyKey>> {

	PythonKeyedStream(KeyedStream<PyObject, PyKey> stream) {
		super(stream);
	}

	/**
	 * A thin wrapper layer over {@link KeyedStream#countWindow(long, long)}.
	 *
	 * @param size The size of the windows in number of elements.
	 * @param slide The slide interval in number of elements.
	 * @return The python windowed stream {@link PythonWindowedStream}
	 */
	public PythonWindowedStream count_window(long size, long slide) {
		return new PythonWindowedStream<GlobalWindow>(this.stream.countWindow(size, slide));
	}

	/**
	 * A thin wrapper layer over {@link KeyedStream#timeWindow(Time)}.
	 *
	 * @param size The size of the window.
	 * @return The python windowed stream {@link PythonWindowedStream}
	 */
	public PythonWindowedStream time_window(Time size) {
		return new PythonWindowedStream<TimeWindow>(this.stream.timeWindow(size));
	}

	/**
	 * A thin wrapper layer over {@link KeyedStream#timeWindow(Time, Time)}.
	 *
	 * @param size The size of the window.
	 * @return The python wrapper {@link PythonWindowedStream}
	 */
	public PythonWindowedStream time_window(Time size, Time slide) {
		return new PythonWindowedStream<TimeWindow>(this.stream.timeWindow(size, slide));
	}

	/**
	 * A thin wrapper layer over {@link KeyedStream#reduce(ReduceFunction)}.
	 *
	 * @param reducer The {@link ReduceFunction} that will be called for every
	 * element of the input values with the same key.
	 * @return The transformed data stream @{link PythonSingleOutputStreamOperator}.
	 */
	public PythonSingleOutputStreamOperator reduce(ReduceFunction<PyObject> reducer) throws IOException {
		return new PythonSingleOutputStreamOperator(this.stream.reduce(new PythonReduceFunction(reducer)));
	}
}
