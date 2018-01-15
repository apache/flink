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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;

import org.python.core.PyObject;

/**
 * A thin wrapper layer over {@link IterativeStream}.
 *
 * <p>The python iterative data stream represents the start of an iteration in a
 * {@link PythonDataStream}.</p>
 */
@PublicEvolving
public class PythonIterativeStream extends PythonSingleOutputStreamOperator {

	PythonIterativeStream(IterativeStream<PyObject> iterativeStream) {
		super(iterativeStream);
	}

	/**
	 * A thin wrapper layer over {@link IterativeStream#closeWith(org.apache.flink.streaming.api.datastream.DataStream)}
	 *
	 * <p>Please note that this function works with {@link PythonDataStream} and thus wherever a DataStream is mentioned in
	 * the above {@link IterativeStream#closeWith(org.apache.flink.streaming.api.datastream.DataStream)} description,
	 * the user may regard it as {@link PythonDataStream} .
	 *
	 * @param feedback_stream {@link PythonDataStream} that will be used as input to the iteration
	 * head.
	 * @return The feedback stream.
	 */
	public PythonDataStream close_with(PythonDataStream<? extends DataStream<PyObject>> feedback_stream) {
		((IterativeStream<PyObject>) this.stream).closeWith(feedback_stream.stream);
		return feedback_stream;
	}
}
