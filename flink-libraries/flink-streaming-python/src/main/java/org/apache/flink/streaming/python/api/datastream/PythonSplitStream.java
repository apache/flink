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
import org.apache.flink.streaming.api.datastream.SplitStream;

import org.python.core.PyObject;

/**
 * A thin wrapper layer over {@link SplitStream}.
 *
 * <p>The {@code PythonSplitStream} represents an operator that has been split using an
 * {@link org.apache.flink.streaming.api.collector.selector.OutputSelector}. Named outputs
 * can be selected using the {@link #select} function. To apply transformation on the whole
 * output simply call the transformation on the {@code PythonSplitStream}</p>
 */
@PublicEvolving
public class PythonSplitStream extends PythonDataStream<SplitStream<PyObject>> {

	PythonSplitStream(SplitStream<PyObject> splitStream) {
		super(splitStream);
	}

	/**
	 * A thin wrapper layer over {@link SplitStream#select(java.lang.String...)}.
	 *
	 * @param output_names The output names for which the operator will receive the
	 * input.
	 * @return Returns the selected {@link PythonDataStream}
	 */
	public PythonDataStream select(String... output_names) {
		return new PythonDataStream<>(this.stream.select(output_names));
	}
}
