/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.functions;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.api.streaming.data.PythonDualInputStreamer;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * CoGroupFunction that uses a python script.
 *
 * @param <IN1>
 * @param <IN2>
 * @param <OUT>
 */
public class PythonCoGroup<IN1, IN2, OUT> extends RichCoGroupFunction<IN1, IN2, OUT> implements ResultTypeQueryable<OUT> {

	private static final long serialVersionUID = -3997396583317513873L;

	private final PythonDualInputStreamer<IN1, IN2, OUT> streamer;
	private final transient TypeInformation<OUT> typeInformation;

	public PythonCoGroup(Configuration config, int envID, int setID, TypeInformation<OUT> typeInformation) {
		this.typeInformation = typeInformation;
		streamer = new PythonDualInputStreamer<>(this, config, envID, setID, typeInformation instanceof PrimitiveArrayTypeInfo);
	}

	/**
	 * Opens this function.
	 *
	 * @param config configuration
	 * @throws IOException
	 */
	@Override
	public void open(Configuration config) throws IOException {
		streamer.open();
		streamer.sendBroadCastVariables(config);
	}

	/**
	 * Calls the external python function.
	 *
	 * @param first
	 * @param second
	 * @param out collector
	 * @throws IOException
	 */
	@Override
	public final void coGroup(Iterable<IN1> first, Iterable<IN2> second, Collector<OUT> out) throws Exception {
		streamer.streamBufferWithGroups(first.iterator(), second.iterator(), out);
	}

	/**
	 * Closes this function.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		streamer.close();
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return typeInformation;
	}
}
