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
package org.apache.flink.languagebinding.api.java.python.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import org.apache.flink.util.Collector;
import java.io.IOException;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;

/**
 * Multi-purpose class, used for Combine-operations using a python script, and as a preprocess step for
 * GroupReduce-operations.
 *
 * @param <IN>
 */
public class PythonCombineIdentity<IN> extends RichGroupReduceFunction<IN, IN> {
	private PythonStreamer streamer;

	public PythonCombineIdentity() {
		streamer = null;
	}

	public PythonCombineIdentity(int id) {
		streamer = new PythonStreamer(this, id);
	}

	@Override
	public void open(Configuration config) throws IOException {
		if (streamer != null) {
			streamer.open();
			streamer.sendBroadCastVariables(config);
		}
	}

	/**
	 * Calls the external python function.
	 *
	 * @param values function input
	 * @param out collector
	 * @throws IOException
	 */
	@Override
	public final void reduce(Iterable<IN> values, Collector<IN> out) throws Exception {
		for (IN value : values) {
			out.collect(value);
		}
	}

	/**
	 * Calls the external python function.
	 *
	 * @param values function input
	 * @param out collector
	 * @throws IOException
	 */
	@Override
	public final void combine(Iterable<IN> values, Collector<IN> out) throws Exception {
		streamer.streamBufferWithoutGroups(values.iterator(), out);
	}

	@Override
	public void close() throws IOException {
		if (streamer != null) {
			streamer.close();
			streamer = null;
		}
	}
}
