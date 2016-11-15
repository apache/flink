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
package org.apache.flink.streaming.python.api.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;

import java.io.IOException;

/**
 * The {@code PythonGeneratorFunction} is a thin wrapper layer over a Python UDF {@code SourceFunction}.
 * It receives a {@code SourceFunction} as an input and keeps it internally in a serialized form.
 * It is then delivered, as part of the job graph, up to the TaskManager, then it is opened and becomes
 * a sort of mediator to the Python UDF {@code SourceFunction}.
 *
 * <p>This function is used internally by the Python thin wrapper layer over the streaming data
 * functionality</p>
 */
public class PythonGeneratorFunction extends RichSourceFunction<Object> {
	private static final long serialVersionUID = 3854587935845323082L;

	private final byte[] serFun;
	private transient SourceFunction<Object> fun;

	public PythonGeneratorFunction(SourceFunction<Object> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		this.fun = (SourceFunction<Object>) UtilityFunctions.smartFunctionDeserialization(
			getRuntimeContext(), this.serFun);
	}

	@Override
	public void close() throws Exception {}

	public void run(SourceContext<Object> ctx) throws Exception {
		this.fun.run(ctx);
	}

	public void cancel() {
		if (this.fun != null) {
			this.fun.cancel();
		}
	}
}
