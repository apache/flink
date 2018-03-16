/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.PrintStream;

/**
 * Implementation of the SinkFunction writing every tuple to the standard
 * output or standard error stream.
 *
 * @param <IN>
 *            Input record type
 */
@PublicEvolving
public class PrintSinkFunction<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final boolean STD_OUT = false;
	private static final boolean STD_ERR = true;

	private boolean target;
	private transient PrintStream stream;
	private transient String prefix;

	/**
	 * Instantiates a print sink function that prints to standard out.
	 */
	public PrintSinkFunction() {}

	/**
	 * Instantiates a print sink function that prints to standard out.
	 *
	 * @param stdErr True, if the format should print to standard error instead of standard out.
	 */
	public PrintSinkFunction(boolean stdErr) {
		target = stdErr;
	}

	public void setTargetToStandardOut() {
		target = STD_OUT;
	}

	public void setTargetToStandardErr() {
		target = STD_ERR;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		// get the target stream
		stream = target == STD_OUT ? System.out : System.err;

		// set the prefix if we have a >1 parallelism
		prefix = (context.getNumberOfParallelSubtasks() > 1) ?
				((context.getIndexOfThisSubtask() + 1) + "> ") : null;
	}

	@Override
	public void invoke(IN record) {
		if (prefix != null) {
			stream.println(prefix + record.toString());
		}
		else {
			stream.println(record.toString());
		}
	}

	@Override
	public void close() {
		this.stream = null;
		this.prefix = null;
	}

	@Override
	public String toString() {
		return "Print to " + (target == STD_OUT ? "System.out" : "System.err");
	}
}
