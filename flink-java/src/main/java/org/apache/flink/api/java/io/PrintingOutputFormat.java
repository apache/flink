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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.PrintStream;

/**
 * Output format that prints results into either stdout or stderr.
 * @param <T>
 */
@PublicEvolving
public class PrintingOutputFormat<T> extends RichOutputFormat<T> {

	private static final long serialVersionUID = 1L;

	private static final boolean STD_OUT = false;
	private static final boolean STD_ERR = true;

	private String sinkIdentifier;

	private boolean target;

	private transient PrintStream stream;

	private transient String prefix;

	// --------------------------------------------------------------------------------------------

	/**
	 * Instantiates a printing output format that prints to standard out.
	 */
	public PrintingOutputFormat() {}

	/**
	 * Instantiates a printing output format that prints to standard out.
	 *
	 * @param stdErr True, if the format should print to standard error instead of standard out.
	 */
	public PrintingOutputFormat(boolean stdErr) {
		this.target = stdErr;
	}

	/**
	 * Instantiates a printing output format that prints to standard out with a prefixed message.
	 * @param sinkIdentifier Message that is prefixed to the output of the value.
	 * @param stdErr True, if the format should print to standard error instead of standard out.
	 */
	public PrintingOutputFormat(String sinkIdentifier, boolean stdErr) {
		this(stdErr);
		this.sinkIdentifier = sinkIdentifier;
	}

	public void setTargetToStandardOut() {
		this.target = STD_OUT;
	}

	public void setTargetToStandardErr() {
		this.target = STD_ERR;
	}

	@Override
	public void configure(Configuration parameters) {}

	@Override
	public void open(int taskNumber, int numTasks) {
		// get the target stream
		this.stream = this.target == STD_OUT ? System.out : System.err;

		/**
		 * Four possible format options:
		 *      sinkId:taskId> output  <- sink id provided, parallelism > 1
		 *      sinkId> output         <- sink id provided, parallelism == 1
		 *      taskId> output         <- no sink id provided, parallelism > 1
		 *      output                 <- no sink id provided, parallelism == 1
		 */
		if (this.sinkIdentifier != null) {
			this.prefix = this.sinkIdentifier;
			if (numTasks > 1) {
				this.prefix += ":" + (taskNumber + 1);
			}
			this.prefix += "> ";
		} else if (numTasks > 1) {
			this.prefix = (taskNumber + 1) + "> ";
		} else {
			this.prefix = "";
		}

	}

	@Override
	public void writeRecord(T record) {
		this.stream.println(this.prefix + record.toString());
	}

	@Override
	public void close() {
		this.stream = null;
		this.prefix = null;
		this.sinkIdentifier = null;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return "Print to " + (target == STD_OUT ? "System.out" : "System.err");
	}
}
