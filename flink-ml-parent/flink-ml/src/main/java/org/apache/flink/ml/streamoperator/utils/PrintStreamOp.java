/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.streamoperator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;

import java.io.PrintStream;

/**
 * Print operator for streaming data.
 */
public class PrintStreamOp extends StreamOperator {

	public PrintStreamOp() {
		super(null);
	}

	public PrintStreamOp(Params params) {
		super(params);
	}

	public static void setStreamPrintStream(PrintStream printStream) {
		System.setErr(printStream);
	}

	@Override
	public StreamOperator linkFrom(StreamOperator in) {
		try {
			RowTypeDataStream.fromTable(in.getTable()).addSink(new StreamPrintSinkFunction());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		this.table = in.getTable();
		return this;
	}

	/**
	 * Sink function for streaming print.
	 */
	public static class StreamPrintSinkFunction extends RichSinkFunction <Row> {
		private static final long serialVersionUID = 1L;
		private transient PrintStream stream;
		private transient String prefix;

		public StreamPrintSinkFunction() {
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			StreamingRuntimeContext context = (StreamingRuntimeContext) this.getRuntimeContext();
			this.stream = System.err;
			this.prefix = context.getNumberOfParallelSubtasks() > 1 ? context.getIndexOfThisSubtask() + 1 + "> " :
				null;
		}

		public void invoke(Row record) {
			if (this.prefix != null) {
				this.stream.println(this.prefix + record.toString());
			} else {
				this.stream.println(record.toString());
			}

		}

		@Override
		public void close() {
			this.stream = null;
			this.prefix = null;
		}

		@Override
		public String toString() {
			return "Print to " + this.stream.toString();
		}
	}
}

