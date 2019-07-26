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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

class JDBCUpsertSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> implements CheckpointedFunction {
	private final JDBCUpsertOutputFormat outputFormat;

	JDBCUpsertSinkFunction(JDBCUpsertOutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		RuntimeContext ctx = getRuntimeContext();
		outputFormat.setRuntimeContext(ctx);
		outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
		outputFormat.writeRecord(value);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		outputFormat.flush();
	}

	@Override
	public void close() throws Exception {
		outputFormat.close();
	}
}
