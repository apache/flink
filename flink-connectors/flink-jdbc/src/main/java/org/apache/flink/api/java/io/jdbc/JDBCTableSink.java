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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * An at-least-once Table sink for JDBC.
 */
public class JDBCTableSink extends RichSinkFunction<Row>
	implements AppendStreamTableSink<Row>, CheckpointedFunction {
	private final JDBCOutputFormat outputFormat;

	private String[] fieldNames;
	private TypeInformation[] fieldTypes;

	public JDBCTableSink(JDBCOutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		dataStream.addSink(this);
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		JDBCTableSink copy = new JDBCTableSink(outputFormat);
		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}

	@Override
	public void invoke(Row value) throws Exception {
		outputFormat.writeRecord(value);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		RuntimeContext ctx = getRuntimeContext();
		outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
	}

	@Override
	public void close() throws Exception {
		outputFormat.close();
		super.close();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		outputFormat.flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}
}
