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

package org.apache.flink.walkthrough.common.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.walkthrough.common.sink.LoggerOutputFormat;

/**
 * A simple table sink for writing to stdout.
 */
@PublicEvolving
@SuppressWarnings("deprecation")
public class SpendReportTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

	private final TableSchema schema;

	public SpendReportTableSink() {
		this.schema = TableSchema
			.builder()
			.field("accountId", Types.LONG)
			.field("timestamp", Types.SQL_TIMESTAMP)
			.field("amount", Types.DOUBLE)
			.build();
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		dataSet
			.map(SpendReportTableSink::format)
			.output(new LoggerOutputFormat());
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		dataStream
			.map(SpendReportTableSink::format)
			.writeUsingOutputFormat(new LoggerOutputFormat())
			.setParallelism(dataStream.getParallelism());
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public DataType getConsumedDataType() {
		return getTableSchema().toRowDataType();
	}

	@Override
	public String[] getFieldNames() {
		return getTableSchema().getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return getTableSchema().getFieldTypes();
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return this;
	}

	private static String format(Row row) {
		//noinspection MalformedFormatString
		return String.format("%s, %s, $%.2f", row.getField(0), row.getField(1), row.getField(2));
	}
}
