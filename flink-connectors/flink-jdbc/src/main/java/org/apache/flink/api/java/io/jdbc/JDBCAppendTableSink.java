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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * An at-least-once Table sink for JDBC.
 *
 * <p>The mechanisms of Flink guarantees delivering messages at-least-once to this sink.
 * However, one common use case is to run idempotent queries (e.g., <code>REPLACE</code> or
 * <code>INSERT OVERWRITE</code>) to upsert into the database and achieve exactly-once semantic.</p>
 */
public class JDBCAppendTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {
	private final JDBCSinkFunction sink;

	private String[] fieldNames;
	private TypeInformation[] fieldTypes;

	JDBCAppendTableSink(JDBCOutputFormat outputFormat) {
		this.sink = new JDBCSinkFunction(outputFormat);
	}

	public static JDBCAppendTableSinkBuilder builder() {
		return new JDBCAppendTableSinkBuilder();
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		dataStream.addSink(sink);
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		dataSet.output(sink.outputFormat);
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
		int[] types = sink.outputFormat.getTypesArray();
		Preconditions.checkArgument(fieldTypes.length == types.length);
		for (int i = 0; i < types.length; ++i) {
			Preconditions.checkArgument(JDBCTypeUtil.typeInformationToSqlType(fieldTypes[i]) == types[i],
				"Incompatible types between fields and JDBC format at " + i);
		}

		JDBCAppendTableSink copy = new JDBCAppendTableSink(sink.outputFormat);
		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}

	@VisibleForTesting
	JDBCSinkFunction getSink() {
		return sink;
	}
}
