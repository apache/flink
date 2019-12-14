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
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * An at-least-once Table sink for JDBC.
 *
 * <p>The mechanisms of Flink guarantees delivering messages at-least-once to this sink (if
 * checkpointing is enabled). However, one common use case is to run idempotent queries
 * (e.g., <code>REPLACE</code> or <code>INSERT OVERWRITE</code>) to upsert into the database and
 * achieve exactly-once semantic.</p>
 */
public class JDBCAppendTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

	private final JDBCOutputFormat outputFormat;

	private String[] fieldNames;
	private TypeInformation[] fieldTypes;

	JDBCAppendTableSink(JDBCOutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	public static JDBCAppendTableSinkBuilder builder() {
		return new JDBCAppendTableSinkBuilder();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return dataStream
			.addSink(new JDBCSinkFunction(outputFormat))
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), fieldNames));
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		dataSet.output(outputFormat);
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
		int[] types = outputFormat.getTypesArray();

		String sinkSchema =
			String.join(", ", IntStream.of(types).mapToObj(JDBCTypeUtil::getTypeName).collect(Collectors.toList()));
		String tableSchema =
			String.join(", ", Stream.of(fieldTypes).map(JDBCTypeUtil::getTypeName).collect(Collectors.toList()));
		String msg = String.format("Schema of output table is incompatible with JDBCAppendTableSink schema. " +
			"Table schema: [%s], sink schema: [%s]", tableSchema, sinkSchema);

		Preconditions.checkArgument(fieldTypes.length == types.length, msg);
		for (int i = 0; i < types.length; ++i) {
			Preconditions.checkArgument(
				JDBCTypeUtil.typeInformationToSqlType(fieldTypes[i]) == types[i],
				msg);
		}

		JDBCAppendTableSink copy;
		try {
			copy = new JDBCAppendTableSink(InstantiationUtil.clone(outputFormat));
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}

	@VisibleForTesting
	JDBCOutputFormat getOutputFormat() {
		return outputFormat;
	}
}
