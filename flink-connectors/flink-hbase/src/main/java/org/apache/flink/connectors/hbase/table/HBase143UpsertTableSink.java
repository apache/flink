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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hbase.streaming.HBase143Writer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.sinks.BatchCompatibleStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A simple UpsertTableSink for HBase table, can be used for both streaming and batch.
 * Use a flat row type: Row(rowKey, family1.qualifier1, family1.qualifier2, family2.qualifier1 ...).
 */
public class HBase143UpsertTableSink
		implements UpsertStreamTableSink<Row>, BatchCompatibleStreamTableSink<Tuple2<Boolean, Row>> {

	private RichTableSchema tableSchema; // for table type info
	private String hbaseTableName;
	private HBaseTableSchemaV2 hbaseTableSchema; // for writing to hbase
	private int rowKeyIndex;
	private List<Integer> qualifierSourceIndexes;
	private org.apache.hadoop.conf.Configuration hbaseConfiguration;

	public HBase143UpsertTableSink(
			RichTableSchema tableSchema,
			String hbaseTableName,
			HBaseTableSchemaV2 hbaseTableSchema,
			int rowKeyIndex,
			List<Integer> qualifierSourceIndexes,
			Configuration hbaseConfiguration) {
		// pre check for tableAPI code path
		Preconditions.checkArgument(null != tableSchema && null != hbaseTableSchema, "given sql and hbase schemas should not be null!");
		// only support single sql primary key mapping to hbase rowKey for now.
		Preconditions.checkArgument(1 == tableSchema.getPrimaryKeys().size(),
			"A single primary key sql schema is necessary for the HBaseBridgeTable!");
		int hbaseQualifierCnt = hbaseTableSchema.getFamilySchema().getTotalQualifiers();
		int sqlColumnCnt = tableSchema.getColumnNames().length;
		Preconditions.checkArgument(hbaseQualifierCnt + 1 == sqlColumnCnt,
			"the given hbase schema's qualifier number(" + hbaseQualifierCnt +
			") plus one rowKey column is not consist with sql schema's column number(" + sqlColumnCnt + ")!");
		this.tableSchema = tableSchema;
		this.hbaseTableName = hbaseTableName;
		this.hbaseTableSchema = hbaseTableSchema;
		this.rowKeyIndex = rowKeyIndex;
		this.qualifierSourceIndexes = qualifierSourceIndexes;
		this.hbaseConfiguration = hbaseConfiguration;
	}

	@Override
	public void setKeyFields(String[] keys) {
		// do nothing
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// do nothing for now
	}

	@Override
	public DataStreamSink<Tuple2<Boolean, Row>> emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		HBase143Writer sink = null;
		try {
			sink = new HBase143Writer(hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, hbaseConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("encounter exception when prepare emitDataStream", e);
		}
		return dataStream.addSink(sink).name(sink.toString());
	}

	@Override
	public DataStreamSink<?> emitBoundedStream(DataStream<Tuple2<Boolean, Row>> boundedStream) {
		HBase143Writer sink = null;
		try {
			sink = new HBase143Writer(hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, hbaseConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("encounter exception when prepare emitBoundedStream", e);
		}
		return boundedStream.addSink(sink).name(sink.toString());
	}

	public TableSink<Tuple2<Boolean, Row>> copy() {
		return new HBase143UpsertTableSink(tableSchema, hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, hbaseConfiguration);
	}

	@Override
	public String[] getFieldNames() {
		return tableSchema.getColumnNames();
	}

	@Override
	public DataType[] getFieldTypes() {
		return tableSchema.getColumnTypes();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, DataType[] fieldTypes) {
		// only match fieldTypes, do not care fieldNames
		Preconditions.checkArgument(Arrays.equals(fieldTypes, getFieldTypes()),
			"Reconfiguration with different field types is not allowed. Expected: " + Arrays.toString(getFieldNames()) + " / "
			+ Arrays.toString(getFieldTypes()) + ". But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		return copy();
	}

	@Override
	public DataType getRecordType() {
		return tableSchema.getResultRowType();
	}

	@Override
	public DataType getOutputType() {
		return DataTypes.createTupleType(DataTypes.BOOLEAN, getRecordType());
	}
}
