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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * A base HBaseTableSource for HBase, applicable for 1.4.3.
 * Use a flat row type: Row(rowKey, family1.qualifier1, family1.qualifier2, family2.qualifier1 ...).
 */
public class HBase143TableSource implements StreamTableSource<Row>, LookupableTableSource<Row>, BatchTableSource<Row> {
	private RichTableSchema tableSchema; // for table type info
	private String hbaseTableName;
	private HBaseTableSchemaV2 hbaseTableSchema; // for writing to hbase
	private int rowKeyIndex;
	private List<Integer> qualifierSourceIndexes;
	private org.apache.hadoop.conf.Configuration hbaseConfiguration;

	public HBase143TableSource(
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
	public TableFunction<Row> getLookupFunction(int[] lookupKeys) {
		Preconditions.checkArgument(null != lookupKeys && lookupKeys.length == 1, "HBase table can only be retrieved by rowKey for now.");
		try {
			return new HBaseLookupFunction(tableSchema, hbaseTableName, hbaseTableSchema, rowKeyIndex, qualifierSourceIndexes, hbaseConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("encounter an IOException when initialize the HBase143LookupFunction.", e);
		}
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(int[] lookupKeys) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public LookupConfig getLookupConfig() {
		// use default value for now.
		return new LookupConfig();
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public DataStream<Row> getBoundedStream(StreamExecutionEnvironment streamEnv) {
		throw new UnsupportedOperationException("HBase table can not convert to BoundedDataStream currently.");
	}

	@Override
	public DataType getReturnType() {
		// do not use composite row type : Row(rowKey, family: Row(qualifier [, qualifier]* [, family: Row...]* ))
		// just flat row type here
		return tableSchema.getResultRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		String rowKeyColumn = tableSchema.getColumnNames()[rowKeyIndex];
		return TableSchemaUtil.builderFromDataType(getReturnType()).primaryKey(rowKeyColumn).build();
	}

	@Override
	public String explainSource() {
		return "HBase[" + hbaseTableName + "], schema:{" + getReturnType() + "}";
	}

	@Override
	public TableStats getTableStats() {
		return null;
	}
}
