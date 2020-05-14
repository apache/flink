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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

/**
 * Creates a TableSource to scan an HBase table.
 *
 * <p>The table name and required HBase configuration is passed during {@link HBaseTableSource} construction.
 * Use {@link #addColumn(String, String, Class)} to specify the family, qualifier, and type of columns to scan.
 *
 * <p>The TableSource returns {@link Row} with nested Rows for each column family.
 *
 * <p>The HBaseTableSource is used as shown in the example below.
 *
 * <pre>
 * {@code
 * HBaseTableSource hSrc = new HBaseTableSource(conf, "hTable");
 * hSrc.setRowKey("rowkey", String.class);
 * hSrc.addColumn("fam1", "col1", byte[].class);
 * hSrc.addColumn("fam1", "col2", Integer.class);
 * hSrc.addColumn("fam2", "col1", String.class);
 *
 * tableEnv.registerTableSource("hTable", hSrc);
 * Table res = tableEnv.sqlQuery(
 *   "SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t " +
 *   "WHERE t.rowkey LIKE 'flink%' GROUP BY t.fam2.col1");
 * }
 * </pre>
 */
@Internal
public class HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row>, StreamTableSource<Row>, LookupableTableSource<Row> {

	private final Configuration conf;
	private final String tableName;
	private final HBaseTableSchema hbaseSchema;
	private final int[] projectFields;

	/**
	 * The HBase configuration and the name of the table to read.
	 *
	 * @param conf      hbase configuration
	 * @param tableName the tableName
	 */
	public HBaseTableSource(Configuration conf, String tableName) {
		this(conf, tableName, new HBaseTableSchema(), null);
	}

	public HBaseTableSource(Configuration conf, String tableName, HBaseTableSchema hbaseSchema, int[] projectFields) {
		this.conf = conf;
		this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
		this.hbaseSchema = hbaseSchema;
		this.projectFields = projectFields;
	}

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		this.hbaseSchema.addColumn(family, qualifier, clazz);
	}

	/**
	 * Sets row key information in the table schema.
	 * @param rowKeyName the row key field name
	 * @param clazz the data type of the row key
	 */
	public void setRowKey(String rowKeyName, Class<?> clazz) {
		this.hbaseSchema.setRowKey(rowKeyName, clazz);
	}

	/**
	 * Specifies the charset to parse Strings to HBase byte[] keys and String values.
	 *
	 * @param charset Name of the charset to use.
	 */
	public void setCharset(String charset) {
		this.hbaseSchema.setCharset(charset);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		HBaseTableSchema projectedSchema = hbaseSchema.getProjectedHBaseTableSchema(projectFields);
		return projectedSchema.convertsToTableSchema().toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return hbaseSchema.convertsToTableSchema();
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		HBaseTableSchema projectedSchema = hbaseSchema.getProjectedHBaseTableSchema(projectFields);
		return execEnv
			.createInput(new HBaseRowInputFormat(conf, tableName, projectedSchema), getReturnType())
			.name(explainSource());
	}

	@Override
	public HBaseTableSource projectFields(int[] fields) {
		return new HBaseTableSource(this.conf, tableName, hbaseSchema, fields);
	}

	@Override
	public String explainSource() {
		return "HBaseTableSource[schema=" + Arrays.toString(getTableSchema().getFieldNames())
			+ ", projectFields=" + Arrays.toString(projectFields) + "]";
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		Preconditions.checkArgument(
			null != lookupKeys && lookupKeys.length == 1,
			"HBase table can only be retrieved by rowKey for now.");
		Preconditions.checkState(
			hbaseSchema.getRowKeyName().isPresent(),
			"HBase schema must have a row key when used in lookup mode.");
		Preconditions.checkState(
			hbaseSchema.getRowKeyName().get().equals(lookupKeys[0]),
			"The lookup key is not row key of HBase.");

		return new HBaseLookupFunction(
			this.conf,
			this.tableName,
			hbaseSchema.getProjectedHBaseTableSchema(projectFields));
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("HBase table doesn't support async lookup currently.");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public boolean isBounded() {
		// HBase source is always bounded.
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		HBaseTableSchema projectedSchema = hbaseSchema.getProjectedHBaseTableSchema(projectFields);
		return execEnv
			.createInput(new HBaseRowInputFormat(conf, tableName, projectedSchema), getReturnType())
			.name(explainSource());
	}

	@VisibleForTesting
	public HBaseTableSchema getHBaseTableSchema() {
		return this.hbaseSchema;
	}
}
