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

package org.apache.flink.connector.hbase2.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.connector.hbase.source.AbstractHBaseTableSource;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;

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
 * tableEnv.registerTableSourceInternal("hTable", hSrc);
 * Table res = tableEnv.sqlQuery(
 *   "SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t " +
 *   "WHERE t.rowkey LIKE 'flink%' GROUP BY t.fam2.col1");
 * }
 * </pre>
 */
@Internal
public class HBaseTableSource extends AbstractHBaseTableSource {

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
		super(conf, tableName, hbaseSchema, projectFields);
	}

	@Override
	public HBaseTableSource projectFields(int[] fields) {
		return new HBaseTableSource(conf, tableName, hbaseSchema, fields);
	}

	@Override
	public InputFormat<Row, ?> getInputFormat(HBaseTableSchema projectedSchema) {
		return new HBaseRowInputFormat(conf, tableName, projectedSchema);
	}
}
