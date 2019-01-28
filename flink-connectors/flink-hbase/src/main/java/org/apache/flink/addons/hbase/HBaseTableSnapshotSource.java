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

package org.apache.flink.addons.hbase;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

/**
 * Creates a TableSource to scan an HBase table through HBase snapshot.
 *
 * <p>Subclass of {@link HBaseTableSource}.
 *
 * <p>For more information about HBase snapshot, please refer to
 * <a href="http://hbase.apache.org/book.html#ops.snapshots">HBase official document for snapshot</a>.
 *
 * <p>The table name and required HBase configuration is passed during {@link HBaseTableSnapshotSource} construction.
 * Use {@link #addColumn(String, String, Class)} to specify the family, qualifier, and type of columns to scan.
 *
 * <p>The TableSource returns {@link Row} with nested Rows for each column family.
 *
 * <p>The HBaseTableSnapshotSource is used as shown in the example below.
 *
 * <pre>
 * {@code
 * HBaseTableSnapshotSource hSrc = new HBaseTableSnapshotSource(conf, "hTable", "snapshotname", "/restore_path");
 * hSrc.addColumn("fam1", "col1", byte[].class);
 * hSrc.addColumn("fam1", "col2", Integer.class);
 * hSrc.addColumn("fam2", "col1", String.class);
 *
 * tableEnv.registerTableSource("hTable", hSrc);
 * Table res = tableEnv.sql("SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t GROUP BY t.fam2.col1");
 * }
 * </pre>
 */
public class HBaseTableSnapshotSource extends HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

	/**
	 * HBase snapshot name.
	 */
	private String snapshotName;

	/**
	 * HBase snapshot restore directory.
	 */
	private String restoreDirPath;

	/**
	 * The HBase configuration and the name of the table to read.
	 *
	 * @param conf           hbase configuration
	 * @param tableName      the tableName
	 * @param snapshotName   the snapshotName
	 * @param restoreDirPath the restore directory path
	 */
	public HBaseTableSnapshotSource(Configuration conf, String tableName, String snapshotName, String restoreDirPath) {
		super(conf, tableName);
		this.snapshotName = Preconditions.checkNotNull(snapshotName, "Snapshot name");
		this.restoreDirPath = Preconditions.checkNotNull(restoreDirPath, "Restore dir path name");
	}

	protected HBaseTableSnapshotSource(Configuration conf, String tableName, String snapshotName, String restoreDirPath, TableSchema tableSchema) {
		super(conf, tableName, tableSchema);
		this.snapshotName = Preconditions.checkNotNull(snapshotName, "Snapshot name");
		this.restoreDirPath = Preconditions.checkNotNull(restoreDirPath, "Restore dir path name");
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(new HBaseSnapshotRowInputFormat(conf, tableName, snapshotName, restoreDirPath,
			hBaseSchema), getReturnType()).name(explainSource());
	}
}
