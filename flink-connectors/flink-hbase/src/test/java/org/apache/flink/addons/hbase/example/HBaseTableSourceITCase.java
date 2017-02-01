/*
 * Copyright The Apache Software Foundation
 *
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
package org.apache.flink.addons.hbase.example;

import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.addons.hbase.HBaseTestingClusterAutostarter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HBaseTableSourceITCase extends HBaseTestingClusterAutostarter {

	private static final byte[] ROW_1 = Bytes.toBytes("row1");
	private static final byte[] ROW_2 = Bytes.toBytes("row2");
	private static final byte[] ROW_3 = Bytes.toBytes("row3");
	private static final byte[] F_1 = Bytes.toBytes("f1");
	private static final byte[] F_2 = Bytes.toBytes("f2");
	private static final byte[] Q_1 = Bytes.toBytes("q1");
	private static final byte[] Q_2 = Bytes.toBytes("q2");
	private static final byte[] Q_3 = Bytes.toBytes("q3");

	@BeforeClass
	public static void activateHBaseCluster(){
		registerHBaseMiniClusterInClasspath();
	}

	@Test
	public void testHBaseTableSourceWithSingleColumnFamily() throws Exception {
		// create a table with single region
		TableName tableName = TableName.valueOf("test");
		// no split keys
		byte[][] famNames = new byte[1][];
		famNames[0] = F_1;
		createTable(tableName, famNames, null);
		// get the htable instance
		HTable table = openTable(tableName);
		List<Put> puts = new ArrayList<Put>();
		// add some data
		Put put = new Put(ROW_1);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(100));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue"));
		// 3rd qual is long
		put.addColumn(F_1, Q_3, Bytes.toBytes(19991L));
		puts.add(put);

		put = new Put(ROW_2);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(101));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue1"));
		// 3rd qual is long
		put.addColumn(F_1, Q_3, Bytes.toBytes(19992L));
		puts.add(put);

		put = new Put(ROW_3);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(102));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue2"));
		// 3rd qual is long
		put.addColumn(F_1, Q_3, Bytes.toBytes(19993L));
		puts.add(put);
		// add the mutations to the table
		table.put(puts);
		table.close();
		// preparetion is done
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		HBaseTableSchema schema = new HBaseTableSchema();
		schema.addColumn(Bytes.toString(F_1), Bytes.toString(Q_1), Integer.class);
		schema.addColumn(Bytes.toString(F_1), Bytes.toString(Q_2), String.class);
		schema.addColumn(Bytes.toString(F_1), Bytes.toString(Q_3), Long.class);
		// fetch row2 from the table till the end
		BatchTableSource hbaseTable = new HBaseTableSource(getConf(), tableName.getNameAsString(), schema);
		tableEnv.registerTableSource("test", hbaseTable);
		Table result = tableEnv
			.sql("SELECT test.f1.q1, test.f1.q2, test.f1.q3 FROM test");
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "100,strvalue,19991\n" +
			"101,strvalue1,19992\n" +
			"102,strvalue2,19993\n";
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testHBaseTableSourceWithTwoColumnFamily() throws Exception {
		// create a table with single region
		TableName tableName = TableName.valueOf("test1");
		// no split keys
		byte[][] famNames = new byte[2][];
		famNames[0] = F_1;
		famNames[1] = F_2;
		createTable(tableName, famNames, null);
		// get the htable instance
		HTable table = openTable(tableName);
		List<Put> puts = new ArrayList<Put>();
		// add some data
		Put put = new Put(ROW_1);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(100));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue"));
		// 3rd qual is long
		put.addColumn(F_1, Q_3, Bytes.toBytes(19991L));
		puts.add(put);

		put = new Put(ROW_2);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_2, Q_1, Bytes.toBytes(201));
		//2nd qual is String
		put.addColumn(F_2, Q_2, Bytes.toBytes("newvalue1"));
		// 3rd qual is long
		put.addColumn(F_2, Q_3, Bytes.toBytes(29992L));
		puts.add(put);

		put = new Put(ROW_3);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(102));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue2"));
		// 3rd qual is long
		put.addColumn(F_1, Q_3, Bytes.toBytes(19993L));
		puts.add(put);
		// add the mutations to the table
		table.put(puts);
		table.close();
		// preparation is done
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		HBaseTableSchema schema = new HBaseTableSchema();
		schema.addColumn(Bytes.toString(F_1), Bytes.toString(Q_1), Integer.class);
		schema.addColumn(Bytes.toString(F_1), Bytes.toString(Q_2), String.class);
		schema.addColumn(Bytes.toString(F_1), Bytes.toString(Q_3), Long.class);
		schema.addColumn(Bytes.toString(F_2), Bytes.toString(Q_1), Integer.class);
		schema.addColumn(Bytes.toString(F_2), Bytes.toString(Q_2), String.class);
		schema.addColumn(Bytes.toString(F_2), Bytes.toString(Q_3), Long.class);
		// fetch row2 from the table till the end
		BatchTableSource hbaseTable = new HBaseTableSource(getConf(), tableName.getNameAsString(), schema);
		tableEnv.registerTableSource("test1", hbaseTable);
		Table result = tableEnv
			.sql("SELECT test1.f1.q1, test1.f1.q2, test1.f1.q3, test1.f2.q1, test1.f2.q2, test1.f2.q3 FROM test1");
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected = "100,strvalue,19991,null,null,null\n" +
			"null,null,null,201,newvalue1,29992\n" +
			"102,strvalue2,19993,null,null,null\n";
		TestBaseUtils.compareResultAsText(results, expected);
	}
}
