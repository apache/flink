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

import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.addons.hbase.HBaseTestingClusterAutostarter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import scala.tools.cmd.gen.AnyVals;

import java.util.ArrayList;
import java.util.List;


public class HBaseTableSourceITCase extends HBaseTestingClusterAutostarter {


	public static final byte[] ROW_1 = Bytes.toBytes("row1");
	public static final byte[] ROW_2 = Bytes.toBytes("row2");
	public static final byte[] ROW_3 = Bytes.toBytes("row3");
	public static final byte[] F_1 = Bytes.toBytes("f1");
	public static final byte[] Q_1 = Bytes.toBytes("q1");
	public static final byte[] Q_2 = Bytes.toBytes("q2");
	public static final byte[] Q_3 = Bytes.toBytes("q3");

	@Test
	public void testHBaseTableSource() throws Exception {
		// create a table with single region
		TableName tableName = TableName.valueOf("test");
		createTable(tableName, F_1, new byte[1][]);
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
		put.addColumn(F_1, Q_2, Bytes.toBytes(19991l));
		puts.add(put);

		put = new Put(ROW_2);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(101));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue1"));
		// 3rd qual is long
		put.addColumn(F_1, Q_2, Bytes.toBytes(19992l));
		puts.add(put);

		put = new Put(ROW_3);
		// add 3 qualifiers per row
		//1st qual is integer
		put.addColumn(F_1, Q_1, Bytes.toBytes(102));
		//2nd qual is String
		put.addColumn(F_1, Q_2, Bytes.toBytes("strvalue2"));
		// 3rd qual is long
		put.addColumn(F_1, Q_2, Bytes.toBytes(19993l));
		puts.add(put);
		// add the mutations to the table
		table.put(puts);
		table.close();
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, 	new TableConfig());
		String[] colNames = new String[3];
		colNames[0] = Bytes.toString(F_1)+":"+Bytes.toString(Q_1);
		colNames[1] = Bytes.toString(F_1)+":"+Bytes.toString(Q_2);
		colNames[2] = Bytes.toString(F_1)+":"+Bytes.toString(Q_3);
		TypeInformation<?>[] typeInfos = new TypeInformation<?>[3];
		typeInfos[0] = BasicTypeInfo.INT_TYPE_INFO;
		typeInfos[1] = BasicTypeInfo.STRING_TYPE_INFO;
		typeInfos[2] = BasicTypeInfo.LONG_TYPE_INFO;
		// fetch row2 from the table till the end
		BatchTableSource hbaseTable = new HBaseTableSource(getConf(), tableName.getNameAsString(), ROW_2, colNames, typeInfos);

		tableEnv.registerTableSource("test", hbaseTable);
		Table result = tableEnv.scan("test")
			.select("q1, q2, q3");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
	}
}
