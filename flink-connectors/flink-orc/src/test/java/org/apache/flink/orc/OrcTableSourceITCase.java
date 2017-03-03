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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.Row;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link OrcTableSource}.
 */
public class OrcTableSourceITCase extends MultipleProgramsTestBase {

	private static final String TEST1_SCHEMA = "struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int," +
		"long1:bigint,float1:float,double1:double,bytes1:binary,string1:string," +
		"middle:struct<list:array<struct<int1:int,string1:string>>>," +
		"list:array<struct<int1:int,string1:string>>," +
		"map:map<string,struct<int1:int,string1:string>>>";

	private final URL test1URL = getClass().getClassLoader().getResource("TestOrcFile.test1.orc");


	private static final String[] TEST1_DATA = new String[] {
		"false,1,1024,65536,9223372036854775807,1.0,-15.0,[0, 1, 2, 3, 4],hi,[1,bye, 2,sigh],[3,good, 4,bad],{}",
		"true,100,2048,65536,9223372036854775807,2.0,-5.0,[],bye,[1,bye, 2,sigh]," +
			"[100000000,cat, -100000,in, 1234,hat],{chani=5,chani, mauddib=1,mauddib}" };

	public OrcTableSourceITCase() {
		super(TestExecutionMode.COLLECTION);
	}

	@Test
	public void testOrcTableSource() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		assert (test1URL != null);
		OrcTableSource orc = new OrcTableSource(test1URL.getPath(), TEST1_SCHEMA);

		tEnv.registerTableSource("orcTable", orc);

		String query = "Select * from orcTable";
		Table t = tEnv.sql(query);

		DataSet<Row> dataSet = tEnv.toDataSet(t, Row.class);
		List<Row> records = dataSet.collect();

		Assert.assertEquals(records.size(), 2);

		List<String> actualRecords = new ArrayList<>();
		for (Row record : records) {
			Assert.assertEquals(record.getArity(), 12);
			actualRecords.add(record.toString());
		}

		Assert.assertThat(actualRecords, CoreMatchers.hasItems(TEST1_DATA));
	}

	@Test
	public void testOrcTableProjection() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		assert(test1URL != null);
		OrcTableSource orc = new OrcTableSource(test1URL.getPath(), TEST1_SCHEMA);

		tEnv.registerTableSource("orcTable", orc);

		String query = "Select middle,list,map from orcTable";
		Table t = tEnv.sql(query);

		String[] colNames = new String[] {"middle", "list", "map"};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			new TypeInformation[] {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"int1", "string1"});

		RowTypeInfo structTypeInfo = new RowTypeInfo(
			new TypeInformation[] {ObjectArrayTypeInfo.getInfoFor(rowTypeInfo)},
			new String[] {"list"});

		TypeInformation[] colTypes = new TypeInformation[] {
			structTypeInfo,
			ObjectArrayTypeInfo.getInfoFor(rowTypeInfo),
			new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, rowTypeInfo)
		};

		TableSchema actualTableSchema = new TableSchema(colNames, colTypes);

		Assert.assertArrayEquals(t.getSchema().getColumnNames(), colNames);
		Assert.assertArrayEquals(t.getSchema().getTypes(), colTypes);
		Assert.assertEquals(actualTableSchema.toString(), t.getSchema().toString());

		DataSet<Row> dataSet = tEnv.toDataSet(t, Row.class);
		List<Row> records = dataSet.collect();

		Assert.assertEquals(records.size(), 2);
		for (Row record: records) {
			Assert.assertEquals(record.getArity(), 3);
		}

	}

}
