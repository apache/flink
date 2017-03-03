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
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

/**
 * Unit Tests for {@link OrcTableSource}.
 */
public class OrcTableSourceTest {

	private static final String TEST1_SCHEMA = "struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int," +
		"long1:bigint,float1:float,double1:double,bytes1:binary,string1:string," +
		"middle:struct<list:array<struct<int1:int,string1:string>>>," +
		"list:array<struct<int1:int,string1:string>>," +
		"map:map<string,struct<int1:int,string1:string>>>";

	private final URL test1URL = getClass().getClassLoader().getResource("TestOrcFile.test1.orc");

	@Test
	public void testOrcSchema() throws Exception {

		assert(test1URL != null);
		OrcTableSource orc = new OrcTableSource(test1URL.getPath(), TEST1_SCHEMA);

		String expectedSchema = "Row(boolean1: Boolean, byte1: Byte, short1: Short, int1: Integer, long1: Long, " +
			"float1: Float, double1: Double, bytes1: byte[], string1: String, " +
			"middle: Row(list: ObjectArrayTypeInfo<Row(int1: Integer, string1: String)>), " +
			"list: ObjectArrayTypeInfo<Row(int1: Integer, string1: String)>, " +
			"map: Map<String, Row(int1: Integer, string1: String)>)";

		Assert.assertEquals(expectedSchema, orc.getReturnType().toString());

	}

	@Test
	public void testOrcTableSchema() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		assert(test1URL != null);
		OrcTableSource orc = new OrcTableSource(test1URL.getPath(), TEST1_SCHEMA);

		tEnv.registerTableSource("orcTable", orc);
		String query = "Select * from orcTable";
		Table t = tEnv.sql(query);

		String[] colNames = new String[] {
			"boolean1", "byte1", "short1", "int1", "long1", "float1",
			"double1", "bytes1", "string1", "list", "list0", "map"
		};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			new TypeInformation[] {
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"int1", "string1"});

		TypeInformation[] colTypes = new TypeInformation[] {
			BasicTypeInfo.BOOLEAN_TYPE_INFO,
			BasicTypeInfo.BYTE_TYPE_INFO,
			BasicTypeInfo.SHORT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.FLOAT_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			ObjectArrayTypeInfo.getInfoFor(rowTypeInfo),
			ObjectArrayTypeInfo.getInfoFor(rowTypeInfo),
			new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, rowTypeInfo)
		};
		TableSchema expectedTableSchema = new TableSchema(colNames, colTypes);

		Assert.assertArrayEquals(t.getSchema().getColumnNames(), colNames);
		Assert.assertArrayEquals(t.getSchema().getTypes(), colTypes);
		Assert.assertEquals(expectedTableSchema.toString(), t.getSchema().toString());

	}

}
