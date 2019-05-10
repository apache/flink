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

package org.apache.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.formats.thrift.ThriftCodeGenerator;
import org.apache.flink.formats.thrift.generated.StructWithBinaryField;
import org.apache.flink.formats.thrift.typeutils.ThriftTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for flink sql using data in thrift format.
 */
public class ThriftFlinkSQLTest {

	private static List<StructWithBinaryField> generateTestData() {
		List<StructWithBinaryField> result = new ArrayList<>();
		StructWithBinaryField message = new StructWithBinaryField();
		message.setId(1000L);
		message.setSegmentIds("123456789".getBytes());
		result.add(message);
		return result;
	}

	@Test
	public void testFlinkSQLWithThriftBinaryField() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.registerType(StructWithBinaryField.class);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
		List<StructWithBinaryField> messages = generateTestData();

		DataSet<StructWithBinaryField> eventDataSet = env.fromCollection(messages,
			new ThriftTypeInfo(StructWithBinaryField.class, ThriftCodeGenerator.SCROOGE));
		tEnv.registerDataSet("binary_field_struct", eventDataSet);
		String querySql = "SELECT segmentIds FROM binary_field_struct\n ";

		System.out.println(querySql);
		Table table = tEnv.sqlQuery(querySql);
		DataSet<ByteBuffer> result = tEnv.toDataSet(table, TypeInformation.of(ByteBuffer.class));
		result.print();
	}
}
