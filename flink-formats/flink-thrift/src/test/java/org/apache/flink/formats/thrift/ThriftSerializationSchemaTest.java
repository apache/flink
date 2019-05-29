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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.thrift.generated.StructWithBinaryField;
import org.apache.flink.formats.thrift.typeutils.ThriftTypeInfo;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.formats.thrift.utils.ThriftOutputFormat;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Tests for thrift serialization sink.
 */
public class ThriftSerializationSchemaTest {

	@Test
	public void thriftOutputFormatSerializationTest() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.registerType(StructWithBinaryField.class);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
		List<StructWithBinaryField> messages = TestUtils.generateStructWithBinaryFieldTestData();

		DataSet<StructWithBinaryField> eventDataSet = env.fromCollection(messages,
			new ThriftTypeInfo(StructWithBinaryField.class, ThriftCodeGenerator.THRIFT));
		tEnv.registerDataSet("binary_field_struct", eventDataSet);
		String querySql = "SELECT `id`, segmentIds, `value`, nums FROM binary_field_struct ";
		System.out.println(querySql);

		Table table = tEnv.sqlQuery(querySql);
		DataSet<StructWithBinaryField> result = tEnv.toDataSet(table,
			new ThriftTypeInfo(StructWithBinaryField.class, ThriftCodeGenerator.THRIFT));

		File resultFile = File.createTempFile("thriftOutputFormat", ".txt");
		resultFile.deleteOnExit();
		System.out.println("result file path : " + resultFile);

		Path filePath = new Path(resultFile.getAbsolutePath());
		result.write(new ThriftOutputFormat<>(filePath, TSimpleJSONProtocol.class), resultFile.getAbsolutePath());
	}

	@Test
	public void thriftRowSerializationSchemaTest() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.registerType(StructWithBinaryField.class);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
		List<StructWithBinaryField> messages = TestUtils.generateStructWithBinaryFieldTestData();

		DataSet<StructWithBinaryField> eventDataSet = env.fromCollection(messages,
			new ThriftTypeInfo(StructWithBinaryField.class, ThriftCodeGenerator.THRIFT));
		tEnv.registerDataSet("binary_field_struct", eventDataSet);
		String querySql = "SELECT `id`, segmentIds, `value`, nums FROM binary_field_struct ";
		System.out.println(querySql);

		Table table = tEnv.sqlQuery(querySql);
		File resultFile = File.createTempFile("rowserialization", ".txt");
		resultFile.deleteOnExit();
		System.out.println("result file path : " + resultFile);

		DataSet<Row> result = tEnv.toDataSet(table, TypeInformation.of(Row.class));
		ThriftTableSink thriftTableSink =
			new ThriftTableSink(
				new ThriftRowSerializationSchema(StructWithBinaryField.class, table.getSchema(),
					TSimpleJSONProtocol.Factory.class),
				resultFile.getAbsolutePath(),
				TSimpleJSONProtocol.class);

		String[] fieldNames = table.getSchema().getFieldNames();
		TypeInformation[] typeInfos = new TypeInformation[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			typeInfos[i] = ThriftUtils.getFieldTypeInformation(StructWithBinaryField.class, fieldNames[i],
				ThriftCodeGenerator.THRIFT);
		}
		thriftTableSink = (ThriftTableSink) thriftTableSink.configure(fieldNames, typeInfos);
		tEnv.registerTableSink("my_sink", thriftTableSink);
		thriftTableSink.emitDataSet(result);
	}
}
