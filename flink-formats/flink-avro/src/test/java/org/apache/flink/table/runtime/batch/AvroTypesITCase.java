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

package org.apache.flink.table.runtime.batch;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed16;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.runtime.utils.TableProgramsClusterTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.apache.avro.util.Utf8;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for interoperability with Avro types.
 */
@RunWith(Parameterized.class)
public class AvroTypesITCase extends TableProgramsClusterTestBase {

	private static final User USER_1 = User.newBuilder()
			.setName("Charlie")
			.setFavoriteColor("blue")
			.setFavoriteNumber(null)
			.setTypeBoolTest(false)
			.setTypeDoubleTest(1.337d)
			.setTypeNullTest(null)
			.setTypeLongTest(1337L)
			.setTypeArrayString(new ArrayList<>())
			.setTypeArrayBoolean(new ArrayList<>())
			.setTypeNullableArray(null)
			.setTypeEnum(Colors.RED)
			.setTypeMap(new HashMap<>())
			.setTypeFixed(null)
			.setTypeUnion(null)
			.setTypeNested(
					Address.newBuilder()
							.setNum(42)
							.setStreet("Bakerstreet")
							.setCity("Berlin")
							.setState("Berlin")
							.setZip("12049").build())
			.setTypeBytes(ByteBuffer.allocate(10))
			.setTypeDate(LocalDate.parse("2014-03-01"))
			.setTypeTimeMillis(LocalTime.parse("12:12:12"))
			.setTypeTimeMicros(123456)
			.setTypeTimestampMillis(DateTime.parse("2014-03-01T12:12:12.321Z"))
			.setTypeTimestampMicros(123456L)
			.setTypeDecimalBytes(ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.setTypeDecimalFixed(new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.build();

	private static final User USER_2 = User.newBuilder()
			.setName("Whatever")
			.setFavoriteNumber(null)
			.setFavoriteColor("black")
			.setTypeLongTest(42L)
			.setTypeDoubleTest(0.0)
			.setTypeNullTest(null)
			.setTypeBoolTest(true)
			.setTypeArrayString(Collections.singletonList("hello"))
			.setTypeArrayBoolean(Collections.singletonList(true))
			.setTypeEnum(Colors.GREEN)
			.setTypeMap(new HashMap<>())
			.setTypeFixed(new Fixed16())
			.setTypeUnion(null)
			.setTypeNested(null).setTypeDate(LocalDate.parse("2014-03-01"))
			.setTypeBytes(ByteBuffer.allocate(10))
			.setTypeTimeMillis(LocalTime.parse("12:12:12"))
			.setTypeTimeMicros(123456)
			.setTypeTimestampMillis(DateTime.parse("2014-03-01T12:12:12.321Z"))
			.setTypeTimestampMicros(123456L)
			.setTypeDecimalBytes(ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.setTypeDecimalFixed(new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.build();

	private static final User USER_3 = User.newBuilder()
			.setName("Terminator")
			.setFavoriteNumber(null)
			.setFavoriteColor("yellow")
			.setTypeLongTest(1L)
			.setTypeDoubleTest(0.0)
			.setTypeNullTest(null)
			.setTypeBoolTest(false)
			.setTypeArrayString(Collections.singletonList("world"))
			.setTypeArrayBoolean(Collections.singletonList(false))
			.setTypeEnum(Colors.GREEN)
			.setTypeMap(new HashMap<>())
			.setTypeFixed(new Fixed16())
			.setTypeUnion(null)
			.setTypeNested(null)
			.setTypeBytes(ByteBuffer.allocate(10))
			.setTypeDate(LocalDate.parse("2014-03-01"))
			.setTypeTimeMillis(LocalTime.parse("12:12:12"))
			.setTypeTimeMicros(123456)
			.setTypeTimestampMillis(DateTime.parse("2014-03-01T12:12:12.321Z"))
			.setTypeTimestampMicros(123456L)
			.setTypeDecimalBytes(ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.setTypeDecimalFixed(new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.build();

	public AvroTypesITCase(
			TestExecutionMode executionMode,
			TableConfigMode tableConfigMode) {
		super(executionMode, tableConfigMode);
	}

	@Test
	public void testAvroToRow() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().registerTypeWithKryoSerializer(LocalDate.class, AvroKryoSerializerUtils.JodaLocalDateSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(LocalTime.class, AvroKryoSerializerUtils.JodaLocalTimeSerializer.class);
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env, config());

		Table t = tEnv.fromDataSet(testData(env));
		Table result = t.select("*");

		List<Row> results = tEnv.toDataSet(result, Row.class).collect();
		String expected =
			"black,null,Whatever,[true],[hello],true,java.nio.HeapByteBuffer[pos=0 lim=10 cap=10]," +
			"2014-03-01,java.nio.HeapByteBuffer[pos=0 lim=2 cap=2],[7, -48],0.0,GREEN," +
			"[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],42,{},null,null,null,123456," +
			"12:12:12.000,123456,2014-03-01T12:12:12.321Z,null\n" +
			"blue,null,Charlie,[],[],false,java.nio.HeapByteBuffer[pos=0 lim=10 cap=10],2014-03-01," +
			"java.nio.HeapByteBuffer[pos=0 lim=2 cap=2],[7, -48],1.337,RED,null,1337,{}," +
			"{\"num\": 42, \"street\": \"Bakerstreet\", \"city\": \"Berlin\", \"state\": " +
			"\"Berlin\", \"zip\": \"12049\"},null,null,123456,12:12:12.000,123456," +
			"2014-03-01T12:12:12.321Z,null\n" +
			"yellow,null,Terminator,[false],[world],false," +
			"java.nio.HeapByteBuffer[pos=0 lim=10 cap=10],2014-03-01," +
			"java.nio.HeapByteBuffer[pos=0 lim=2 cap=2],[7, -48],0.0,GREEN," +
			"[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],1,{},null,null,null,123456," +
			"12:12:12.000,123456,2014-03-01T12:12:12.321Z,null";
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testAvroStringAccess() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env, config());

		Table t = tEnv.fromDataSet(testData(env));
		Table result = t.select("name");
		List<Utf8> results = tEnv.toDataSet(result, Types.GENERIC(Utf8.class)).collect();
		String expected = "Charlie\n" +
				"Terminator\n" +
				"Whatever";
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testAvroObjectAccess() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env, config());

		Table t = tEnv.fromDataSet(testData(env));
		Table result = t
				.filter("type_nested.isNotNull")
				.select("type_nested.flatten()").as("city, num, state, street, zip");

		List<Address> results = tEnv.toDataSet(result, Types.POJO(Address.class)).collect();
		String expected = USER_1.getTypeNested().toString();
		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testAvroToAvro() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env, config());

		Table t = tEnv.fromDataSet(testData(env));
		Table result = t.select("*");

		List<User> results = tEnv.toDataSet(result, Types.POJO(User.class)).collect();
		List<User> expected = Arrays.asList(USER_1, USER_2, USER_3);
		assertEquals(expected, results);
	}

	private DataSet<User> testData(ExecutionEnvironment env) {
		List<User> data = new ArrayList<>(3);
		data.add(USER_1);
		data.add(USER_2);
		data.add(USER_3);
		return env.fromCollection(data);
	}
}
