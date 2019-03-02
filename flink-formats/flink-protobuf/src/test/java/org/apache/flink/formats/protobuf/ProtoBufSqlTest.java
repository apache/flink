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

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.ProtoTestUtils.ProtoTestData;
import org.apache.flink.formats.protobuf.test.Models.Person;
import org.apache.flink.formats.protobuf.test.Models.PhoneNumber;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Test sql execution on protobuf objects.
 */
public class ProtoBufSqlTest {

	private StreamTableEnvironment tableEnv;
	private StreamExecutionEnvironment env;
	private ProtoTestData testData;

	@Before
	public void before() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();

		ProtoFlinkUtils.registerProtobufSerDeWithKryo(env);
		env.setParallelism(1);

		TestProtoTableSink.clear();

		testData = ProtoTestUtils.getTestData();

		SingleOutputStreamOperator personStream =
			env.fromElements(testData.protoObj)
				.returns((TypeInformation) new ProtoTypeInfo<>(Person.class));

		tableEnv = StreamTableEnvironment.create(env);
		tableEnv.registerDataStream("persons", personStream);
	}

	@Test
	public void testSqlSelectAll() throws Exception {
		tableEnv.sqlQuery("select * from persons").writeToSink(new TestProtoTableSink(Person.class));

		env.execute();

		// oneof & enums are not supported yet and won't survive sql transformation,
		// hence remove it before comparing
		Person expectedPerson =
			((Person) testData.protoObj).toBuilder().clearPrimaryEmail().clearMood().build();

		Person actualPerson = Person.parseFrom(TestProtoTableSink.MESSAGE_BYTES.get(0));
		assertEquals(expectedPerson, actualPerson);
	}

	@Test
	public void testNestedObjectExtraction() throws Exception {
		tableEnv
			.sqlQuery("select previousPhoneNumber_.* from persons")
			.writeToSink(new TestProtoTableSink(PhoneNumber.class));
		env.execute();

		PhoneNumber expectedPhoneNumber = ((Person) testData.protoObj).getPreviousPhoneNumber();
		((Person) testData.protoObj).toBuilder().clearPrimaryEmail().clearMood().build();

		PhoneNumber actualPhoneNumber = PhoneNumber
			.parseFrom(TestProtoTableSink.MESSAGE_BYTES.get(0));
		assertEquals(expectedPhoneNumber, actualPhoneNumber);
	}
}
