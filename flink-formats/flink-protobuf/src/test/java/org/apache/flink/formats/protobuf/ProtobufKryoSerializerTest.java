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

import org.apache.flink.formats.protobuf.ProtoTestUtils.ProtoTestData;
import org.apache.flink.formats.protobuf.test.Models.Person;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.Message;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Tests for {@link ProtobufKryoSerializer}.
 */
public class ProtobufKryoSerializerTest {

	@Test
	public void testProtobufSerialization() {
		final ProtoTestData testData = ProtoTestUtils.getTestData();

		final ProtobufKryoSerializer kryoSerializer = new ProtobufKryoSerializer();

		Kryo kryo = new Kryo();
		byte[] outputBuffer = new byte[1_000];
		Output output = new Output(outputBuffer);
		kryoSerializer.write(kryo, output, testData.protoObj);
		Input input = new Input(output.getBuffer());

		Message actualPerson =
			kryoSerializer.read(kryo, input, (Class<Message>) Person.class.asSubclass(Message.class));

		Person expectedPerson = ((Person) testData.protoObj).toBuilder().build();
		assertEquals(expectedPerson, actualPerson);
	}
}
