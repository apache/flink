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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.thrift.generated.StructWithBinaryField;
import org.apache.flink.formats.thrift.typeutils.ThriftSerializer;

import org.apache.thrift.TBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  Thrift-format test utils.
 */
public class TestUtils {

	public static <T extends TBase> boolean isSerDeConsistentFor(T object, ThriftCodeGenerator codeGenerator)
		throws IOException {
		ThriftSerializer thriftSerializer = new ThriftSerializer(object.getClass(), codeGenerator);
		DataOutputSerializer output = new DataOutputSerializer(1024 * 1024);
		thriftSerializer.serialize(object, output);

		DataInputDeserializer input = new DataInputDeserializer();
		input.setBuffer(output.getSharedBuffer());
		T deserObject = (T) thriftSerializer.deserialize(input);
		return object.equals(deserObject);
	}

	public static <T extends TBase> boolean isSerDeConsistentFor(T object) throws IOException {
		return isSerDeConsistentFor(object, ThriftCodeGenerator.THRIFT);
	}

	public static List<StructWithBinaryField> generateStructWithBinaryFieldTestData() {
		List<StructWithBinaryField> result = new ArrayList<>();

		for (int i = 0; i < 20; i++) {
			StructWithBinaryField message = new StructWithBinaryField();
			message.setId(1000L + i);
			message.setSegmentIds("123456789".getBytes());
			message.setValue("4567890");
			message.setNums(200L + i);
			result.add(message);
		}
		return result;
	}
}
