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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.formats.protobuf.test.Models.Person;
import org.apache.flink.formats.protobuf.test.Models.PhoneNumber;

import com.google.protobuf.Timestamp;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ProtoTypeInfo}.
 */
public class ProtoTypeInfoTest {

	@Test
	public void testSimpleObject() {
		ProtoTypeInfo protoTypeInfo = new ProtoTypeInfo(PhoneNumber.class);
		assertEquals(2, protoTypeInfo.getArity());
		assertEquals(2, protoTypeInfo.getTotalFields());

		assertArrayEquals(new String[]{"number_", "type_"}, protoTypeInfo.getFieldNames());
		assertEquals(0, protoTypeInfo.getFieldIndex("number_"));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, protoTypeInfo.getTypeAt(0));
		assertEquals(1, protoTypeInfo.getFieldIndex("type_"));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, protoTypeInfo.getTypeAt(1));
	}

	@Test
	public void testComplexObject() {
		ProtoTypeInfo protoTypeInfo = new ProtoTypeInfo(Person.class);
		assertEquals(8, protoTypeInfo.getArity());
		assertEquals(10, protoTypeInfo.getTotalFields());

		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, protoTypeInfo.getTypeAt("name_"));
		assertEquals(BasicTypeInfo.INT_TYPE_INFO, protoTypeInfo.getTypeAt("id_"));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, protoTypeInfo.getTypeAt("email_"));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, protoTypeInfo.getTypeAt("Metadata_"));
		assertEquals(new ListTypeInfo(PhoneNumber.class), protoTypeInfo.getTypeAt("phoneNumbers_"));
		assertEquals(new ListTypeInfo(String.class), protoTypeInfo.getTypeAt("nicknames_"));
		assertEquals(Timestamp.class, protoTypeInfo.getTypeAt("lastUpdated_").getTypeClass());
		assertEquals(PhoneNumber.class,
			protoTypeInfo.getTypeAt("previousPhoneNumber_").getTypeClass());
		// TODO: Handle Enum types
		// assertEquals(new EnumTypeInfo(Person.Moods.class), protoTypeInfo.getTypeAt("mood_"));
		// TODO: Handle oneof type - but what should the TypeInfo look like in this case?

		String[] fieldNames = protoTypeInfo.getFieldNames();
		Arrays.sort(fieldNames);
		assertArrayEquals(
			new String[]{
				"Metadata_", "email_", "id_", "lastUpdated_",
				"name_", "nicknames_", "phoneNumbers_", "previousPhoneNumber_"
			},
			fieldNames);
	}
}
