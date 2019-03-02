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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.test.Models.Person;
import org.apache.flink.formats.protobuf.test.Models.Person.Moods;
import org.apache.flink.formats.protobuf.test.Models.PhoneNumber;
import org.apache.flink.types.Row;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

/**
 * Utility methods to test protobuf-flink functionality.
 */
public class ProtoTestUtils {

	/**
	 * Container class for various representation of a proto object used for testing.
	 */
	public static class ProtoTestData {

		public Class<? extends Message> protoClass;
		public Message protoObj;
		public Row row;
		public RowTypeInfo rowTypeInfo;
	}

	public static ProtoTestData getTestData() {

		PhoneNumber phoneNumber1 =
			PhoneNumber.newBuilder().setNumber("415-123-4567").setType("mobile").build();

		PhoneNumber phoneNumber2 =
			PhoneNumber.newBuilder().setNumber("415-123-4568").setType("telephone").build();

		PhoneNumber previousPhoneNumber =
			PhoneNumber.newBuilder().setNumber("202-267-9195").setType("mobile").build();

		Timestamp ts = Timestamp.newBuilder().setSeconds(1536720365).build();

		Person person =
			Person.newBuilder()
				.setName("Test User")
				.setId(100)
				.addPhoneNumbers(phoneNumber1)
				.addPhoneNumbers(phoneNumber2)
				.setEmail("email1@example.com")
				.setMood(Moods.sad)
				.setPrimaryEmail("email2@example.com")
				.addNicknames("mike")
				.addNicknames("luke")
				.setLastUpdated(ts)
				.setMetadata("test metadata")
				.setPreviousPhoneNumber(previousPhoneNumber)
				.build();

		ProtoTestData protoTestData = new ProtoTestData();
		protoTestData.protoClass = Person.class;
		protoTestData.protoObj = person;

		final Row rowPerson = new Row(10);
		rowPerson.setField(0, "Test User");
		rowPerson.setField(1, 100);
		rowPerson.setField(2, new PhoneNumber[]{phoneNumber1, phoneNumber2});
		rowPerson.setField(3, "email1@example.com");
		rowPerson.setField(4, "happy");
		rowPerson.setField(5, "email2@example.com");
		rowPerson.setField(6, new String[]{"mike", "luke"});
		rowPerson.setField(7, ts);
		rowPerson.setField(8, "test metadata");
		rowPerson.setField(9, previousPhoneNumber);
		protoTestData.row = rowPerson;

		RowTypeInfo rowTypeInfo =
			new RowTypeInfo(
				new TypeInformation[]{
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO,
					new ListTypeInfo(PhoneNumber.class),
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					new ListTypeInfo(String.class),
					new ProtoTypeInfo(Timestamp.class),
					BasicTypeInfo.STRING_TYPE_INFO,
					new ProtoTypeInfo(Timestamp.class)
				},
				new String[]{
					"name_",
					"id_",
					"phoneNumbers_",
					"email_",
					"mood_",
					"primaryEmail_",
					"nicknames_",
					"lastUpdated_",
					"Metadata_",
					"previousPhoneNumber_"
				});
		protoTestData.rowTypeInfo = rowTypeInfo;

		return protoTestData;
	}
}
