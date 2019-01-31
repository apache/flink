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

package org.apache.flink.formats.protobuf.utils;

import org.apache.flink.formats.protobuf.generated.UserProtobuf;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator for random test data for the generated Protobuf User type.
 */
public class TestDataGenerator {

	/**
	 * Note, Do not edit the predefined user data.
	 */
	private static final UserProtobuf.User USER_1 = UserProtobuf.User.newBuilder()
		.setName("Charlie")
		.setFavoriteColor("blue")
		.setTypeBoolTest(false)
		.setTypeDoubleTest(1.337d)
		.setTypeLongTest(1337L)
		.addAllTypeArrayString(new ArrayList<>())
		.addAllTypeArrayBoolean(new ArrayList<>())
		.setTypeUnion(UserProtobuf.OneOfValue.newBuilder().setBoolValue(true).build())
		.setTypeEnum(UserProtobuf.Colors.RED)
		.addAllTypeMap(Arrays.asList(
			UserProtobuf.MapFieldEntry.newBuilder().setKey("1").setValue(1L).build(),
			UserProtobuf.MapFieldEntry.newBuilder().setKey("2").setValue(2L).build()))
		.setTypeNested(
			UserProtobuf.Address.newBuilder()
				.setNum(42)
				.setStreet("Bakerstreet")
				.setCity("Berlin")
				.setState("Berlin")
				.setZip("12049").build())
		.setTypeBytes(ByteString.copyFromUtf8("Charlie"))
		.build();

	private static final UserProtobuf.User USER_2 = UserProtobuf.User.newBuilder()
		.setName("Whatever")
		.setFavoriteColor("black")
		.setTypeLongTest(42L)
		.setTypeDoubleTest(0.0)
		.setTypeBoolTest(true)
		.setTypeUnion(UserProtobuf.OneOfValue.newBuilder().setLongValue(1L).build())
		.addAllTypeArrayString(Collections.singletonList("hello"))
		.addAllTypeArrayBoolean(Collections.singletonList(true))
		.setTypeEnum(UserProtobuf.Colors.GREEN)
		.setTypeBytes(ByteString.copyFromUtf8("Whatever"))
		.build();

	private static final UserProtobuf.User USER_3 = UserProtobuf.User.newBuilder()
		.setName("Terminator")
		.setFavoriteColor("yellow")
		.setTypeLongTest(1L)
		.setTypeDoubleTest(0.0)
		.setTypeBoolTest(false)
		.setTypeUnion(UserProtobuf.OneOfValue.newBuilder().setDoubleValue(1.0).build())
		.addAllTypeArrayString(Collections.singletonList("world"))
		.addAllTypeArrayBoolean(Collections.singletonList(false))
		.setTypeEnum(UserProtobuf.Colors.GREEN)
		.addAllTypeMap(new ArrayList<>())
		.setTypeBytes(ByteString.copyFromUtf8("Terminator"))
		.build();

	public static UserProtobuf.User[] getPredefinedData() {
		return new UserProtobuf.User[] {
			USER_1,
			USER_2,
			USER_3
		};
	}

	public static UserProtobuf.User generateRandomUser() {
		ThreadLocalRandom rnd = ThreadLocalRandom.current();
		UserProtobuf.User.Builder builder = UserProtobuf.User.newBuilder()
			.setName(generateRandomString(rnd, 50))
			.setFavoriteNumber(rnd.nextInt(100))
			.setFavoriteColor(generateRandomString(rnd, 6))
			.setTypeLongTest(rnd.nextLong())
			.setTypeDoubleTest(rnd.nextDouble())
			.setTypeBoolTest(rnd.nextBoolean())
			.addAllTypeArrayString(generateRandomStringList(rnd, 20, 30))
			.addAllTypeArrayBoolean(generateRandomBooleanList(rnd, 20))
			.setTypeEnum(generateRandomColor(rnd))
			.addTypeMap(
				UserProtobuf.MapFieldEntry.newBuilder()
					.setKey(generateRandomString(rnd, 6))
					.setValue(rnd.nextLong()).build())
			.setTypeNested(generateRandomAddress(rnd))
			.setTypeBytes(generateRandomBytes(rnd));

		switch (rnd.nextInt(3)) {
			case 0:
				builder.setTypeUnion(UserProtobuf.OneOfValue.newBuilder().setBoolValue(rnd.nextBoolean()).build());
				break;
			case 1:
				builder.setTypeUnion(UserProtobuf.OneOfValue.newBuilder().setLongValue(rnd.nextLong()).build());
				break;
			case 2:
				builder.setTypeUnion(UserProtobuf.OneOfValue.newBuilder().setDoubleValue(rnd.nextDouble()).build());
		}

		return builder.build();
	}

	public static UserProtobuf.Colors generateRandomColor(Random rnd) {
		return UserProtobuf.Colors.values()[rnd.nextInt(UserProtobuf.Colors.values().length)];
	}

	public static UserProtobuf.Address generateRandomAddress(Random rnd) {
		return UserProtobuf.Address.newBuilder()
				.setNum(rnd.nextInt())
				.setStreet(generateRandomString(rnd, 20))
				.setCity(generateRandomString(rnd, 20))
				.setState(generateRandomString(rnd, 20))
				.setZip(generateRandomString(rnd, 20)).build();
	}

	public static ByteString generateRandomBytes(Random rnd) {
		final byte[] bytes = new byte[10];
		rnd.nextBytes(bytes);
		return ByteString.copyFrom(bytes);
	}

	private static List<Boolean> generateRandomBooleanList(Random rnd, int maxEntries) {
		final int num = rnd.nextInt(maxEntries + 1);
		ArrayList<Boolean> list = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			list.add(rnd.nextBoolean());
		}
		return list;
	}

	private static List<String> generateRandomStringList(Random rnd, int maxEntries, int maxLen) {
		final int num = rnd.nextInt(maxEntries + 1);
		ArrayList<String> list = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			list.add(generateRandomString(rnd, maxLen));
		}
		return list;
	}

	private static String generateRandomString(Random rnd, int maxLen) {
		char[] chars = new char[rnd.nextInt(maxLen + 1)];
		for (int i = 0; i < chars.length; i++) {
			chars[i] = (char) rnd.nextInt(Character.MIN_SURROGATE);
		}
		return new String(chars);
	}
}
