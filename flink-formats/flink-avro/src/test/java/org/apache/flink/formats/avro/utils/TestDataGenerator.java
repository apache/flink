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

package org.apache.flink.formats.avro.utils;

import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed16;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.SimpleUser;
import org.apache.flink.formats.avro.generated.User;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Generator for random test data for the generated Avro User type.
 */
public class TestDataGenerator {

	public static User generateRandomUser(Random rnd) {
		return new User(
				generateRandomString(rnd, 50),
				rnd.nextBoolean() ? null : rnd.nextInt(),
				rnd.nextBoolean() ? null : generateRandomString(rnd, 6),
				rnd.nextBoolean() ? null : rnd.nextLong(),
				rnd.nextDouble(),
				null,
				rnd.nextBoolean(),
				generateRandomStringList(rnd, 20, 30),
				generateRandomBooleanList(rnd, 20),
				rnd.nextBoolean() ? null : generateRandomStringList(rnd, 20, 20),
				generateRandomColor(rnd),
				new HashMap<>(),
				generateRandomFixed16(rnd),
				generateRandomUnion(rnd),
				generateRandomAddress(rnd),
				generateRandomBytes(rnd),
				LocalDate.parse("2014-03-01"),
				LocalTime.parse("12:12:12"),
				LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS),
				Instant.parse("2014-03-01T12:12:12.321Z"),
				Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS),
				ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()),
				new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));
	}

	public static SimpleUser generateRandomSimpleUser(Random rnd) {
		return new SimpleUser(
				generateRandomString(rnd, 50),
				rnd.nextBoolean() ? null : rnd.nextInt(),
				rnd.nextBoolean() ? null : generateRandomString(rnd, 6),
				rnd.nextBoolean() ? null : rnd.nextLong(),
				rnd.nextDouble(),
				null,
				rnd.nextBoolean(),
				generateRandomStringList(rnd, 20, 30),
				generateRandomBooleanList(rnd, 20),
				rnd.nextBoolean() ? null : generateRandomStringList(rnd, 20, 20),
				generateRandomColor(rnd),
				new HashMap<>(),
				generateRandomFixed16(rnd),
				generateRandomUnion(rnd),
				generateRandomAddress(rnd),
				generateRandomBytes(rnd));
	}

	public static Colors generateRandomColor(Random rnd) {
		return Colors.values()[rnd.nextInt(Colors.values().length)];
	}

	public static Fixed16 generateRandomFixed16(Random rnd) {
		if (rnd.nextBoolean()) {
			return new Fixed16();
		}
		else {
			byte[] bytes = new byte[16];
			rnd.nextBytes(bytes);
			return new Fixed16(bytes);
		}
	}

	public static Address generateRandomAddress(Random rnd) {
		return new Address(
				rnd.nextInt(),
				generateRandomString(rnd, 20),
				generateRandomString(rnd, 20),
				generateRandomString(rnd, 20),
				generateRandomString(rnd, 20));
	}

	public static ByteBuffer generateRandomBytes(Random rnd) {
		final byte[] bytes = new byte[10];
		rnd.nextBytes(bytes);
		return ByteBuffer.wrap(bytes);
	}

	private static List<Boolean> generateRandomBooleanList(Random rnd, int maxEntries) {
		final int num = rnd.nextInt(maxEntries + 1);
		ArrayList<Boolean> list = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			list.add(rnd.nextBoolean());
		}
		return list;
	}

	private static List<CharSequence> generateRandomStringList(Random rnd, int maxEntries, int maxLen) {
		final int num = rnd.nextInt(maxEntries + 1);
		ArrayList<CharSequence> list = new ArrayList<>();
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

	private static Object generateRandomUnion(Random rnd) {
		if (rnd.nextBoolean()) {
			if (rnd.nextBoolean()) {
				return null;
			} else {
				return rnd.nextBoolean();
			}
		} else {
			if (rnd.nextBoolean()) {
				return rnd.nextLong();
			} else {
				return rnd.nextDouble();
			}
		}
	}
}
