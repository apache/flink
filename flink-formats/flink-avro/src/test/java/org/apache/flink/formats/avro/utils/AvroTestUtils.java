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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed16;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;

/**
 * Utilities for creating Avro Schemas.
 */
public final class AvroTestUtils {

	/**
	 * Tests all Avro data types as well as nested types for a specific record.
	 */
	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getSpecificTestData() {
		final Address addr = Address.newBuilder()
			.setNum(42)
			.setStreet("Main Street 42")
			.setCity("Test City")
			.setState("Test State")
			.setZip("12345")
			.build();

		final Row rowAddr = new Row(5);
		rowAddr.setField(0, 42);
		rowAddr.setField(1, "Main Street 42");
		rowAddr.setField(2, "Test City");
		rowAddr.setField(3, "Test State");
		rowAddr.setField(4, "12345");

		final User user = User.newBuilder()
			.setName("Charlie")
			.setFavoriteNumber(null)
			.setFavoriteColor("blue")
			.setTypeLongTest(1337L)
			.setTypeDoubleTest(1.337d)
			.setTypeNullTest(null)
			.setTypeBoolTest(false)
			.setTypeArrayString(Arrays.asList("hello", "world"))
			.setTypeArrayBoolean(Arrays.asList(true, true, false))
			.setTypeNullableArray(null)
			.setTypeEnum(Colors.RED)
			.setTypeMap(Collections.singletonMap("test", 12L))
			.setTypeFixed(new Fixed16(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
			.setTypeUnion(12.0)
			.setTypeNested(addr)
			.setTypeBytes(ByteBuffer.allocate(10))
			.setTypeDate(LocalDate.parse("2014-03-01"))
			.setTypeTimeMillis(LocalTime.parse("12:12:12"))
			.setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS))
			.setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"))
			.setTypeTimestampMicros(Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS))
			// byte array must contain the two's-complement representation of the
			// unscaled integer value in big-endian byte order
			.setTypeDecimalBytes(ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			// array of length n can store at most
			// Math.floor(Math.log10(Math.pow(2, 8 * n - 1) - 1))
			// base-10 digits of precision
			.setTypeDecimalFixed(new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
			.build();

		final Row rowUser = new Row(23);
		rowUser.setField(0, "Charlie");
		rowUser.setField(1, null);
		rowUser.setField(2, "blue");
		rowUser.setField(3, 1337L);
		rowUser.setField(4, 1.337d);
		rowUser.setField(5, null);
		rowUser.setField(6, false);
		rowUser.setField(7, new String[]{"hello", "world"});
		rowUser.setField(8, new Boolean[]{true, true, false});
		rowUser.setField(9, null);
		rowUser.setField(10, "RED");
		rowUser.setField(11, Collections.singletonMap("test", 12L));
		rowUser.setField(12, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
		rowUser.setField(13, 12.0);
		rowUser.setField(14, rowAddr);
		rowUser.setField(15, new byte[10]);
		rowUser.setField(16, Date.valueOf("2014-03-01"));
		rowUser.setField(17, Time.valueOf("12:12:12"));
		rowUser.setField(18, Time.valueOf(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS)));
		rowUser.setField(19, Timestamp.valueOf("2014-03-01 12:12:12.321"));
		rowUser.setField(20, Timestamp.from(Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS)));
		rowUser.setField(21, BigDecimal.valueOf(2000, 2));
		rowUser.setField(22, BigDecimal.valueOf(2000, 2));

		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> t = new Tuple3<>();
		t.f0 = User.class;
		t.f1 = user;
		t.f2 = rowUser;

		return t;
	}

	/**
	 * Tests almost all Avro data types as well as nested types for a generic record.
	 */
	public static Tuple3<GenericRecord, Row, Schema> getGenericTestData() {
		final String schemaString =
			"{\"type\":\"record\",\"name\":\"GenericUser\",\"namespace\":\"org.apache.flink.formats.avro.generated\"," +
			"\"fields\": [{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]}," +
			"{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]},{\"name\":\"type_long_test\",\"type\":[\"long\",\"null\"]}" +
			",{\"name\":\"type_double_test\",\"type\":\"double\"},{\"name\":\"type_null_test\",\"type\":[\"null\"]}," +
			"{\"name\":\"type_bool_test\",\"type\":[\"boolean\"]},{\"name\":\"type_array_string\",\"type\":" +
			"{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"type_array_boolean\",\"type\":{\"type\":\"array\"," +
			"\"items\":\"boolean\"}},{\"name\":\"type_nullable_array\",\"type\":[\"null\",{\"type\":\"array\"," +
			"\"items\":\"string\"}],\"default\":null},{\"name\":\"type_enum\",\"type\":{\"type\":\"enum\"," +
			"\"name\":\"Colors\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}},{\"name\":\"type_map\",\"type\":{\"type\":\"map\"," +
			"\"values\":\"long\"}},{\"name\":\"type_fixed\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"Fixed16\"," +
			"\"size\":16}],\"size\":16},{\"name\":\"type_union\",\"type\":[\"null\",\"boolean\",\"long\",\"double\"]}," +
			"{\"name\":\"type_nested\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Address\",\"fields\":[{\"name\":\"num\"," +
			"\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"}," +
			"{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}]},{\"name\":\"type_bytes\"," +
			"\"type\":\"bytes\"},{\"name\":\"type_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
			"{\"name\":\"type_time_millis\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}},{\"name\":\"type_time_micros\"," +
			"\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},{\"name\":\"type_timestamp_millis\",\"type\":{\"type\":\"long\"," +
			"\"logicalType\":\"timestamp-millis\"}},{\"name\":\"type_timestamp_micros\",\"type\":{\"type\":\"long\"," +
			"\"logicalType\":\"timestamp-micros\"}},{\"name\":\"type_decimal_bytes\",\"type\":{\"type\":\"bytes\"," +
			"\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}},{\"name\":\"type_decimal_fixed\",\"type\":{\"type\":\"fixed\"," +
			"\"name\":\"Fixed2\",\"size\":2,\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}";
		final Schema schema = new Schema.Parser().parse(schemaString);
		GenericRecord addr = new GenericData.Record(schema.getField("type_nested").schema().getTypes().get(1));
		addr.put("num", 42);
		addr.put("street", "Main Street 42");
		addr.put("city", "Test City");
		addr.put("state", "Test State");
		addr.put("zip", "12345");

		final Row rowAddr = new Row(5);
		rowAddr.setField(0, 42);
		rowAddr.setField(1, "Main Street 42");
		rowAddr.setField(2, "Test City");
		rowAddr.setField(3, "Test State");
		rowAddr.setField(4, "12345");

		final GenericRecord user = new GenericData.Record(schema);
		user.put("name", "Charlie");
		user.put("favorite_number", null);
		user.put("favorite_color", "blue");
		user.put("type_long_test", 1337L);
		user.put("type_double_test", 1.337d);
		user.put("type_null_test", null);
		user.put("type_bool_test", false);
		user.put("type_array_string", Arrays.asList("hello", "world"));
		user.put("type_array_boolean", Arrays.asList(true, true, false));
		user.put("type_nullable_array", null);
		user.put("type_enum", new GenericData.EnumSymbol(schema.getField("type_enum").schema(), "RED"));
		user.put("type_map", Collections.singletonMap("test", 12L));
		user.put("type_fixed", new Fixed16(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}));
		user.put("type_union", 12.0);
		user.put("type_nested", addr);
		user.put("type_bytes", ByteBuffer.allocate(10));
		user.put("type_date", LocalDate.parse("2014-03-01"));
		user.put("type_time_millis", LocalTime.parse("12:12:12"));
		user.put("type_time_micros", LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS));
		user.put("type_timestamp_millis", Instant.parse("2014-03-01T12:12:12.321Z"));
		user.put("type_timestamp_micros", Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS));
		user.put("type_decimal_bytes",
			ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));
		user.put("type_decimal_fixed",
			new GenericData.Fixed(
				schema.getField("type_decimal_fixed").schema(),
				BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));

		final Row rowUser = new Row(23);
		rowUser.setField(0, "Charlie");
		rowUser.setField(1, null);
		rowUser.setField(2, "blue");
		rowUser.setField(3, 1337L);
		rowUser.setField(4, 1.337d);
		rowUser.setField(5, null);
		rowUser.setField(6, false);
		rowUser.setField(7, new String[]{"hello", "world"});
		rowUser.setField(8, new Boolean[]{true, true, false});
		rowUser.setField(9, null);
		rowUser.setField(10, "RED");
		rowUser.setField(11, Collections.singletonMap("test", 12L));
		rowUser.setField(12, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
		rowUser.setField(13, 12.0);
		rowUser.setField(14, rowAddr);
		rowUser.setField(15, new byte[10]);
		rowUser.setField(16, Date.valueOf("2014-03-01"));
		rowUser.setField(17, Time.valueOf("12:12:12"));
		rowUser.setField(18, Time.valueOf(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS)));
		rowUser.setField(19, Timestamp.valueOf("2014-03-01 12:12:12.321"));
		rowUser.setField(20, Timestamp.from(Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS)));
		rowUser.setField(21, BigDecimal.valueOf(2000, 2));
		rowUser.setField(22, BigDecimal.valueOf(2000, 2));

		final Tuple3<GenericRecord, Row, Schema> t = new Tuple3<>();
		t.f0 = user;
		t.f1 = rowUser;
		t.f2 = schema;

		return t;
	}

	/**
	 * Writes given record using specified schema.
	 * @param record record to serialize
	 * @param schema schema to use for serialization
	 * @return serialized record
	 */
	public static byte[] writeRecord(GenericRecord record, Schema schema) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

		new GenericDatumWriter<>(schema).write(record, encoder);
		encoder.flush();
		return stream.toByteArray();
	}

	/**
	 * Writes given specific record.
	 * @param record record to serialize
	 * @return serialized record
	 */
	public static <T extends SpecificRecord> byte[] writeRecord(T record) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

		@SuppressWarnings("unchecked")
		SpecificDatumWriter<T> writer = new SpecificDatumWriter<>((Class<T>) record.getClass());
		writer.write(record, encoder);
		encoder.flush();
		return stream.toByteArray();
	}
}
