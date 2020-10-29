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

package org.apache.flink.table.formats.raw;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.RAW;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.util.StringUtils.hexStringToByte;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RawFormatDeserializationSchema} {@link RawFormatSerializationSchema}.
 */
@RunWith(Parameterized.class)
public class RawFormatSerDeSchemaTest {

	@Parameterized.Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.type(TINYINT())
				.value(Byte.MAX_VALUE)
				.binary(new byte[]{Byte.MAX_VALUE}),

			TestSpec
				.type(SMALLINT())
				.value(Short.MAX_VALUE)
				.binary(hexStringToByte("7fff")),

			TestSpec
				.type(SMALLINT())
				.value(Short.MAX_VALUE)
				.withLittleEndian()
				.binary(hexStringToByte("ff7f")),

			TestSpec
				.type(INT())
				.value(Integer.MAX_VALUE)
				.binary(hexStringToByte("7fffffff")),

			TestSpec
				.type(INT())
				.value(Integer.MAX_VALUE)
				.withLittleEndian()
				.binary(hexStringToByte("ffffff7f")),

			TestSpec
				.type(BIGINT())
				.value(Long.MAX_VALUE)
				.binary(hexStringToByte("7fffffffffffffff")),

			TestSpec
				.type(BIGINT())
				.value(Long.MAX_VALUE)
				.withLittleEndian()
				.binary(hexStringToByte("ffffffffffffff7f")),

			TestSpec
				.type(FLOAT())
				.value(Float.MAX_VALUE)
				.binary(hexStringToByte("7f7fffff")),

			TestSpec
				.type(FLOAT())
				.value(Float.MAX_VALUE)
				.withLittleEndian()
				.binary(hexStringToByte("ffff7f7f")),

			TestSpec
				.type(DOUBLE())
				.value(Double.MAX_VALUE)
				.binary(hexStringToByte("7fefffffffffffff")),

			TestSpec
				.type(DOUBLE())
				.value(Double.MAX_VALUE)
				.withLittleEndian()
				.binary(hexStringToByte("ffffffffffffef7f")),

			TestSpec
				.type(BOOLEAN())
				.value(true)
				.binary(new byte[]{1}),

			TestSpec
				.type(BOOLEAN())
				.value(false)
				.binary(new byte[]{0}),

			TestSpec
				.type(STRING())
				.value("Hello World")
				.binary("Hello World".getBytes()),

			TestSpec
				.type(STRING())
				.value("你好世界，Hello World")
				.binary("你好世界，Hello World".getBytes()),

			TestSpec
				.type(STRING())
				.value("Flink Awesome!")
				.withCharset("UTF-16")
				.binary("Flink Awesome!".getBytes(StandardCharsets.UTF_16)),

			TestSpec
				.type(STRING())
				.value("Flink 帅哭!")
				.withCharset("UTF-16")
				.binary("Flink 帅哭!".getBytes(StandardCharsets.UTF_16)),

			TestSpec
				.type(STRING())
				.value("")
				.binary("".getBytes()),

			TestSpec
				.type(VARCHAR(5))
				.value("HELLO")
				.binary("HELLO".getBytes()),

			TestSpec
				.type(BYTES())
				.value(new byte[]{1, 3, 5, 7, 9})
				.binary(new byte[]{1, 3, 5, 7, 9}),

			TestSpec
				.type(BYTES())
				.value(new byte[]{})
				.binary(new byte[]{}),

			TestSpec
				.type(BINARY(3))
				.value(new byte[]{1, 3, 5})
				.binary(new byte[]{1, 3, 5}),

			TestSpec
				.type(RAW(LocalDateTime.class, new LocalDateTimeSerializer()))
				.value(LocalDateTime.parse("2020-11-11T18:08:01.123"))
				.binary(serializeLocalDateTime(LocalDateTime.parse("2020-11-11T18:08:01.123"))),

			// test nulls
			TestSpec.type(TINYINT()).value(null).binary(null),
			TestSpec.type(SMALLINT()).value(null).binary(null),
			TestSpec.type(INT()).value(null).binary(null),
			TestSpec.type(BIGINT()).value(null).binary(null),
			TestSpec.type(FLOAT()).value(null).binary(null),
			TestSpec.type(DOUBLE()).value(null).binary(null),
			TestSpec.type(BOOLEAN()).value(null).binary(null),
			TestSpec.type(STRING()).value(null).binary(null),
			TestSpec.type(BYTES()).value(null).binary(null),
			TestSpec
				.type(RAW(LocalDateTime.class, new LocalDateTimeSerializer()))
				.value(null)
				.binary(null)
		);
	}

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Test
	public void testSerializationAndDeserialization() throws Exception {
		RawFormatDeserializationSchema deserializationSchema = new RawFormatDeserializationSchema(
			testSpec.type.getLogicalType(),
			TypeInformation.of(RowData.class),
			testSpec.charsetName,
			testSpec.isBigEndian);
		RawFormatSerializationSchema serializationSchema = new RawFormatSerializationSchema(
			testSpec.type.getLogicalType(),
			testSpec.charsetName,
			testSpec.isBigEndian);
		deserializationSchema.open(mock(DeserializationSchema.InitializationContext.class));
		serializationSchema.open(mock(SerializationSchema.InitializationContext.class));

		Row row = Row.of(testSpec.value);
		DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(
			ROW(FIELD("single", testSpec.type)));
		RowData originalRowData = (RowData) converter.toInternal(row);

		byte[] serializedBytes = serializationSchema.serialize(originalRowData);
		assertArrayEquals(testSpec.binary, serializedBytes);

		RowData deserializeRowData = deserializationSchema.deserialize(serializedBytes);
		Row actual = (Row) converter.toExternal(deserializeRowData);
		assertEquals(row, actual);
	}

	private static byte[] serializeLocalDateTime(LocalDateTime localDateTime) {
		DataOutputSerializer dos = new DataOutputSerializer(16);
		LocalDateTimeSerializer serializer = new LocalDateTimeSerializer();
		try {
			serializer.serialize(localDateTime, dos);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return dos.getCopyOfBuffer();
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		private Object value;
		private byte[] binary;
		private DataType type;
		private String charsetName = "UTF-8";
		private boolean isBigEndian = true;

		private TestSpec(DataType type) {
			this.type = type;
		}

		public static TestSpec type(DataType fieldType) {
			return new TestSpec(fieldType);
		}

		public TestSpec value(Object value) {
			this.value = value;
			return this;
		}

		public TestSpec binary(byte[] bytes) {
			this.binary = bytes;
			return this;
		}

		public TestSpec withCharset(String charsetName) {
			this.charsetName = charsetName;
			return this;
		}

		public TestSpec withLittleEndian() {
			this.isBigEndian = false;
			return this;
		}

		@Override
		public String toString() {
			String hex = binary == null ? "null" : "0x" + StringUtils.byteToHexString(binary);
			return "TestSpec{" +
				"value=" + value +
				", binary=" + hex +
				", type=" + type +
				", charsetName='" + charsetName + '\'' +
				", isBigEndian=" + isBigEndian +
				'}';
		}
	}
}
