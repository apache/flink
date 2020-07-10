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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.generated.JodaTimeRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test for the Avro serialization and deserialization schema.
 */
public class AvroRowDataDeSerializationSchemaTest {

	@Test
	public void testSerializeDeserialize() throws Exception {
		final DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("tinyint", TINYINT()),
			FIELD("smallint", SMALLINT()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("float", FLOAT()),
			FIELD("double", DOUBLE()),
			FIELD("name", STRING()),
			FIELD("bytes", BYTES()),
			FIELD("decimal", DECIMAL(19, 6)),
			FIELD("doubles", ARRAY(DOUBLE())),
			FIELD("time", TIME(0)),
			FIELD("date", DATE()),
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp3_2", TIMESTAMP(3)),
			FIELD("map", MAP(STRING(), BIGINT())),
			FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
			FIELD("map2array", MAP(STRING(), ARRAY(INT()))));
		final RowType rowType = (RowType) dataType.getLogicalType();
		final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

		final Schema schema = AvroSchemaConverter.convertToSchema(rowType);
		final GenericRecord record = new GenericData.Record(schema);
		record.put(0, true);
		record.put(1, (int) Byte.MAX_VALUE);
		record.put(2, (int) Short.MAX_VALUE);
		record.put(3, 33);
		record.put(4, 44L);
		record.put(5, 12.34F);
		record.put(6, 23.45);
		record.put(7, "hello avro");
		record.put(8, ByteBuffer.wrap(new byte[]{1, 2, 4, 5, 6, 7, 8, 12}));

		record.put(9, ByteBuffer.wrap(
				BigDecimal.valueOf(123456789, 6).unscaledValue().toByteArray()));

		List<Double> doubles = new ArrayList<>();
		doubles.add(1.2);
		doubles.add(3.4);
		doubles.add(567.8901);
		record.put(10, doubles);

		record.put(11, 18397);
		record.put(12, 10087);
		record.put(13, 1589530213123L);
		record.put(14, 1589530213122L);

		Map<String, Long> map = new HashMap<>();
		map.put("flink", 12L);
		map.put("avro", 23L);
		record.put(15, map);

		Map<String, Map<String, Integer>> map2map = new HashMap<>();
		Map<String, Integer> innerMap = new HashMap<>();
		innerMap.put("inner_key1", 123);
		innerMap.put("inner_key2", 234);
		map2map.put("outer_key", innerMap);
		record.put(16, map2map);

		List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6);
		List<Integer> list2 = Arrays.asList(11, 22, 33, 44, 55);
		Map<String, List<Integer>> map2list = new HashMap<>();
		map2list.put("list1", list1);
		map2list.put("list2", list2);
		record.put(17, map2list);

		AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
		serializationSchema.open(null);
		AvroRowDataDeserializationSchema deserializationSchema =
			new AvroRowDataDeserializationSchema(rowType, typeInfo);
		deserializationSchema.open(null);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
		datumWriter.write(record, encoder);
		encoder.flush();
		byte[] input = byteArrayOutputStream.toByteArray();

		RowData rowData = deserializationSchema.deserialize(input);
		byte[] output = serializationSchema.serialize(rowData);

		assertArrayEquals(input, output);
	}

	@Test
	public void testSpecificType() throws Exception {
		JodaTimeRecord record = new JodaTimeRecord();
		record.setTypeTimestampMillis(DateTime.parse("2010-06-30T01:20:20"));
		record.setTypeDate(LocalDate.parse("2014-03-01"));
		record.setTypeTimeMillis(LocalTime.parse("12:12:12"));
		SpecificDatumWriter<JodaTimeRecord> datumWriter = new SpecificDatumWriter<>(JodaTimeRecord.class);
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
		datumWriter.write(record, encoder);
		encoder.flush();
		byte[] input = byteArrayOutputStream.toByteArray();

		DataType dataType = ROW(
				FIELD("type_timestamp_millis", TIMESTAMP(3)),
				FIELD("type_date", DATE()),
				FIELD("type_time_millis", TIME(3)));
		final RowType rowType = (RowType) dataType.getLogicalType();
		final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);
		AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
		serializationSchema.open(null);
		AvroRowDataDeserializationSchema deserializationSchema =
				new AvroRowDataDeserializationSchema(rowType, typeInfo);
		deserializationSchema.open(null);

		RowData rowData = deserializationSchema.deserialize(input);
		byte[] output = serializationSchema.serialize(rowData);
		RowData rowData2 = deserializationSchema.deserialize(output);
		Assert.assertEquals(rowData, rowData2);
		Assert.assertEquals("2010-06-30T01:20:20", rowData.getTimestamp(0, 3).toString());
		Assert.assertEquals("2014-03-01", DataFormatConverters.LocalDateConverter.INSTANCE.toExternal(
				rowData.getInt(1)).toString());
		Assert.assertEquals("12:12:12", DataFormatConverters.LocalTimeConverter.INSTANCE.toExternal(
				rowData.getInt(2)).toString());
	}
}
