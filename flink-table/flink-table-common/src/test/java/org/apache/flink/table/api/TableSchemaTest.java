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

package org.apache.flink.table.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TableSchema}.
 */
public class TableSchemaTest {

	private static final String WATERMARK_EXPRESSION = "now()";
	private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testTableSchema() {
		TableSchema schema = TableSchema.builder()
			.field("f0", DataTypes.BIGINT())
			.field("f1", DataTypes.ROW(
				DataTypes.FIELD("q1", DataTypes.STRING()),
				DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3))))
			.field("f2", DataTypes.STRING())
			.field("f3", DataTypes.BIGINT(), "f0 + 1")
			.watermark("f1.q2", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.build();

		// test toString()
		String expected = "root\n" +
			" |-- f0: BIGINT\n" +
			" |-- f1: ROW<`q1` STRING, `q2` TIMESTAMP(3)>\n" +
			" |-- f2: STRING\n" +
			" |-- f3: BIGINT AS f0 + 1\n" +
			" |-- WATERMARK FOR f1.q2 AS now()";
		assertEquals(expected, schema.toString());

		// test getFieldNames and getFieldDataType
		assertEquals(Optional.of("f2"), schema.getFieldName(2));
		assertEquals(Optional.of(DataTypes.BIGINT()), schema.getFieldDataType(3));
		assertEquals(Optional.of(TableColumn.of("f3", DataTypes.BIGINT(), "f0 + 1")),
			schema.getTableColumn(3));
		assertEquals(Optional.of(DataTypes.STRING()), schema.getFieldDataType("f2"));
		assertEquals(Optional.of(DataTypes.STRING()), schema.getFieldDataType("f1")
			.map(r -> ((FieldsDataType) r).getFieldDataTypes().get("q1")));
		assertFalse(schema.getFieldName(4).isPresent());
		assertFalse(schema.getFieldType(-1).isPresent());
		assertFalse(schema.getFieldType("c").isPresent());
		assertFalse(schema.getFieldDataType("f1.q1").isPresent());
		assertFalse(schema.getFieldDataType("f1.q3").isPresent());

		// test copy() and equals()
		assertEquals(schema, schema.copy());
		assertEquals(schema.hashCode(), schema.copy().hashCode());
	}

	@Test
	public void testWatermarkOnDifferentFields() {
		// column_name, column_type, exception_msg
		List<Tuple3<String, DataType, String>> testData = new ArrayList<>();
		testData.add(Tuple3.of("a", DataTypes.BIGINT(), "but is of type 'BIGINT'"));
		testData.add(Tuple3.of("b", DataTypes.STRING(), "but is of type 'STRING'"));
		testData.add(Tuple3.of("c", DataTypes.INT(), "but is of type 'INT'"));
		testData.add(Tuple3.of("d", DataTypes.TIMESTAMP(), "PASS"));
		testData.add(Tuple3.of("e", DataTypes.TIMESTAMP(0), "PASS"));
		testData.add(Tuple3.of("f", DataTypes.TIMESTAMP(3), "PASS"));
		testData.add(Tuple3.of("g", DataTypes.TIMESTAMP(9), "PASS"));
		testData.add(Tuple3.of("h", DataTypes.TIMESTAMP_WITH_TIME_ZONE(3), "but is of type 'TIMESTAMP(3) WITH TIME ZONE'"));

		testData.forEach(t -> {
			TableSchema.Builder builder = TableSchema.builder();
			testData.forEach(e -> builder.field(e.f0, e.f1));
			builder.watermark(t.f0, WATERMARK_EXPRESSION, WATERMARK_DATATYPE);
			if (t.f2.equals("PASS")) {
				TableSchema schema = builder.build();
				assertEquals(1, schema.getWatermarkSpecs().size());
				assertEquals(t.f0, schema.getWatermarkSpecs().get(0).getRowtimeAttribute());
			} else {
				try {
					builder.build();
				} catch (Exception e) {
					assertTrue(e.getMessage().contains(t.f2));
				}
			}
		});
	}

	@Test
	public void testWatermarkOnNestedField() {
		TableSchema schema = TableSchema.builder()
			.field("f0", DataTypes.BIGINT())
			.field("f1", DataTypes.ROW(
				DataTypes.FIELD("q1", DataTypes.STRING()),
				DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3)),
				DataTypes.FIELD("q3", DataTypes.ROW(
					DataTypes.FIELD("t1", DataTypes.TIMESTAMP(3)),
					DataTypes.FIELD("t2", DataTypes.STRING())
				)))
			)
			.watermark("f1.q3.t1", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.build();

		assertEquals(1, schema.getWatermarkSpecs().size());
		assertEquals("f1.q3.t1", schema.getWatermarkSpecs().get(0).getRowtimeAttribute());
	}

	@Test
	public void testWatermarkOnNonExistedField() {
		thrown.expectMessage("Rowtime attribute 'f1.q0' is not defined in schema");

		TableSchema.builder()
			.field("f0", DataTypes.BIGINT())
			.field("f1", DataTypes.ROW(
				DataTypes.FIELD("q1", DataTypes.STRING()),
				DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3))))
			.watermark("f1.q0", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.build();
	}

	@Test
	public void testMultipleWatermarks() {
		thrown.expectMessage("Multiple watermark definition is not supported yet.");

		TableSchema.builder()
			.field("f0", DataTypes.TIMESTAMP())
			.field("f1", DataTypes.ROW(
				DataTypes.FIELD("q1", DataTypes.STRING()),
				DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3))))
			.watermark("f1.q2", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.watermark("f0", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.build();
	}

	@Test
	public void testDifferentWatermarkStrategyOutputTypes() {
		List<Tuple2<DataType, String>> testData = new ArrayList<>();
		testData.add(Tuple2.of(DataTypes.BIGINT(), "but is of type 'BIGINT'"));
		testData.add(Tuple2.of(DataTypes.STRING(), "but is of type 'VARCHAR(2147483647)'"));
		testData.add(Tuple2.of(DataTypes.INT(), "but is of type 'INT'"));
		testData.add(Tuple2.of(DataTypes.TIMESTAMP(), "PASS"));
		testData.add(Tuple2.of(DataTypes.TIMESTAMP(0), "PASS"));
		testData.add(Tuple2.of(DataTypes.TIMESTAMP(3), "PASS"));
		testData.add(Tuple2.of(DataTypes.TIMESTAMP(9), "PASS"));
		testData.add(Tuple2.of(DataTypes.TIMESTAMP_WITH_TIME_ZONE(3), "but is of type 'TIMESTAMP(3) WITH TIME ZONE'"));

		testData.forEach(t -> {
			TableSchema.Builder builder = TableSchema.builder()
				.field("f0", DataTypes.TIMESTAMP())
				.watermark("f0", "f0 - INTERVAL '5' SECOND", t.f0);
			if (t.f1.equals("PASS")) {
				TableSchema schema = builder.build();
				assertEquals(1, schema.getWatermarkSpecs().size());
			} else {
				try {
					builder.build();
				} catch (Exception e) {
					assertTrue(e.getMessage().contains(t.f1));
				}
			}
		});
	}

}
