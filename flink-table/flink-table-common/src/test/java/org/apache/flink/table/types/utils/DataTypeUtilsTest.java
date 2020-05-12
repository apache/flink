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

package org.apache.flink.table.types.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DataTypeUtils}.
 */
public class DataTypeUtilsTest {
	@Test
	public void testExpandRowType() {
		DataType dataType = ROW(
			FIELD("f0", INT()),
			FIELD("f1", STRING()),
			FIELD("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
			FIELD("f3", TIMESTAMP(3)));
		TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

		assertThat(
			schema,
			equalTo(
				TableSchema.builder()
					.field("f0", INT())
					.field("f1", STRING())
					.field("f2", TIMESTAMP(5).bridgedTo(Timestamp.class))
					.field("f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))
					.build()));
	}

	@Test
	public void testExpandLegacyCompositeType() {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(new TupleTypeInfo<>(
			Types.STRING,
			Types.INT,
			Types.SQL_TIMESTAMP));
		TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

		assertThat(
			schema,
			equalTo(
				TableSchema.builder()
					.field("f0", STRING())
					.field("f1", INT())
					.field("f2", TIMESTAMP(3).bridgedTo(Timestamp.class))
					.build()));
	}

	@Test
	public void testExpandStructuredType() {
		StructuredType logicalType = StructuredType.newBuilder(ObjectIdentifier.of("catalog", "database", "type"))
			.attributes(Arrays.asList(
				new StructuredType.StructuredAttribute("f0", DataTypes.INT().getLogicalType()),
				new StructuredType.StructuredAttribute("f1", DataTypes.STRING().getLogicalType()),
				new StructuredType.StructuredAttribute("f2", DataTypes.TIMESTAMP(5).getLogicalType()),
				new StructuredType.StructuredAttribute("f3", DataTypes.TIMESTAMP(3).getLogicalType())
			))
			.build();

		Map<String, DataType> dataTypes = new HashMap<>();
		dataTypes.put("f0", DataTypes.INT());
		dataTypes.put("f1", DataTypes.STRING());
		dataTypes.put("f2", DataTypes.TIMESTAMP(5).bridgedTo(Timestamp.class));
		dataTypes.put("f3", DataTypes.TIMESTAMP(3));
		FieldsDataType dataType = new FieldsDataType(logicalType, dataTypes);

		TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

		assertThat(
			schema,
			equalTo(
				TableSchema.builder()
					.field("f0", INT())
					.field("f1", STRING())
					.field("f2", TIMESTAMP(5).bridgedTo(Timestamp.class))
					.field("f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))
					.build()));
	}

	@Test
	public void testExpandDistinctType() {
		FieldsDataType dataType = (FieldsDataType) ROW(
			FIELD("f0", INT()),
			FIELD("f1", STRING()),
			FIELD("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
			FIELD("f3", TIMESTAMP(3)));

		LogicalType originalLogicalType = dataType.getLogicalType();
		DistinctType distinctLogicalType = DistinctType.newBuilder(
			ObjectIdentifier.of("catalog", "database", "type"),
			originalLogicalType)
			.build();
		DataType distinctDataType = new FieldsDataType(distinctLogicalType, dataType.getFieldDataTypes());

		TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(distinctDataType);

		assertThat(
			schema,
			equalTo(
				TableSchema.builder()
					.field("f0", INT())
					.field("f1", STRING())
					.field("f2", TIMESTAMP(5).bridgedTo(Timestamp.class))
					.field("f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))
					.build()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExpandThrowExceptionOnAtomicType() {
		DataTypeUtils.expandCompositeTypeToSchema(DataTypes.TIMESTAMP());
	}
}
