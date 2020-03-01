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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeChecks}.
 */
public class LogicalTypeChecksTest {

	@Test
	public void testHasNestedRoot() {
		final DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
		assertThat(
			LogicalTypeChecks.hasNestedRoot(dataType.getLogicalType(), LogicalTypeRoot.VARCHAR),
			is(true));

		assertThat(
			LogicalTypeChecks.hasNestedRoot(dataType.getLogicalType(), LogicalTypeRoot.ROW),
			is(true));

		assertThat(
			LogicalTypeChecks.hasNestedRoot(dataType.getLogicalType(), LogicalTypeRoot.BOOLEAN),
			is(false));
	}

	@Test
	public void testIsCompositeTypeRowType() {
		DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
		boolean isCompositeType = LogicalTypeChecks.isCompositeType(dataType.getLogicalType());

		assertThat(isCompositeType, is(true));
	}

	@Test
	public void testIsCompositeTypeDistinctType() {
		DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
		DistinctType distinctType = DistinctType.newBuilder(
			ObjectIdentifier.of("catalog", "database", "type"),
			dataType.getLogicalType()).build();
		boolean isCompositeType = LogicalTypeChecks.isCompositeType(distinctType);

		assertThat(isCompositeType, is(true));
	}

	@Test
	public void testIsCompositeTypeLegacyCompositeType() {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(Types.STRING, Types.INT));
		boolean isCompositeType = LogicalTypeChecks.isCompositeType(dataType.getLogicalType());

		assertThat(isCompositeType, is(true));
	}

	@Test
	public void testIsCompositeTypeStructuredType() {
		StructuredType logicalType = StructuredType.newBuilder(ObjectIdentifier.of("catalog", "database", "type"))
			.attributes(Arrays.asList(
				new StructuredType.StructuredAttribute("f0", DataTypes.INT().getLogicalType()),
				new StructuredType.StructuredAttribute("f1", DataTypes.STRING().getLogicalType())
			))
			.build();

		Map<String, DataType> dataTypes = new HashMap<>();
		dataTypes.put("f0", DataTypes.INT());
		dataTypes.put("f1", DataTypes.STRING());
		FieldsDataType dataType = new FieldsDataType(logicalType, dataTypes);
		boolean isCompositeType = LogicalTypeChecks.isCompositeType(dataType.getLogicalType());

		assertThat(isCompositeType, is(true));
	}

	@Test
	public void testIsCompositeTypeLegacySimpleType() {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(Types.STRING);
		boolean isCompositeType = LogicalTypeChecks.isCompositeType(dataType.getLogicalType());

		assertThat(isCompositeType, is(false));
	}

	@Test
	public void testIsCompositeTypeSimpleType() {
		DataType dataType = DataTypes.TIMESTAMP();
		boolean isCompositeType = LogicalTypeChecks.isCompositeType(dataType.getLogicalType());

		assertThat(isCompositeType, is(false));
	}
}
