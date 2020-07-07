/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PythonTypeUtils}.
 */
public class PythonTypeUtilsTest {

	@Test
	public void testLogicalTypeToFlinkTypeSerializer() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("f1", new BigIntType()));
		RowType rowType = new RowType(rowFields);
		TypeSerializer rowSerializer = PythonTypeUtils.toFlinkTypeSerializer(rowType);
		assertTrue(rowSerializer instanceof RowSerializer);

		assertEquals(1, ((RowSerializer) rowSerializer).getArity());
	}

	@Test
	public void testLogicalTypeToBlinkTypeSerializer() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("f1", new BigIntType()));
		RowType rowType = new RowType(rowFields);
		TypeSerializer baseSerializer = PythonTypeUtils.toBlinkTypeSerializer(rowType);
		assertTrue(baseSerializer instanceof RowDataSerializer);

		assertEquals(1, ((RowDataSerializer) baseSerializer).getArity());
	}

	@Test
	public void testLogicalTypeToProto() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("f1", new BigIntType()));
		RowType rowType = new RowType(rowFields);
		FlinkFnApi.Schema.FieldType protoType =
			rowType.accept(new PythonTypeUtils.LogicalTypeToProtoTypeConverter());
		FlinkFnApi.Schema schema = protoType.getRowSchema();
		assertEquals(1, schema.getFieldsCount());
		assertEquals("f1", schema.getFields(0).getName());
		assertEquals(FlinkFnApi.Schema.TypeName.BIGINT, schema.getFields(0).getType().getTypeName());
	}

	@Test
	public void testUnsupportedTypeSerializer() {
		LogicalType logicalType = new UnresolvedUserDefinedType(UnresolvedIdentifier.of("cat", "db", "MyType"));
		String expectedTestException = "Python UDF doesn't support logical type `cat`.`db`.`MyType` currently.";
		try {
			PythonTypeUtils.toFlinkTypeSerializer(logicalType);
		} catch (Exception e) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(e, expectedTestException).isPresent());
		}
	}

	@Test
	public void testLogicalTypeToConversionClassConverter() {
		PythonTypeUtils.LogicalTypeToConversionClassConverter converter =
			PythonTypeUtils.LogicalTypeToConversionClassConverter.INSTANCE;
		ArrayType arrayType = new ArrayType(new ArrayType(new DateType()));
		Class<?> conversionClass = converter.visit(arrayType);
		assertEquals(Date[][].class, conversionClass);
	}
}
