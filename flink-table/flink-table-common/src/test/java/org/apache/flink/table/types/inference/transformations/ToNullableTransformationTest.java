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

package org.apache.flink.table.types.inference.transformations;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.inference.utils.TestCallContext;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ToNullableTransformation}.
 */
public class ToNullableTransformationTest {
	@Test
	public void testTransformingIfOneOperandIsNull() {
		TypeTransformation toNullable = TypeTransformations.TO_NULLABLE;
		CallContext callContext = TestCallContext.forCall(
			DataTypes.BOOLEAN().notNull(),
			DataTypes.BOOLEAN().nullable());

		DataType dataType = toNullable.transform(callContext, DataTypes.INT().notNull());

		assertThat(dataType, equalTo(DataTypes.INT().nullable()));
	}

	@Test
	public void testNotTransformingIfAllOperandsAreNotNull() {
		TypeTransformation toNullable = TypeTransformations.TO_NULLABLE;
		CallContext callContext = TestCallContext.forCall(
			DataTypes.BOOLEAN().notNull(),
			DataTypes.BOOLEAN().notNull());

		DataType dataType = toNullable.transform(callContext, DataTypes.INT().notNull());

		assertThat(dataType, equalTo(DataTypes.INT().notNull()));
	}

	@Test
	public void testReturnOriginalNullabilityIfAllOperandsAreNotNull() {
		TypeTransformation toNullable = TypeTransformations.TO_NULLABLE;
		CallContext callContext = TestCallContext.forCall(
			DataTypes.BOOLEAN().notNull(),
			DataTypes.BOOLEAN().notNull());

		DataType dataType = toNullable.transform(callContext, DataTypes.INT().nullable());

		assertThat(dataType, equalTo(DataTypes.INT().nullable()));
	}
}
