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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Function;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TableSourceUtils}.
 */
public class TableSourceUtilsTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testFieldMappingReordered() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.build(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {1, 0}));
	}

	@Test
	public void testFieldMappingNonMatchingTypes() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Type STRING of table field 'f0' does not match with type " +
			"TIMESTAMP(3) of the field of the TableSource return type.");
		TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.TIMESTAMP(3))
				.build(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			true,
			Function.identity()
		);
	}

	@Test
	public void testFieldMappingTimeAtributes() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", new AtomicDataType(new TimestampType(true, TimestampKind.ROWTIME, 3)))
				.field("f2", new AtomicDataType(new TimestampType(true, TimestampKind.PROCTIME, 3)))
				.build(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			true,
			Function.identity()
		);

		assertThat(indices,
			equalTo(new int[]{
				1, TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER, TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER}));
	}

	@Test
	public void testFieldMappingProjected() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.field("f2", DataTypes.TIMESTAMP(3))
				.build(),
			new int[] {1, 0},
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {0, 1}));
	}

	@Test
	public void testFieldMappingLegacyCompositeType() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.build(),
			TypeConversions.fromLegacyInfoToDataType(new TupleTypeInfo<>(Types.STRING, Types.LONG)),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {1, 0}));
	}

	@Test
	public void testFieldMappingLegacyCompositeTypeWithRenaming() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("a", DataTypes.BIGINT())
				.field("b", DataTypes.STRING())
				.build(),
			TypeConversions.fromLegacyInfoToDataType(new TupleTypeInfo<>(Types.STRING, Types.LONG)),
			true,
			str -> {
				switch (str) {
					case "a":
						return "f1";
					case "b":
						return "f0";
					default:
						throw new AssertionError();
				}
			}
		);

		assertThat(indices, equalTo(new int[]{1, 0}));
	}
}
