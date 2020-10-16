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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/** Tests for TableSchemaUtils. */
public class TableSchemaUtilsTest {
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	@Test
	public void testBuilderWithGivenSchema() {
		TableSchema oriSchema = TableSchema.builder()
				.field("a", DataTypes.INT().notNull())
				.field("b", DataTypes.STRING())
				.field("c", DataTypes.INT(), "a + 1")
				.field("t", DataTypes.TIMESTAMP(3))
				.primaryKey("ct1", new String[] {"a"})
				.watermark("t", "t", DataTypes.TIMESTAMP(3))
				.build();
		TableSchema newSchema = TableSchemaUtils.builderWithGivenSchema(oriSchema).build();
		assertEquals(oriSchema, newSchema);
	}

	@Test
	public void testDropConstraint() {
		TableSchema originalSchema = TableSchema.builder()
				.field("a", DataTypes.INT().notNull())
				.field("b", DataTypes.STRING())
				.field("c", DataTypes.INT(), "a + 1")
				.field("t", DataTypes.TIMESTAMP(3))
				.primaryKey("ct1", new String[] {"a"})
				.watermark("t", "t", DataTypes.TIMESTAMP(3))
				.build();
		TableSchema newSchema = TableSchemaUtils.dropConstraint(originalSchema, "ct1");
		TableSchema expectedSchema = TableSchema.builder()
				.field("a", DataTypes.INT().notNull())
				.field("b", DataTypes.STRING())
				.field("c", DataTypes.INT(), "a + 1")
				.field("t", DataTypes.TIMESTAMP(3))
				.watermark("t", "t", DataTypes.TIMESTAMP(3))
				.build();
		assertEquals(expectedSchema, newSchema);

		// Drop non-exist constraint.
		exceptionRule.expect(ValidationException.class);
		exceptionRule.expectMessage("Constraint ct2 to drop does not exist");
		TableSchemaUtils.dropConstraint(originalSchema, "ct2");
	}

	@Test
	public void testInvalidProjectSchema() {
		TableSchema schema = TableSchema.builder()
				.field("a", DataTypes.INT().notNull())
				.field("b", DataTypes.STRING())
				.field("c", DataTypes.INT(), "a + 1")
				.field("t", DataTypes.TIMESTAMP(3))
				.primaryKey("ct1", new String[]{"a"})
				.watermark("t", "t", DataTypes.TIMESTAMP(3))
				.build();
		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("It's illegal to project on a schema contains computed columns.");
		int[][] projectedFields = {{1}};
		TableSchemaUtils.projectSchema(schema, projectedFields);
	}

	@Test
	public void testProjectSchema() {
		TableSchema schema = TableSchema.builder()
			.field("a", DataTypes.INT().notNull())
			.field("b", DataTypes.STRING())
			.field("t", DataTypes.TIMESTAMP(3))
			.primaryKey("a")
			.watermark("t", "t", DataTypes.TIMESTAMP(3))
			.build();

		int[][] projectedFields = {{2}, {0}};
		TableSchema projected = TableSchemaUtils.projectSchema(schema, projectedFields);
		TableSchema expected = TableSchema.builder()
			.field("t", DataTypes.TIMESTAMP(3))
			.field("a", DataTypes.INT().notNull())
			.build();
		assertEquals(expected, projected);
	}

	@Test
	public void testProjectSchemaWithNameConflict() {
		TableSchema schema = TableSchema.builder()
				.field("a", DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.STRING())))
				.field("a_f0", DataTypes.STRING())
				.build();
		exceptionRule.expect(TableException.class);
		exceptionRule.expectMessage(
				"Get name conflicts for origin fields `a`.`f0` and `a_f0` with new name `a_f0`. " +
						"When pushing projection into scan, we will concatenate top level names with delimiter '_'. " +
						"Please rename the origin field names when creating table."
		);
		int[][] projectedFields = {{0, 0}, {1}};
		TableSchemaUtils.projectSchema(schema, projectedFields);
	}
}
