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
		TableSchema oriSchema = TableSchema.builder()
				.field("a", DataTypes.INT().notNull())
				.field("b", DataTypes.STRING())
				.field("c", DataTypes.INT(), "a + 1")
				.field("t", DataTypes.TIMESTAMP(3))
				.primaryKey("ct1", new String[] {"a"})
				.watermark("t", "t", DataTypes.TIMESTAMP(3))
				.build();
		TableSchema newSchema = TableSchemaUtils.dropConstraint(oriSchema, "ct1");
		final String expected = "root\n" +
				" |-- a: INT NOT NULL\n" +
				" |-- b: STRING\n" +
				" |-- c: INT AS a + 1\n" +
				" |-- t: TIMESTAMP(3)\n" +
				" |-- WATERMARK FOR t AS t\n";
		assertEquals(expected, newSchema.toString());
		// Drop non-exist constraint.
		exceptionRule.expect(ValidationException.class);
		exceptionRule.expectMessage("Constraint ct2 to drop does not exist");
		TableSchemaUtils.dropConstraint(oriSchema, "ct2");
	}
}
