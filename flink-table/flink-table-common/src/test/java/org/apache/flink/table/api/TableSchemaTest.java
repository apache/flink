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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TableSchema}.
 */
public class TableSchemaTest {

	@Test
	public void testToString() {
		TableSchema.Builder builder = TableSchema.builder()
			.field("a", DataTypes.INT())
			.field("b", DataTypes.STRING())
			.field("c", DataTypes.BIGINT());

		String expected = "root\n |-- a: INT\n |-- b: STRING\n |-- c: BIGINT\n";
		assertEquals(expected, builder.build().toString());


		builder.primaryKey("a");
		String expected2 = expected + " |-- PRIMARY KEY (a)\n";
		assertEquals(expected2, builder.build().toString());

		builder
			.uniqueKey("a", "b")
			.uniqueKey("b", "c");
		String expected3 = expected2 + " |-- UNIQUE KEY (a, b)\n |-- UNIQUE KEY (b, c)\n";
		assertEquals(expected3, builder.build().toString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidPrimaryKeyFieldName() {
		TableSchema.builder()
			.field("a", DataTypes.INT())
			.field("b", DataTypes.STRING())
			.field("c", DataTypes.BIGINT())
			.primaryKey("a", "d");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidMultiPrimaryKey() {
		TableSchema.builder()
			.field("a", DataTypes.INT())
			.field("b", DataTypes.STRING())
			.field("c", DataTypes.BIGINT())
			.primaryKey("a")
			.primaryKey("b");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidUniqueKeyFieldName() {
		TableSchema.builder()
			.field("a", DataTypes.INT())
			.field("b", DataTypes.STRING())
			.field("c", DataTypes.BIGINT())
			.uniqueKey("a", "d");
	}
}
