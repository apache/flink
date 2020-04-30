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
package org.apache.flink.types;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowTest {
	@Test
	public void testRowToString() {
		Row row = new Row(5);
		row.setField(0, 1);
		row.setField(1, "hello");
		row.setField(2, null);
		row.setField(3, new Tuple2<>(2, "hi"));
		row.setField(4, "hello world");

		assertEquals("1,hello,null,(2,hi),hello world", row.toString());
	}

	@Test
	public void testRowOf() {
		Row row1 = Row.of(1, "hello", null, Tuple2.of(2L, "hi"), true);
		Row row2 = new Row(5);
		row2.setField(0, 1);
		row2.setField(1, "hello");
		row2.setField(2, null);
		row2.setField(3, new Tuple2<>(2L, "hi"));
		row2.setField(4, true);
		assertEquals(row1, row2);
	}

	@Test
	public void testRowCopy() {
		Row row = new Row(5);
		row.setField(0, 1);
		row.setField(1, "hello");
		row.setField(2, null);
		row.setField(3, new Tuple2<>(2, "hi"));
		row.setField(4, "hello world");

		Row copy = Row.copy(row);
		assertEquals(row, copy);
		assertTrue(row != copy);
	}

	@Test
	public void testRowProject() {
		Row row = new Row(5);
		row.setField(0, 1);
		row.setField(1, "hello");
		row.setField(2, null);
		row.setField(3, new Tuple2<>(2, "hi"));
		row.setField(4, "hello world");

		Row projected = Row.project(row, new int[]{0, 2, 4});

		Row expected = new Row(3);
		expected.setField(0, 1);
		expected.setField(1, null);
		expected.setField(2, "hello world");
		assertEquals(expected, projected);
	}

	@Test
	public void testRowJoin() {
		Row row1 = new Row(2);
		row1.setField(0, 1);
		row1.setField(1, "hello");

		Row row2 = new Row(2);
		row2.setField(0, null);
		row2.setField(1, new Tuple2<>(2, "hi"));

		Row row3 = new Row(1);
		row3.setField(0, "hello world");

		Row joinedRow = Row.join(row1, row2, row3);

		Row expected = new Row(5);
		expected.setField(0, 1);
		expected.setField(1, "hello");
		expected.setField(2, null);
		expected.setField(3, new Tuple2<>(2, "hi"));
		expected.setField(4, "hello world");
		assertEquals(expected, joinedRow);
	}
}
