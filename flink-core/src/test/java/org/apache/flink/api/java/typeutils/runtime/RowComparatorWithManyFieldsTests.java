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
package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowComparator;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link RowComparator} for wide rows.
 */
public class RowComparatorWithManyFieldsTests extends ComparatorTestBase<Row> {

	private static final int numberOfFields = 10;
	private static RowTypeInfo typeInfo;
	private static final Row[] data = new Row[]{
		createRow(null, "b0", "c0", "d0", "e0", "f0", "g0", "h0", "i0", "j0"),
		createRow("a1", "b1", "c1", "d1", "e1", "f1", "g1", "h1", "i1", "j1"),
		createRow("a2", "b2", "c2", "d2", "e2", "f2", "g2", "h2", "i2", "j2"),
		createRow("a3", "b3", "c3", "d3", "e3", "f3", "g3", "h3", "i3", "j3")
	};

	@BeforeClass
	public static void setUp() throws Exception {
		TypeInformation<?>[] fieldTypes = new TypeInformation[numberOfFields];
		for (int i = 0; i < numberOfFields; i++) {
			fieldTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
		}
		typeInfo = new RowTypeInfo(fieldTypes);

	}

	@Override
	protected void deepEquals(String message, Row should, Row is) {
		int arity = should.getArity();
		assertEquals(message, arity, is.getArity());
		for (int i = 0; i < arity; i++) {
			Object copiedValue = should.getField(i);
			Object element = is.getField(i);
			assertEquals(message, element, copiedValue);
		}
	}

	@Override
	protected TypeComparator<Row> createComparator(boolean ascending) {
		return typeInfo.createComparator(
			new int[]{0},
			new boolean[]{ascending},
			0,
			new ExecutionConfig());
	}

	@Override
	protected TypeSerializer<Row> createSerializer() {
		return typeInfo.createSerializer(new ExecutionConfig());
	}

	@Override
	protected Row[] getSortedTestData() {
		return data;
	}

	@Override
	protected boolean supportsNullKeys() {
		return true;
	}

	private static Row createRow(Object... values) {
		checkNotNull(values);
		checkArgument(values.length == numberOfFields);
		Row row = new Row(numberOfFields);
		for (int i = 0; i < values.length; i++) {
			row.setField(i, values[i]);
		}
		return row;
	}
}
