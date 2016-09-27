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
package org.apache.flink.api.java.batch;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.FieldNotFoundException;
import org.apache.flink.api.table.Schema;
import org.apache.flink.api.table.TableException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SchemaTest {

	@Test
	public void testSchema() {
		String[] fieldNames = new String[]{"a", "b"};
		TypeInformation<?>[] typeInfos = new TypeInformation<?>[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};

		Schema s = new Schema(fieldNames, typeInfos);

		assertArrayEquals(fieldNames, s.getColumnNames());
		assertArrayEquals(typeInfos, s.getTypes());

		assertEquals("b", s.getColumnName(1));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, s.getType(1));
		assertEquals(BasicTypeInfo.INT_TYPE_INFO, s.getType("a"));

		String expectedSchemaString = "root\n" +
		                              " |-- a: Integer\n" +
		                              " |-- b: String\n";
		assertEquals(expectedSchemaString, s.toString());

		try {
			s.getColumnName(3);
			fail("out of index, should throw exception");
		} catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			s.getType(-1);
			fail("out of index, should throw exception");
		} catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			s.getType("c");
			fail("non-exist field name, should throw exception");
		} catch (Exception e) {
			assertTrue(e instanceof FieldNotFoundException);
		}
	}

	@Test(expected = TableException.class)
	public void testInvalidSchema() {
		String[] fieldNames = new String[]{"a", "b", "c"};
		TypeInformation<?>[] typeInfos = new TypeInformation<?>[]{
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};
		Schema s = new Schema(fieldNames, typeInfos);
	}


}
