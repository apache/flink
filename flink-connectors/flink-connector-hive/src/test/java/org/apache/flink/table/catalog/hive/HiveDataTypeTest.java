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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

/**
 * Test for Hive data types.
 */
public class HiveDataTypeTest {
	private static HiveCatalog catalog;

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	@After
	public void close() {
		catalog.dropTable(new ObjectPath("default", "datatypes"), true);
	}

	@AfterClass
	public static void clean() throws IOException {
		catalog.close();
	}

	@Test
	public void testDataTypes() {
		InternalType[] types = new InternalType[] {
			DataTypes.BYTE,
			DataTypes.SHORT,
			DataTypes.INT,
			DataTypes.LONG,
			DataTypes.FLOAT,
			DataTypes.DOUBLE,
			DataTypes.BOOLEAN,
			DataTypes.STRING,
			DataTypes.BYTE_ARRAY,
			DataTypes.DATE,
			DataTypes.TIMESTAMP,
			DataTypes.createDecimalType(5, 2)
		};

		String[] cols = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			if (types[i].toString().toLowerCase().startsWith("decimal")) {
				cols[i] = "decimal_col";
			} else {
				cols[i] = types[i].toString().toLowerCase() + "_col";
			}
		}

		TableSchema schema = new TableSchema(cols, types);

		CatalogTable table = CatalogTestUtil.createCatalogTable("hive", schema,
			new HashMap<>());
		ObjectPath tablePath = new ObjectPath("default", "datatypes");
		catalog.createTable(tablePath, table, true);
		CatalogTable table1 = catalog.getTable(tablePath);

		assertTrue(table.equals(table1));
	}

	@Test
	public void testDataTypesForChar() {
		InternalType[] types = new InternalType[] {
			DataTypes.CHAR
		};

		String[] cols = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			cols[i] = types[i].toString().toLowerCase() + "_col";
		}

		TableSchema schema = new TableSchema(cols, types);

		CatalogTable table = CatalogTestUtil.createCatalogTable("hive", schema,
			new HashMap<>());
		ObjectPath tablePath = new ObjectPath("default", "datatypes");
		catalog.createTable(tablePath, table, true);
		CatalogTable table1 = catalog.getTable(tablePath);
		TableSchema tableSchema = table1.getTableSchema();
		InternalType[] colTypes = tableSchema.getFieldTypes();

		assertTrue(colTypes[0] == DataTypes.STRING);
	}
}
