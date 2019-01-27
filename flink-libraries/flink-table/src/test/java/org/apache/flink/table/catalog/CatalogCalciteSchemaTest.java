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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.calcite.FlinkCalciteCatalogReader;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.plan.schema.CatalogCalciteTable;
import org.apache.flink.table.runtime.utils.CommonTestData;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.csv.CsvTableSource;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for CatalogCalciteSchema.
 */
public class CatalogCalciteSchemaTest {
	private final String metadataSchema = "metadata";
	private final String catalogName = "test";
	private final String db1 = "db1";
	private final String table1 = "tb1";
	private final String db2 = "db2";
	private final String table2 = "tb2";
	private final String builtindb = FlinkInMemoryCatalog.DEFAULT_DB;

	private SchemaPlus catalogSchema;
	private CalciteCatalogReader calciteCatalogReader;

	private SchemaPlus rootSchemaPlus;

	@Before
	public void setup() {
		rootSchemaPlus = CalciteSchema.createRootSchema(true, false).plus();
		final ReadableCatalog catalog = CommonTestData.getTestFlinkInMemoryCatalog(false);

		CatalogCalciteSchema.registerCatalog(rootSchemaPlus, catalogName, catalog);
		catalogSchema = rootSchemaPlus.getSubSchema("test");

		FlinkTypeFactory typeFactory = new FlinkTypeFactory(new FlinkTypeSystem());
		Properties prop = new Properties();
		prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

		CalciteConnectionConfig calciteConnConfig = new CalciteConnectionConfigImpl(prop);

		calciteCatalogReader = new FlinkCalciteCatalogReader(
			CalciteSchema.from(rootSchemaPlus),
			Collections.emptyList(),
			typeFactory,
			calciteConnConfig
		);
	}

	@Test(expected = CatalogAlreadyExistException.class)
	public void testDuplicatedRegisterCatalog() {
		SchemaPlus rootSchemaPlus = CalciteSchema.createRootSchema(true, false).plus();
		ReadableCatalog catalog = CommonTestData.getTestFlinkInMemoryCatalog();
		CatalogCalciteSchema.registerCatalog(rootSchemaPlus, catalogName, catalog);
		CatalogCalciteSchema.registerCatalog(rootSchemaPlus, catalogName, catalog);
	}

	@Test
	public void testRegisterCatalog() {
		assertTrue(rootSchemaPlus.getTableNames().isEmpty());

		assertEquals(2, rootSchemaPlus.getSubSchemaNames().size());
		assertEquals(new HashSet<>(Arrays.asList(metadataSchema, catalogName)), rootSchemaPlus.getSubSchemaNames());

		SchemaPlus schema = rootSchemaPlus.getSubSchema(catalogName);
		assertTrue(schema.getTableNames().isEmpty());
		assertEquals(3, schema.getSubSchemaNames().size());
		assertEquals(new HashSet<>(Arrays.asList(db1, db2, builtindb)), schema.getSubSchemaNames());

		SchemaPlus s1 = schema.getSubSchema(db1);
		assertTrue(s1.getSubSchemaNames().isEmpty());
		assertEquals(1, s1.getTableNames().size());
		assertEquals(new HashSet<>(Arrays.asList(table1)), s1.getTableNames());

		SchemaPlus s2 = schema.getSubSchema(db2);
		assertTrue(s2.getSubSchemaNames().isEmpty());
		assertEquals(1, s2.getTableNames().size());
		assertEquals(new HashSet<>(Arrays.asList(table2)), s2.getTableNames());
	}

	@Test
	public void testGetSubSchema() {
		List<SqlMoniker> allSchemaObjectNames = calciteCatalogReader.getAllSchemaObjectNames(Arrays.asList(catalogName));

		Set<List<String>> subSchemas = allSchemaObjectNames.stream()
			.filter(s -> s.getType().equals(SqlMonikerType.SCHEMA))
			.map(s -> s.getFullyQualifiedNames())
			.collect(Collectors.toSet());

		assertEquals(
			new HashSet<List<String>>() {{
				add(Arrays.asList(catalogName));
				add(Arrays.asList(catalogName, "db1"));
				add(Arrays.asList(catalogName, "db2"));
				add(Arrays.asList(catalogName, builtindb));
			}},
			subSchemas
		);
	}

	@Test
	public void testGetTable() {
		RelOptTable relOptTable = calciteCatalogReader.getTable(Arrays.asList(catalogName, db1, table1));

		assertNotNull(relOptTable);

		CatalogCalciteTable table = relOptTable.unwrap(CatalogCalciteTable.class);

		assertNotNull(table);

		TableSource tableSource = table.batchTableSource();

		assertTrue(tableSource instanceof CsvTableSource);
	}

	@Test
	public void testGetNotExistTable() {
		RelOptTable relOptTable = calciteCatalogReader.getTable(Arrays.asList(catalogName, db1, "nonexist-tb"));
		assertNull(relOptTable);
	}
}
