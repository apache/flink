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

package org.apache.flink.table.sqlexec;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test cases for {@link SqlToOperationConverter}. **/
public class SqlToOperationConverterTest {
	private final TableConfig tableConfig = new TableConfig();
	private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog",
		"default");
	private final CatalogManager catalogManager =
		new CatalogManager("builtin", catalog);
	private final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);
	private final PlanningConfigurationBuilder planningConfigurationBuilder =
		new PlanningConfigurationBuilder(tableConfig,
			functionCatalog,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
			new ExpressionBridge<>(functionCatalog,
				PlannerExpressionConverter.INSTANCE()));

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Before
	public void before() throws TableAlreadyExistException, DatabaseNotExistException {
		final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
		final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
		final TableSchema tableSchema = TableSchema.builder()
			.field("a", DataTypes.BIGINT())
			.field("b", DataTypes.VARCHAR(Integer.MAX_VALUE))
			.field("c", DataTypes.INT())
			.field("d", DataTypes.VARCHAR(Integer.MAX_VALUE))
			.build();
		Map<String, String> properties = new HashMap<>();
		properties.put("connector", "COLLECTION");
		final CatalogTable catalogTable =  new CatalogTableImpl(tableSchema, properties, "");
		catalog.createTable(path1, catalogTable, true);
		catalog.createTable(path2, catalogTable, true);
	}

	@After
	public void after() throws TableNotExistException {
		final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
		final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
		catalog.dropTable(path1, true);
		catalog.dropTable(path2, true);
	}

	@Test
	public void testCreateTable() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar" +
			")\n" +
			"  PARTITIONED BY (a, d)\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		assertEquals(Arrays.asList("a", "d"), catalogTable.getPartitionKeys());
		assertArrayEquals(catalogTable.getSchema().getFieldNames(),
			new String[] {"a", "b", "c", "d"});
		assertArrayEquals(catalogTable.getSchema().getFieldDataTypes(),
			new DataType[]{
				DataTypes.BIGINT(),
				DataTypes.VARCHAR(Integer.MAX_VALUE),
				DataTypes.INT(),
				DataTypes.VARCHAR(Integer.MAX_VALUE)});
	}

	@Test
	public void testCreateTableWithMinusInOptionKey() {
		final String sql = "create table source_table(\n" +
			"  a int,\n" +
			"  b bigint,\n" +
			"  c varchar\n" +
			") with (\n" +
			"  'a-b-c-d124' = 'ab',\n" +
			"  'a.b-c-d.e-f.g' = 'ada',\n" +
			"  'a.b-c-d.e-f1231.g' = 'ada',\n" +
			"  'a.b-c-d.*' = 'adad')\n";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		Map<String, String> properties = catalogTable.toProperties()
			.entrySet().stream()
			.filter(e -> !e.getKey().contains("schema"))
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Map<String, String> sortedProperties = new TreeMap<>(properties);
		final String expected = "{a-b-c-d124=ab, "
			+ "a.b-c-d.*=adad, "
			+ "a.b-c-d.e-f.g=ada, "
			+ "a.b-c-d.e-f1231.g=ada}";
		assertEquals(expected, sortedProperties.toString());
	}

	@Test(expected = SqlConversionException.class)
	public void testCreateTableWithPkUniqueKeys() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar, \n" +
			"  primary key(a), \n" +
			"  unique(a, b) \n" +
			")\n" +
			"  PARTITIONED BY (a, d)\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testSqlInsertWithStaticPartition() {
		final String sql = "insert into t1 partition(a=1) select b, c, d from t2";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.HIVE);
		SqlNode node = planner.parse(sql);
		assert node instanceof RichSqlInsert;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		assert operation instanceof CatalogSinkModifyOperation;
		CatalogSinkModifyOperation sinkModifyOperation = (CatalogSinkModifyOperation) operation;
		final Map<String, String> expectedStaticPartitions = new HashMap<>();
		expectedStaticPartitions.put("a", "1");
		assertEquals(expectedStaticPartitions, sinkModifyOperation.getStaticPartitions());
	}

	@Test
	public void testCreateTableWithFullDataTypes() {
		final String sql = "create table t1(\n" +
			"  f0 CHAR,\n" +
			"  f1 CHAR NOT NULL,\n" +
			"  f2 CHAR NULL,\n" +
			"  f3 CHAR(33),\n" +
			"  f4 VARCHAR,\n" +
			"  f5 VARCHAR(33),\n" +
			"  f6 STRING,\n" +
			"  f7 BOOLEAN,\n" +
			"  f13 DECIMAL,\n" +
			"  f14 DEC,\n" +
			"  f15 NUMERIC,\n" +
			"  f16 DECIMAL(10),\n" +
			"  f17 DEC(10),\n" +
			"  f18 NUMERIC(10),\n" +
			"  f19 DECIMAL(10, 3),\n" +
			"  f20 DEC(10, 3),\n" +
			"  f21 NUMERIC(10, 3),\n" +
			"  f22 TINYINT,\n" +
			"  f23 SMALLINT,\n" +
			"  f24 INTEGER,\n" +
			"  f25 INT,\n" +
			"  f26 BIGINT,\n" +
			"  f27 FLOAT,\n" +
			"  f28 DOUBLE,\n" +
			"  f29 DOUBLE PRECISION,\n" +
			"  f30 DATE,\n" +
			"  f31 TIME,\n" +
			"  f32 TIME WITHOUT TIME ZONE,\n" +
			"  f33 TIME(3),\n" +
			"  f34 TIME(3) WITHOUT TIME ZONE,\n" +
			"  f35 TIMESTAMP,\n" +
			"  f36 TIMESTAMP WITHOUT TIME ZONE,\n" +
			"  f37 TIMESTAMP(3),\n" +
			"  f38 TIMESTAMP(3) WITHOUT TIME ZONE,\n" +
			"  f42 ARRAY<INT NOT NULL>,\n" +
			"  f43 INT ARRAY,\n" +
			"  f44 INT NOT NULL ARRAY,\n" +
			"  f45 INT ARRAY NOT NULL,\n" +
			"  f46 MULTISET<INT NOT NULL>,\n" +
			"  f47 INT MULTISET,\n" +
			"  f48 INT NOT NULL MULTISET,\n" +
			"  f49 INT MULTISET NOT NULL,\n" +
			"  f50 MAP<BIGINT, BOOLEAN>,\n" +
			"  f51 ROW<f0 INT NOT NULL, f1 BOOLEAN>,\n" +
			"  f52 ROW(f0 INT NOT NULL, f1 BOOLEAN),\n" +
			"  f53 ROW<`f0` INT>,\n" +
			"  f54 ROW(`f0` INT),\n" +
			"  f55 ROW<>,\n" +
			"  f56 ROW(),\n" +
			"  f57 ROW<f0 INT NOT NULL 'This is a comment.', f1 BOOLEAN 'This as well.'>)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType1() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: TIMESTAMP_WITH_LOCAL_TIME_ZONE");
		final String sql = "create table t1(\n" +
			"  f41 ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType2() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: TIMESTAMP_WITH_LOCAL_TIME_ZONE");
		final String sql = "create table t1(\n" +
			"  f40 TIMESTAMP(3) WITH LOCAL TIME ZONE)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType3() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: TIMESTAMP_WITH_LOCAL_TIME_ZONE");
		final String sql = "create table t1(\n" +
			"  f39 TIMESTAMP WITH LOCAL TIME ZONE)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType4() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: VARBINARY");
		final String sql = "create table t1(\n" +
			"  f12 BYTES)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType5() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: VARBINARY");
		final String sql = "create table t1(\n" +
			"  f11 VARBINARY(33))";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType6() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: VARBINARY");
		final String sql = "create table t1(\n" +
			"  f10 VARBINARY)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType7() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: BINARY");
		final String sql = "create table t1(\n" +
			"  f9 BINARY(33))";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testCreateTableWithUnSupportedDataType8() {
		expectedEx.expect(TableException.class);
		expectedEx.expectMessage("Type is not supported: BINARY");
		final String sql = "create table t1(\n" +
			"  f8 BINARY)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
		tableConfig.setSqlDialect(sqlDialect);
		return planningConfigurationBuilder.createFlinkPlanner(catalogManager.getCurrentCatalog(),
			catalogManager.getCurrentDatabase());
	}
}
