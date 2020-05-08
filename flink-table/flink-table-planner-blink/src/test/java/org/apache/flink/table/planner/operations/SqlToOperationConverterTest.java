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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterTableAddConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTableDropConstraintOperation;
import org.apache.flink.table.operations.ddl.AlterTablePropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.expressions.utils.Func0$;
import org.apache.flink.table.planner.expressions.utils.Func1$;
import org.apache.flink.table.planner.expressions.utils.Func8$;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Test cases for {@link SqlToOperationConverter}.
 */
public class SqlToOperationConverterTest {
	private final TableConfig tableConfig = new TableConfig();
	private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog",
		"default");
	private final CatalogManager catalogManager = CatalogManagerMocks.preparedCatalogManager()
		.defaultCatalog("builtin", catalog)
		.build();
	private final ModuleManager moduleManager = new ModuleManager();
	private final FunctionCatalog functionCatalog = new FunctionCatalog(
		tableConfig,
		catalogManager,
		moduleManager);
	private final PlannerContext plannerContext =
		new PlannerContext(tableConfig,
			functionCatalog,
			catalogManager,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
			new ArrayList<>());

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

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
	public void testUseCatalog() {
		final String sql = "USE CATALOG cat1";
		Operation operation = parse(sql, SqlDialect.DEFAULT);
		assert operation instanceof UseCatalogOperation;
		assertEquals("cat1", ((UseCatalogOperation) operation).getCatalogName());
	}

	@Test
	public void testUseDatabase() {
		final String sql1 = "USE db1";
		Operation operation1 = parse(sql1, SqlDialect.DEFAULT);
		assert operation1 instanceof UseDatabaseOperation;
		assertEquals("builtin", ((UseDatabaseOperation) operation1).getCatalogName());
		assertEquals("db1", ((UseDatabaseOperation) operation1).getDatabaseName());

		final String sql2 = "USE cat1.db1";
		Operation operation2 = parse(sql2, SqlDialect.DEFAULT);
		assert operation2 instanceof UseDatabaseOperation;
		assertEquals("cat1", ((UseDatabaseOperation) operation2).getCatalogName());
		assertEquals("db1", ((UseDatabaseOperation) operation2).getDatabaseName());
	}

	@Test(expected = SqlConversionException.class)
	public void testUseDatabaseWithException() {
		final String sql = "USE cat1.db1.tbl1";
		Operation operation = parse(sql, SqlDialect.DEFAULT);
	}

	@Test
	public void testCreateDatabase() {
		final String[] createDatabaseSqls = new String[] {
				"create database db1",
				"create database if not exists cat1.db1",
				"create database cat1.db1 comment 'db1_comment'",
				"create database cat1.db1 comment 'db1_comment' with ('k1' = 'v1', 'K2' = 'V2')"
		};
		final String[] expectedCatalogs = new String[] {"builtin", "cat1", "cat1", "cat1"};
		final String expectedDatabase = "db1";
		final String[] expectedComments = new String[] {null, null, "db1_comment", "db1_comment"};
		final boolean[] expectedIgnoreIfExists = new boolean[] {false, true, false, false};
		Map<String, String> properties = new HashMap<>();
		properties.put("k1", "v1");
		properties.put("K2", "V2");
		final Map[] expectedProperties = new Map[] {
				new HashMap<String, String>(),
				new HashMap<String, String>(),
				new HashMap<String, String>(),
				new HashMap(properties)};

		for (int i = 0; i < createDatabaseSqls.length; i++) {
			Operation operation = parse(createDatabaseSqls[i], SqlDialect.DEFAULT);
			assert operation instanceof CreateDatabaseOperation;
			final CreateDatabaseOperation createDatabaseOperation = (CreateDatabaseOperation) operation;
			assertEquals(expectedCatalogs[i], createDatabaseOperation.getCatalogName());
			assertEquals(expectedDatabase, createDatabaseOperation.getDatabaseName());
			assertEquals(expectedComments[i], createDatabaseOperation.getCatalogDatabase().getComment());
			assertEquals(expectedIgnoreIfExists[i], createDatabaseOperation.isIgnoreIfExists());
			assertEquals(expectedProperties[i], createDatabaseOperation.getCatalogDatabase().getProperties());
		}
	}

	@Test
	public void testDropDatabase() {
		final String[] dropDatabaseSqls = new String[] {
				"drop database db1",
				"drop database if exists db1",
				"drop database if exists cat1.db1 CASCADE",
				"drop database if exists cat1.db1 RESTRICT"
		};
		final String[] expectedCatalogs = new String[] {"builtin", "builtin", "cat1", "cat1"};
		final String expectedDatabase = "db1";
		final boolean[] expectedIfExists = new boolean[] {false, true, true, true};
		final boolean[] expectedIsCascades = new boolean[] {false, false, true, false};

		for (int i = 0; i < dropDatabaseSqls.length; i++) {
			Operation operation = parse(dropDatabaseSqls[i], SqlDialect.DEFAULT);
			assert operation instanceof DropDatabaseOperation;
			final DropDatabaseOperation dropDatabaseOperation = (DropDatabaseOperation) operation;
			assertEquals(expectedCatalogs[i], dropDatabaseOperation.getCatalogName());
			assertEquals(expectedDatabase, dropDatabaseOperation.getDatabaseName());
			assertEquals(expectedIfExists[i], dropDatabaseOperation.isIfExists());
			assertEquals(expectedIsCascades[i], dropDatabaseOperation.isCascade());
		}
	}

	@Test
	public void testAlterDatabase() throws Exception {
		catalogManager.registerCatalog("cat1",
									new GenericInMemoryCatalog("default", "default"));
		catalogManager.getCatalog("cat1").get()
						.createDatabase("db1",
										new CatalogDatabaseImpl(new HashMap<>(), "db1_comment"),
										true);
		final String sql = "alter database cat1.db1 set ('k1'='v1', 'K2'='V2')";
		Operation operation = parse(sql, SqlDialect.DEFAULT);
		assert operation instanceof AlterDatabaseOperation;
		Map<String, String> properties = new HashMap<>();
		properties.put("k1", "v1");
		properties.put("K2", "V2");
		assertEquals("db1", ((AlterDatabaseOperation) operation).getDatabaseName());
		assertEquals("cat1", ((AlterDatabaseOperation) operation).getCatalogName());
		assertEquals("db1_comment", ((AlterDatabaseOperation) operation).getCatalogDatabase().getComment());
		assertEquals(properties, ((AlterDatabaseOperation) operation).getCatalogDatabase().getProperties());
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
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		Operation operation = parse(sql, planner, parser);
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
	public void testCreateTableWithPrimaryKey() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar, \n" +
			"  constraint ct1 primary key(a, b) not enforced\n" +
			") with (\n" +
			"  'connector' = 'kafka', \n" +
			"  'kafka.topic' = 'log.test'\n" +
			")\n";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		Operation operation = parse(sql, planner, parser);
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		TableSchema tableSchema = catalogTable.getSchema();
		assertThat(
				tableSchema
						.getPrimaryKey()
						.map(UniqueConstraint::asSummaryString)
						.orElse("fakeVal"),
				is("CONSTRAINT ct1 PRIMARY KEY (a, b)"));
		assertArrayEquals(
				new String[] {"a", "b", "c", "d"},
				tableSchema.getFieldNames());
		assertArrayEquals(
				new DataType[]{
						DataTypes.BIGINT().notNull(),
						DataTypes.STRING().notNull(),
						DataTypes.INT(),
						DataTypes.STRING()},
				tableSchema.getFieldDataTypes());
	}

	@Test
	public void testCreateTableWithPrimaryKeyEnforced() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c int, \n" +
				"  d varchar, \n" +
				// Default is enforced.
				"  constraint ct1 primary key(a, b)\n" +
				") with (\n" +
				"  'connector' = 'kafka', \n" +
				"  'kafka.topic' = 'log.test'\n" +
				")\n";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		exceptionRule.expect(ValidationException.class);
		exceptionRule.expectMessage("Flink doesn't support ENFORCED mode for PRIMARY KEY "
				+ "constaint. ENFORCED/NOT ENFORCED  controls if the constraint "
				+ "checks are performed on the incoming/outgoing data. "
				+ "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode");
		parse(sql, planner, parser);
	}

	@Test
	public void testCreateTableWithUniqueKey() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c int, \n" +
				"  d varchar, \n" +
				"  constraint ct1 unique (a, b) not enforced\n" +
				") with (\n" +
				"  'connector' = 'kafka', \n" +
				"  'kafka.topic' = 'log.test'\n" +
				")\n";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		exceptionRule.expect(UnsupportedOperationException.class);
		exceptionRule.expectMessage("UNIQUE constraint is not supported yet");
		parse(sql, planner, parser);
	}

	@Test
	public void testCreateTableWithMinusInOptionKey() {
		final String sql = "create table source_table(\n" +
			"  a int,\n" +
			"  b bigint,\n" +
			"  c varchar\n" +
			") with (\n" +
			"  'a-B-c-d124' = 'Ab',\n" +
			"  'a.b-c-d.e-f.g' = 'ada',\n" +
			"  'a.b-c-d.e-f1231.g' = 'ada',\n" +
			"  'a.b-c-d.*' = 'adad')\n";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = parser.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		Map<String, String> properties = catalogTable.getProperties()
			.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Map<String, String> sortedProperties = new TreeMap<>(properties);
		final String expected = "{a-B-c-d124=Ab, "
			+ "a.b-c-d.*=adad, "
			+ "a.b-c-d.e-f.g=ada, "
			+ "a.b-c-d.e-f1231.g=ada}";
		assertEquals(expected, sortedProperties.toString());
	}

	@Test
	public void testCreateTableWithWatermark() throws FunctionAlreadyExistException, DatabaseNotExistException {
		CatalogFunction cf = new CatalogFunctionImpl(
			JavaUserDefinedScalarFunctions.JavaFunc5.class.getName());
		catalog.createFunction(ObjectPath.fromString("default.myfunc"), cf, true);

		final String sql = "create table source_table(\n" +
			"  a int,\n" +
			"  b bigint,\n" +
			"  c timestamp(3),\n" +
			"  watermark for `c` as myfunc(c, 1) - interval '5' second\n" +
			") with (\n" +
			"  'connector.type' = 'kafka')\n";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = parser.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		Map<String, String> properties = catalogTable.toProperties();
		Map<String, String> expected = new HashMap<>();
		expected.put("schema.0.name", "a");
		expected.put("schema.0.data-type", "INT");
		expected.put("schema.1.name", "b");
		expected.put("schema.1.data-type", "BIGINT");
		expected.put("schema.2.name", "c");
		expected.put("schema.2.data-type", "TIMESTAMP(3)");
		expected.put("schema.watermark.0.rowtime", "c");
		expected.put(
			"schema.watermark.0.strategy.expr",
			"`builtin`.`default`.`myfunc`(`c`, 1) - INTERVAL '5' SECOND");
		expected.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");
		expected.put("connector.type", "kafka");
		assertEquals(expected, properties);
	}

	@Test
	public void testSqlInsertWithStaticPartition() {
		final String sql = "insert into t1 partition(a=1) select b, c, d from t2";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		Operation operation = parse(sql, planner, parser);
		assert operation instanceof CatalogSinkModifyOperation;
		CatalogSinkModifyOperation sinkModifyOperation = (CatalogSinkModifyOperation) operation;
		final Map<String, String> expectedStaticPartitions = new HashMap<>();
		expectedStaticPartitions.put("a", "1");
		assertEquals(expectedStaticPartitions, sinkModifyOperation.getStaticPartitions());
	}

	@Test
	public void testSqlInsertWithDynamicTableOptions() {
		final String sql = "insert into t1 /*+ OPTIONS('k1'='v1', 'k2'='v2') */\n"
				+ "select a, b, c, d from t2";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		Operation operation = parse(sql, planner, parser);
		assert operation instanceof CatalogSinkModifyOperation;
		CatalogSinkModifyOperation sinkModifyOperation = (CatalogSinkModifyOperation) operation;
		Map<String, String> dynamicOptions = sinkModifyOperation.getDynamicOptions();
		assertNotNull(dynamicOptions);
		assertThat(dynamicOptions.size(), is(2));
		assertThat(dynamicOptions.toString(), is("{k1=v1, k2=v2}"));
	}

	@Test
	public void testDynamicTableWithInvalidOptions() {
		final String sql = "select * from t1 /*+ OPTIONS('opt1', 'opt2') */";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		exceptionRule.expect(AssertionError.class);
		exceptionRule.expectMessage("Hint [OPTIONS] only support "
				+ "non empty key value options");
		parse(sql, planner, parser);
	}

	@Test // TODO: tweak the tests when FLINK-13604 is fixed.
	public void testCreateTableWithFullDataTypes() {
		final List<TestItem> testItems = Arrays.asList(
			createTestItem("CHAR", DataTypes.CHAR(1)),
			createTestItem("CHAR NOT NULL", DataTypes.CHAR(1).notNull()),
			createTestItem("CHAR NULL", DataTypes.CHAR(1)),
			createTestItem("CHAR(33)", DataTypes.CHAR(33)),
			createTestItem("VARCHAR", DataTypes.STRING()),
			createTestItem("VARCHAR(33)", DataTypes.VARCHAR(33)),
			createTestItem("STRING", DataTypes.STRING()),
			createTestItem("BOOLEAN", DataTypes.BOOLEAN()),
			createTestItem("BINARY", DataTypes.BINARY(1)),
			createTestItem("BINARY(33)", DataTypes.BINARY(33)),
			createTestItem("VARBINARY", DataTypes.BYTES()),
			createTestItem("VARBINARY(33)", DataTypes.VARBINARY(33)),
			createTestItem("BYTES", DataTypes.BYTES()),
			createTestItem("DECIMAL", DataTypes.DECIMAL(10, 0)),
			createTestItem("DEC", DataTypes.DECIMAL(10, 0)),
			createTestItem("NUMERIC", DataTypes.DECIMAL(10, 0)),
			createTestItem("DECIMAL(10)", DataTypes.DECIMAL(10, 0)),
			createTestItem("DEC(10)", DataTypes.DECIMAL(10, 0)),
			createTestItem("NUMERIC(10)", DataTypes.DECIMAL(10, 0)),
			createTestItem("DECIMAL(10, 3)", DataTypes.DECIMAL(10, 3)),
			createTestItem("DEC(10, 3)", DataTypes.DECIMAL(10, 3)),
			createTestItem("NUMERIC(10, 3)", DataTypes.DECIMAL(10, 3)),
			createTestItem("TINYINT", DataTypes.TINYINT()),
			createTestItem("SMALLINT", DataTypes.SMALLINT()),
			createTestItem("INTEGER", DataTypes.INT()),
			createTestItem("INT", DataTypes.INT()),
			createTestItem("BIGINT", DataTypes.BIGINT()),
			createTestItem("FLOAT", DataTypes.FLOAT()),
			createTestItem("DOUBLE", DataTypes.DOUBLE()),
			createTestItem("DOUBLE PRECISION", DataTypes.DOUBLE()),
			createTestItem("DATE", DataTypes.DATE()),
			createTestItem("TIME", DataTypes.TIME()),
			createTestItem("TIME WITHOUT TIME ZONE", DataTypes.TIME()),
			// Expect to be TIME(3).
			createTestItem("TIME(3)", DataTypes.TIME()),
			// Expect to be TIME(3).
			createTestItem("TIME(3) WITHOUT TIME ZONE", DataTypes.TIME()),
			createTestItem("TIMESTAMP", DataTypes.TIMESTAMP(6)),
			createTestItem("TIMESTAMP WITHOUT TIME ZONE", DataTypes.TIMESTAMP(6)),
			createTestItem("TIMESTAMP(3)", DataTypes.TIMESTAMP(3)),
			createTestItem("TIMESTAMP(3) WITHOUT TIME ZONE", DataTypes.TIMESTAMP(3)),
			createTestItem("TIMESTAMP WITH LOCAL TIME ZONE",
				DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
			createTestItem("TIMESTAMP(3) WITH LOCAL TIME ZONE",
				DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
			createTestItem("ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>",
				DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))),
			createTestItem("ARRAY<INT NOT NULL>",
				DataTypes.ARRAY(DataTypes.INT().notNull())),
			createTestItem("INT ARRAY", DataTypes.ARRAY(DataTypes.INT())),
			createTestItem("INT NOT NULL ARRAY",
				DataTypes.ARRAY(DataTypes.INT().notNull())),
			createTestItem("INT ARRAY NOT NULL",
				DataTypes.ARRAY(DataTypes.INT()).notNull()),
			createTestItem("MULTISET<INT NOT NULL>",
				DataTypes.MULTISET(DataTypes.INT().notNull())),
			createTestItem("INT MULTISET",
				DataTypes.MULTISET(DataTypes.INT())),
			createTestItem("INT NOT NULL MULTISET",
				DataTypes.MULTISET(DataTypes.INT().notNull())),
			createTestItem("INT MULTISET NOT NULL",
				DataTypes.MULTISET(DataTypes.INT()).notNull()),
			createTestItem("MAP<BIGINT, BOOLEAN>",
				DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BOOLEAN())),
			// Expect to be ROW<`f0` INT NOT NULL, `f1` BOOLEAN>.
			createTestItem("ROW<f0 INT NOT NULL, f1 BOOLEAN>",
				DataTypes.ROW(
					DataTypes.FIELD("f0", DataTypes.INT()),
					DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
			// Expect to be ROW<`f0` INT NOT NULL, `f1` BOOLEAN>.
			createTestItem("ROW(f0 INT NOT NULL, f1 BOOLEAN)",
				DataTypes.ROW(
					DataTypes.FIELD("f0", DataTypes.INT()),
					DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
			createTestItem("ROW<`f0` INT>",
				DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT()))),
			createTestItem("ROW(`f0` INT)",
				DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT()))),
			createTestItem("ROW<>", DataTypes.ROW()),
			createTestItem("ROW()", DataTypes.ROW()),
			// Expect to be ROW<`f0` INT NOT NULL '...', `f1` BOOLEAN '...'>.
			createTestItem("ROW<f0 INT NOT NULL 'This is a comment.',"
				+ " f1 BOOLEAN 'This as well.'>",
				DataTypes.ROW(
					DataTypes.FIELD("f0", DataTypes.INT()),
					DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
			createTestItem("ARRAY<ROW<f0 INT, f1 BOOLEAN>>",
				DataTypes.ARRAY(
					DataTypes.ROW(
						DataTypes.FIELD("f0", DataTypes.INT()),
						DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
			createTestItem("ROW<f0 INT, f1 BOOLEAN> MULTISET",
				DataTypes.MULTISET(
					DataTypes.ROW(
						DataTypes.FIELD("f0", DataTypes.INT()),
						DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
			createTestItem("MULTISET<ROW<f0 INT, f1 BOOLEAN>>",
				DataTypes.MULTISET(
					DataTypes.ROW(
						DataTypes.FIELD("f0", DataTypes.INT()),
						DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
			createTestItem("ROW<f0 Row<f00 INT, f01 BOOLEAN>, "
					+ "f1 INT ARRAY, "
					+ "f2 BOOLEAN MULTISET>",
				DataTypes.ROW(DataTypes.FIELD("f0",
					DataTypes.ROW(
						DataTypes.FIELD("f00", DataTypes.INT()),
						DataTypes.FIELD("f01", DataTypes.BOOLEAN()))),
					DataTypes.FIELD("f1", DataTypes.ARRAY(DataTypes.INT())),
					DataTypes.FIELD("f2", DataTypes.MULTISET(DataTypes.BOOLEAN()))))
		);
		StringBuilder buffer = new StringBuilder("create table t1(\n");
		for (int i = 0; i < testItems.size(); i++) {
			buffer.append("f")
				.append(i)
				.append(" ")
				.append(testItems.get(i).testExpr);
			if (i == testItems.size() - 1) {
				buffer.append(")");
			} else {
				buffer.append(",\n");
			}
		}
		final String sql = buffer.toString();
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
		SqlNode node = parser.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, node).get();
		TableSchema schema = ((CreateTableOperation) operation).getCatalogTable().getSchema();
		Object[] expectedDataTypes = testItems.stream().map(item -> item.expectedType).toArray();
		assertArrayEquals(expectedDataTypes, schema.getFieldDataTypes());
	}

	@Test
	public void testCreateTableWithComputedColumn() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  a int,\n" +
			"  b varchar, \n" +
			"  c as a - 1, \n" +
			"  d as b || '$$', \n" +
			"  e as my_udf1(a)," +
			"  f as `default`.my_udf2(a) + 1," +
			"  g as builtin.`default`.my_udf3(a) || '##'\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		functionCatalog.registerTempCatalogScalarFunction(
			ObjectIdentifier.of("builtin", "default", "my_udf1"),
			Func0$.MODULE$);
		functionCatalog.registerTempCatalogScalarFunction(
			ObjectIdentifier.of("builtin", "default", "my_udf2"),
			Func1$.MODULE$);
		functionCatalog.registerTempCatalogScalarFunction(
			ObjectIdentifier.of("builtin", "default", "my_udf3"),
			Func8$.MODULE$);
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		Operation operation = parse(sql, planner, getParserBySqlDialect(SqlDialect.DEFAULT));
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		assertArrayEquals(
			new String[] {"a", "b", "c", "d", "e", "f", "g"},
			catalogTable.getSchema().getFieldNames());
		assertArrayEquals(
			new DataType[]{
				DataTypes.INT(),
				DataTypes.STRING(),
				DataTypes.INT(),
				DataTypes.STRING(),
				DataTypes.INT().notNull(),
				DataTypes.INT(),
				DataTypes.STRING()},
			catalogTable.getSchema().getFieldDataTypes());
		String[] columnExpressions =
			catalogTable.getSchema().getTableColumns().stream()
				.filter(TableColumn::isGenerated)
				.map(c -> c.getExpr().orElse(null))
				.toArray(String[]::new);
		String[] expected = new String[] {
			"`a` - 1",
			"`b` || '$$'",
			"`builtin`.`default`.`my_udf1`(`a`)",
			"`builtin`.`default`.`my_udf2`(`a`) + 1",
			"`builtin`.`default`.`my_udf3`(`a`) || '##'"
		};
		assertArrayEquals(
			expected,
			columnExpressions);
	}

	@Test
	public void testAlterTable() throws Exception {
		Catalog catalog = new GenericInMemoryCatalog("default", "default");
		catalogManager.registerCatalog("cat1", catalog);
		catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder().field("a", DataTypes.STRING()).build(),
				new HashMap<>(),
				"tb1");
		catalogManager.setCurrentCatalog("cat1");
		catalogManager.setCurrentDatabase("db1");
		catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable, true);
		final String[] renameTableSqls = new String[] {
				"alter table cat1.db1.tb1 rename to tb2",
				"alter table db1.tb1 rename to tb2",
				"alter table tb1 rename to cat1.db1.tb2",
		};
		final ObjectIdentifier expectedIdentifier =
				ObjectIdentifier.of("cat1", "db1", "tb1");
		final ObjectIdentifier expectedNewIdentifier =
				ObjectIdentifier.of("cat1", "db1", "tb2");
		//test rename table converter
		for (int i = 0; i < renameTableSqls.length; i++) {
			Operation operation = parse(renameTableSqls[i], SqlDialect.DEFAULT);
			assert operation instanceof AlterTableRenameOperation;
			final AlterTableRenameOperation alterTableRenameOperation = (AlterTableRenameOperation) operation;
			assertEquals(expectedIdentifier, alterTableRenameOperation.getTableIdentifier());
			assertEquals(expectedNewIdentifier, alterTableRenameOperation.getNewTableIdentifier());
		}
		// test alter table properties
		Operation operation = parse("alter table cat1.db1.tb1 set ('k1' = 'v1', 'K2' = 'V2')", SqlDialect.DEFAULT);
		assert operation instanceof AlterTablePropertiesOperation;
		final AlterTablePropertiesOperation alterTablePropertiesOperation = (AlterTablePropertiesOperation) operation;
		assertEquals(expectedIdentifier, alterTablePropertiesOperation.getTableIdentifier());
		assertEquals(2, alterTablePropertiesOperation.getCatalogTable().getProperties().size());
		Map<String, String> properties = new HashMap<>();
		properties.put("k1", "v1");
		properties.put("K2", "V2");
		assertEquals(properties, alterTablePropertiesOperation.getCatalogTable().getProperties());
	}

	@Test
	public void testAlterTableAddPkConstraint() throws Exception {
		Catalog catalog = new GenericInMemoryCatalog("default", "default");
		catalogManager.registerCatalog("cat1", catalog);
		catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder()
						.field("a", DataTypes.STRING().notNull())
						.field("b", DataTypes.BIGINT().notNull())
						.field("c", DataTypes.BIGINT())
						.build(),
				new HashMap<>(),
				"tb1");
		catalogManager.setCurrentCatalog("cat1");
		catalogManager.setCurrentDatabase("db1");
		catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable, true);
		// Test alter add table constraint.
		Operation operation = parse("alter table tb1 add constraint ct1 primary key(a, b) not enforced",
				SqlDialect.DEFAULT);
		assert operation instanceof AlterTableAddConstraintOperation;
		AlterTableAddConstraintOperation addConstraintOperation =
				(AlterTableAddConstraintOperation) operation;
		assertThat(addConstraintOperation.asSummaryString(),
				is("ALTER TABLE ADD CONSTRAINT: (identifier: [`cat1`.`db1`.`tb1`], "
						+ "constraintName: [ct1], columns: [a, b])"));
		// Test alter table add pk on nullable column
		exceptionRule.expect(ValidationException.class);
		exceptionRule.expectMessage("Could not create a PRIMARY KEY 'ct1'. Column 'c' is nullable.");
		parse("alter table tb1 add constraint ct1 primary key(c) not enforced",
				SqlDialect.DEFAULT);
	}

	@Test
	public void testAlterTableAddPkConstraintEnforced() throws Exception {
		Catalog catalog = new GenericInMemoryCatalog("default", "default");
		catalogManager.registerCatalog("cat1", catalog);
		catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder()
						.field("a", DataTypes.STRING().notNull())
						.field("b", DataTypes.BIGINT().notNull())
						.field("c", DataTypes.BIGINT())
						.build(),
				new HashMap<>(),
				"tb1");
		catalogManager.setCurrentCatalog("cat1");
		catalogManager.setCurrentDatabase("db1");
		catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable, true);
		// Test alter table add enforced
		exceptionRule.expect(ValidationException.class);
		exceptionRule.expectMessage("Flink doesn't support ENFORCED mode for PRIMARY KEY constaint. "
				+ "ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the "
				+ "incoming/outgoing data. Flink does not own the data therefore the "
				+ "only supported mode is the NOT ENFORCED mode");
		parse("alter table tb1 add constraint ct1 primary key(a, b)",
				SqlDialect.DEFAULT);
	}

	@Test
	public void testAlterTableAddUniqueConstraint() throws Exception {
		Catalog catalog = new GenericInMemoryCatalog("default", "default");
		catalogManager.registerCatalog("cat1", catalog);
		catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder()
						.field("a", DataTypes.STRING().notNull())
						.field("b", DataTypes.BIGINT().notNull())
						.build(),
				new HashMap<>(),
				"tb1");
		catalogManager.setCurrentCatalog("cat1");
		catalogManager.setCurrentDatabase("db1");
		catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable, true);
		// Test alter add table constraint.
		exceptionRule.expect(UnsupportedOperationException.class);
		exceptionRule.expectMessage("UNIQUE constraint is not supported yet");
		parse("alter table tb1 add constraint ct1 unique(a, b) not enforced",
				SqlDialect.DEFAULT);
	}

	@Test
	public void testAlterTableAddUniqueConstraintEnforced() throws Exception {
		Catalog catalog = new GenericInMemoryCatalog("default", "default");
		catalogManager.registerCatalog("cat1", catalog);
		catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder()
						.field("a", DataTypes.STRING().notNull())
						.field("b", DataTypes.BIGINT().notNull())
						.field("c", DataTypes.BIGINT())
						.build(),
				new HashMap<>(),
				"tb1");
		catalogManager.setCurrentCatalog("cat1");
		catalogManager.setCurrentDatabase("db1");
		catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable, true);
		// Test alter table add enforced
		exceptionRule.expect(UnsupportedOperationException.class);
		exceptionRule.expectMessage("UNIQUE constraint is not supported yet");
		parse("alter table tb1 add constraint ct1 unique(a, b)",
				SqlDialect.DEFAULT);
	}

	@Test
	public void testAlterTableDropConstraint() throws Exception {
		Catalog catalog = new GenericInMemoryCatalog("default", "default");
		catalogManager.registerCatalog("cat1", catalog);
		catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder()
						.field("a", DataTypes.STRING().notNull())
						.field("b", DataTypes.BIGINT().notNull())
						.field("c", DataTypes.BIGINT())
						.primaryKey("ct1", new String[] { "a", "b" })
						.build(),
				new HashMap<>(),
				"tb1");
		catalogManager.setCurrentCatalog("cat1");
		catalogManager.setCurrentDatabase("db1");
		catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable, true);
		// Test alter table add enforced
		Operation operation = parse("alter table tb1 drop constraint ct1", SqlDialect.DEFAULT);
		assert operation instanceof AlterTableDropConstraintOperation;
		AlterTableDropConstraintOperation dropConstraint = (AlterTableDropConstraintOperation) operation;
		assertThat(dropConstraint.asSummaryString(), is("ALTER TABLE `cat1`.`db1`.`tb1` DROP CONSTRAINT ct1"));
		exceptionRule.expect(ValidationException.class);
		exceptionRule.expectMessage("CONSTRAINT [ct2] does not exist");
		parse("alter table tb1 drop constraint ct2", SqlDialect.DEFAULT);
	}

	//~ Tool Methods ----------------------------------------------------------

	private static TestItem createTestItem(Object... args) {
		assert args.length == 2;
		final String testExpr = (String) args[0];
		TestItem testItem = TestItem.fromTestExpr(testExpr);
		if (args[1] instanceof String) {
			testItem.withExpectedError((String) args[1]);
		} else {
			testItem.withExpectedType(args[1]);
		}
		return testItem;
	}

	private Operation parse(String sql, FlinkPlannerImpl planner, CalciteParser parser) {
		SqlNode node = parser.parse(sql);
		return SqlToOperationConverter.convert(planner, catalogManager, node).get();
	}

	private Operation parse(String sql, SqlDialect sqlDialect) {
		FlinkPlannerImpl planner = getPlannerBySqlDialect(sqlDialect);
		final CalciteParser parser = getParserBySqlDialect(sqlDialect);
		SqlNode node = parser.parse(sql);
		return SqlToOperationConverter.convert(planner, catalogManager, node).get();
	}

	private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
		tableConfig.setSqlDialect(sqlDialect);
		return plannerContext.createFlinkPlanner(catalogManager.getCurrentCatalog(),
			catalogManager.getCurrentDatabase());
	}

	private CalciteParser getParserBySqlDialect(SqlDialect sqlDialect) {
		tableConfig.setSqlDialect(sqlDialect);
		return plannerContext.createCalciteParser();
	}

	//~ Inner Classes ----------------------------------------------------------

	private static class TestItem {
		private final String testExpr;
		@Nullable
		private Object expectedType;
		@Nullable
		private String expectedError;

		private TestItem(String testExpr) {
			this.testExpr = testExpr;
		}

		static TestItem fromTestExpr(String testExpr) {
			return new TestItem(testExpr);
		}

		TestItem withExpectedType(Object expectedType) {
			this.expectedType = expectedType;
			return this;
		}

		TestItem withExpectedError(String expectedError) {
			this.expectedError = expectedError;
			return this;
		}

		@Override
		public String toString() {
			return this.testExpr;
		}
	}

}
