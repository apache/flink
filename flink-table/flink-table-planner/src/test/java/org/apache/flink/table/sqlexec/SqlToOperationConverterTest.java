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
import org.apache.flink.table.api.Types;
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
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

	@Test // TODO: tweak the tests when FLINK-13604 is fixed.
	public void testCreateTableWithFullDataTypes() {
		final List<TestItem> testItems = Arrays.asList(
			// Expect to be DataTypes.CHAR(1).
			createTestItem("CHAR", DataTypes.STRING()),
			// Expect to be DataTypes.CHAR(1).notNull().
			createTestItem("CHAR NOT NULL", DataTypes.STRING()),
			// Expect to be DataTypes.CHAR(1).
			createTestItem("CHAR NULL", DataTypes.STRING()),
			// Expect to be DataTypes.CHAR(33).
			createTestItem("CHAR(33)", DataTypes.STRING()),
			createTestItem("VARCHAR", DataTypes.STRING()),
			// Expect to be DataTypes.VARCHAR(33).
			createTestItem("VARCHAR(33)", DataTypes.STRING()),
			createTestItem("STRING", DataTypes.STRING()),
			createTestItem("BOOLEAN", DataTypes.BOOLEAN()),
			// Expect to be DECIMAL(10, 0).
			createTestItem("DECIMAL",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 0).
			createTestItem("DEC",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 0).
			createTestItem("NUMERIC",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 0).
			createTestItem("DECIMAL(10)",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 0).
			createTestItem("DEC(10)",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 0).
			createTestItem("NUMERIC(10)",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 3).
			createTestItem("DECIMAL(10, 3)",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 3).
			createTestItem("DEC(10, 3)",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			// Expect to be DECIMAL(10, 3).
			createTestItem("NUMERIC(10, 3)",
				TypeConversions.fromLegacyInfoToDataType(Types.DECIMAL())),
			createTestItem("TINYINT", DataTypes.TINYINT()),
			createTestItem("SMALLINT", DataTypes.SMALLINT()),
			createTestItem("INTEGER", DataTypes.INT()),
			createTestItem("INT", DataTypes.INT()),
			createTestItem("BIGINT", DataTypes.BIGINT()),
			createTestItem("FLOAT", DataTypes.FLOAT()),
			createTestItem("DOUBLE", DataTypes.DOUBLE()),
			createTestItem("DOUBLE PRECISION", DataTypes.DOUBLE()),
			createTestItem("DATE",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_DATE())),
			createTestItem("TIME",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIME())),
			createTestItem("TIME WITHOUT TIME ZONE",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIME())),
			// Expect to be Time(3).
			createTestItem("TIME(3)",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIME())),
			// Expect to be Time(3).
			createTestItem("TIME(3) WITHOUT TIME ZONE",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIME())),
			createTestItem("TIMESTAMP",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIMESTAMP())),
			createTestItem("TIMESTAMP WITHOUT TIME ZONE",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIMESTAMP())),
			// Expect to be timestamp(3).
			createTestItem("TIMESTAMP(3)",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIMESTAMP())),
			// Expect to be timestamp(3).
			createTestItem("TIMESTAMP(3) WITHOUT TIME ZONE",
				TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIMESTAMP())),
			// Expect to be ARRAY<INT NOT NULL>.
			createTestItem("ARRAY<INT NOT NULL>",
				DataTypes.ARRAY(DataTypes.INT())),
			createTestItem("INT ARRAY", DataTypes.ARRAY(DataTypes.INT())),
			// Expect to be ARRAY<INT NOT NULL>.
			createTestItem("INT NOT NULL ARRAY",
				DataTypes.ARRAY(DataTypes.INT())),
			// Expect to be ARRAY<INT> NOT NULL.
			createTestItem("INT ARRAY NOT NULL",
				DataTypes.ARRAY(DataTypes.INT())),
			// Expect to be MULTISET<INT NOT NULL>.
			createTestItem("MULTISET<INT NOT NULL>",
				DataTypes.MULTISET(DataTypes.INT())),
			createTestItem("INT MULTISET", DataTypes.MULTISET(DataTypes.INT())),
			// Expect to be MULTISET<INT NOT NULL>.
			createTestItem("INT NOT NULL MULTISET",
				DataTypes.MULTISET(DataTypes.INT())),
			// Expect to be MULTISET<INT> NOT NULL.
			createTestItem("INT MULTISET NOT NULL",
				DataTypes.MULTISET(DataTypes.INT())),
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
			createTestItem("ROW<f0 INT NOT NULL 'This is a comment.', "
					+ "f1 BOOLEAN 'This as well.'>",
				DataTypes.ROW(
					DataTypes.FIELD("f0", DataTypes.INT()),
					DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
			createTestItem("ROW<f0 INT, f1 BOOLEAN> ARRAY",
				DataTypes.ARRAY(
					DataTypes.ROW(
						DataTypes.FIELD("f0", DataTypes.INT()),
						DataTypes.FIELD("f1", DataTypes.BOOLEAN())))),
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
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		TableSchema schema = ((CreateTableOperation) operation).getCatalogTable().getSchema();
		Object[] expectedDataTypes = testItems.stream().map(item -> item.expectedType).toArray();
		assertArrayEquals(expectedDataTypes, schema.getFieldDataTypes());
	}

	@Test
	public void testCreateTableWithUnSupportedDataTypes() {
		final List<TestItem> testItems = Arrays.asList(
			createTestItem("ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>",
				"Type is not supported: TIMESTAMP_WITH_LOCAL_TIME_ZONE"),
			createTestItem("TIMESTAMP(3) WITH LOCAL TIME ZONE",
				"Type is not supported: TIMESTAMP_WITH_LOCAL_TIME_ZONE"),
			createTestItem("TIMESTAMP WITH LOCAL TIME ZONE",
				"Type is not supported: TIMESTAMP_WITH_LOCAL_TIME_ZONE"),
			createTestItem("BYTES", "Type is not supported: VARBINARY"),
			createTestItem("VARBINARY(33)", "Type is not supported: VARBINARY"),
			createTestItem("VARBINARY", "Type is not supported: VARBINARY"),
			createTestItem("BINARY(33)", "Type is not supported: BINARY"),
			createTestItem("BINARY", "Type is not supported: BINARY")
		);
		final String sqlTemplate = "create table t1(\n" +
			"  f0 %s)";
		final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		for (TestItem item : testItems) {
			String sql = String.format(sqlTemplate, item.testExpr);
			SqlNode node = planner.parse(sql);
			assert node instanceof SqlCreateTable;
			expectedEx.expect(TableException.class);
			expectedEx.expectMessage(item.expectedError);
			SqlToOperationConverter.convert(planner, node);
		}
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

	private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
		tableConfig.setSqlDialect(sqlDialect);
		return planningConfigurationBuilder.createFlinkPlanner(catalogManager.getCurrentCatalog(),
			catalogManager.getCurrentDatabase());
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
