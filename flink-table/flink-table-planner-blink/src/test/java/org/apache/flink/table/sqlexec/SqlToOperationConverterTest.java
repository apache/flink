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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.operations.SqlConversionException;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test cases for {@link org.apache.flink.table.planner.operations.SqlToOperationConverter}.
 */
public class SqlToOperationConverterTest {
	private final TableConfig tableConfig = new TableConfig();
	private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog",
		"default");
	private final CatalogManager catalogManager =
		new CatalogManager("builtin", catalog);
	private final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);
	private final PlannerContext plannerContext =
		new PlannerContext(tableConfig,
			functionCatalog,
			asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
			new ArrayList<>());

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
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
		Operation operation = parse(sql, planner);
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

	@Test(expected = SqlConversionException.class)
	public void testCreateTableWithPkUniqueKeys() {
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
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
		parse(sql, planner);
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

	@Test
	public void testSqlInsertWithStaticPartition() {
		final String sql = "insert into t1 partition(a=1) select b, c, d from t2";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.HIVE);
		Operation operation = parse(sql, planner);
		assert operation instanceof CatalogSinkModifyOperation;
		CatalogSinkModifyOperation sinkModifyOperation = (CatalogSinkModifyOperation) operation;
		final Map<String, String> expectedStaticPartitions = new HashMap<>();
		expectedStaticPartitions.put("a", "1");
		assertEquals(expectedStaticPartitions, sinkModifyOperation.getStaticPartitions());
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
			createTestItem("TIMESTAMP", DataTypes.TIMESTAMP(3)),
			createTestItem("TIMESTAMP WITHOUT TIME ZONE", DataTypes.TIMESTAMP(3)),
			createTestItem("TIMESTAMP(3)", DataTypes.TIMESTAMP(3)),
			createTestItem("TIMESTAMP(3) WITHOUT TIME ZONE", DataTypes.TIMESTAMP(3)),
			createTestItem("TIMESTAMP WITH LOCAL TIME ZONE",
				DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
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
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		TableSchema schema = ((CreateTableOperation) operation).getCatalogTable().getSchema();
		Object[] expectedDataTypes = testItems.stream().map(item -> item.expectedType).toArray();
		assertArrayEquals(expectedDataTypes, schema.getFieldDataTypes());
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

	private Operation parse(String sql, FlinkPlannerImpl planner) {
		SqlNode node = planner.parse(sql);
		return SqlToOperationConverter.convert(planner, node);
	}

	private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
		tableConfig.setSqlDialect(sqlDialect);
		return plannerContext.createFlinkPlanner(catalogManager.getCurrentCatalog(),
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
