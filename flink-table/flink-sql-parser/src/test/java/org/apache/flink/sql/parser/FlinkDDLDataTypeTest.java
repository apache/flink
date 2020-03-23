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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.SqlValidatorTestCase;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.Util;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for all the supported Flink DDL data types.
 */
@RunWith(Parameterized.class)
public class FlinkDDLDataTypeTest {
	private static final Fixture FIXTURE =
		new Fixture(TestFactory.INSTANCE.getTypeFactory());
	private static final String DDL_FORMAT = "create table t1 (\n" +
		"  f0 %s\n" +
		") with (\n" +
		"  'k1' = 'v1'\n" +
		")";

	@Parameterized.Parameters(name = "{index}: {0}")
	public static List<TestItem> testData() {
		return Arrays.asList(
			createTestItem("CHAR", nullable(FIXTURE.char1Type), "CHAR"),
			createTestItem("CHAR NOT NULL", FIXTURE.char1Type, "CHAR NOT NULL"),
			createTestItem("CHAR   NOT \t\nNULL", FIXTURE.char1Type, "CHAR NOT NULL"),
			createTestItem("char not null", FIXTURE.char1Type, "CHAR NOT NULL"),
			createTestItem("CHAR NULL", nullable(FIXTURE.char1Type), "CHAR"),
			createTestItem("CHAR(33)", nullable(FIXTURE.char33Type), "CHAR(33)"),
			createTestItem("VARCHAR", nullable(FIXTURE.varcharType), "VARCHAR"),
			createTestItem("VARCHAR(33)", nullable(FIXTURE.varchar33Type), "VARCHAR(33)"),
			createTestItem("STRING",
				nullable(FIXTURE.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE)), "STRING"),
			createTestItem("BOOLEAN", nullable(FIXTURE.booleanType), "BOOLEAN"),
			createTestItem("BINARY", nullable(FIXTURE.binaryType), "BINARY"),
			createTestItem("BINARY(33)", nullable(FIXTURE.binary33Type), "BINARY(33)"),
			createTestItem("VARBINARY", nullable(FIXTURE.varbinaryType), "VARBINARY"),
			createTestItem("VARBINARY(33)", nullable(FIXTURE.varbinary33Type),
				"VARBINARY(33)"),
			createTestItem("BYTES",
				nullable(FIXTURE.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE)),
				"BYTES"),
			createTestItem("DECIMAL", nullable(FIXTURE.decimalType), "DECIMAL"),
			createTestItem("DEC", nullable(FIXTURE.decimalType), "DECIMAL"),
			createTestItem("NUMERIC", nullable(FIXTURE.decimalType), "DECIMAL"),
			createTestItem("DECIMAL(10)", nullable(FIXTURE.decimalP10S0Type), "DECIMAL(10)"),
			createTestItem("DEC(10)", nullable(FIXTURE.decimalP10S0Type), "DECIMAL(10)"),
			createTestItem("NUMERIC(10)", nullable(FIXTURE.decimalP10S0Type), "DECIMAL(10)"),
			createTestItem("DECIMAL(10, 3)", nullable(FIXTURE.decimalP10S3Type),
				"DECIMAL(10, 3)"),
			createTestItem("DEC(10, 3)", nullable(FIXTURE.decimalP10S3Type),
				"DECIMAL(10, 3)"),
			createTestItem("NUMERIC(10, 3)", nullable(FIXTURE.decimalP10S3Type),
				"DECIMAL(10, 3)"),
			createTestItem("TINYINT", nullable(FIXTURE.tinyintType), "TINYINT"),
			createTestItem("SMALLINT", nullable(FIXTURE.smallintType), "SMALLINT"),
			createTestItem("INTEGER", nullable(FIXTURE.intType), "INTEGER"),
			createTestItem("INT", nullable(FIXTURE.intType), "INTEGER"),
			createTestItem("BIGINT", nullable(FIXTURE.bigintType), "BIGINT"),
			createTestItem("FLOAT", nullable(FIXTURE.floatType), "FLOAT"),
			createTestItem("DOUBLE", nullable(FIXTURE.doubleType), "DOUBLE"),
			createTestItem("DOUBLE PRECISION", nullable(FIXTURE.doubleType), "DOUBLE"),
			createTestItem("DATE", nullable(FIXTURE.dateType), "DATE"),
			createTestItem("TIME", nullable(FIXTURE.timeType), "TIME"),
			createTestItem("TIME WITHOUT TIME ZONE", nullable(FIXTURE.timeType), "TIME"),
			createTestItem("TIME(3)", nullable(FIXTURE.time3Type), "TIME(3)"),
			createTestItem("TIME(3) WITHOUT TIME ZONE", nullable(FIXTURE.time3Type),
				"TIME(3)"),
			createTestItem("TIMESTAMP", nullable(FIXTURE.timestampType), "TIMESTAMP"),
			createTestItem("TIMESTAMP WITHOUT TIME ZONE", nullable(FIXTURE.timestampType),
				"TIMESTAMP"),
			createTestItem("TIMESTAMP(3)", nullable(FIXTURE.timestamp3Type), "TIMESTAMP(3)"),
			createTestItem("TIMESTAMP(3) WITHOUT TIME ZONE",
				nullable(FIXTURE.timestamp3Type), "TIMESTAMP(3)"),
			createTestItem("TIMESTAMP WITH LOCAL TIME ZONE",
				nullable(FIXTURE.timestampWithLocalTimeZoneType),
				"TIMESTAMP WITH LOCAL TIME ZONE"),
			createTestItem("TIMESTAMP(3) WITH LOCAL TIME ZONE",
				nullable(FIXTURE.timestamp3WithLocalTimeZoneType),
				"TIMESTAMP(3) WITH LOCAL TIME ZONE"),
			createTestItem("ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>",
				nullable(FIXTURE.createArrayType(nullable(FIXTURE.timestamp3WithLocalTimeZoneType))),
				"ARRAY< TIMESTAMP(3) WITH LOCAL TIME ZONE >"),
			createTestItem("ARRAY<INT NOT NULL>",
				nullable(FIXTURE.createArrayType(FIXTURE.intType)),
				"ARRAY< INTEGER NOT NULL >"),
			createTestItem("INT ARRAY",
				nullable(FIXTURE.createArrayType(nullable(FIXTURE.intType))),
				"INTEGER ARRAY"),
			createTestItem("INT NOT NULL ARRAY",
				nullable(FIXTURE.createArrayType(FIXTURE.intType)),
				"INTEGER NOT NULL ARRAY"),
			createTestItem("INT ARRAY NOT NULL",
				FIXTURE.createArrayType(nullable(FIXTURE.intType)),
				"INTEGER ARRAY NOT NULL"),
			createTestItem("MULTISET<INT NOT NULL>",
				nullable(FIXTURE.createMultisetType(FIXTURE.intType)),
				"MULTISET< INTEGER NOT NULL >"),
			createTestItem("INT MULTISET",
				nullable(FIXTURE.createMultisetType(nullable(FIXTURE.intType))),
				"INTEGER MULTISET"),
			createTestItem("INT NOT NULL MULTISET",
				nullable(FIXTURE.createMultisetType(FIXTURE.intType)),
				"INTEGER NOT NULL MULTISET"),
			createTestItem("INT MULTISET NOT NULL",
				FIXTURE.createMultisetType(nullable(FIXTURE.intType)),
				"INTEGER MULTISET NOT NULL"),
			createTestItem("MAP<BIGINT, BOOLEAN>",
				nullable(FIXTURE.createMapType(
					nullable(FIXTURE.bigintType),
					nullable(FIXTURE.booleanType))),
				"MAP< BIGINT, BOOLEAN >"),
			createTestItem("ROW<f0 INT NOT NULL, f1 BOOLEAN>",
				nullable(FIXTURE.createStructType(
					Arrays.asList(FIXTURE.intType, nullable(FIXTURE.booleanType)),
					Arrays.asList("f0", "f1"))),
				"ROW< `f0` INTEGER NOT NULL, `f1` BOOLEAN >"),
			createTestItem("ROW(f0 INT NOT NULL, f1 BOOLEAN)",
				nullable(FIXTURE.createStructType(
					Arrays.asList(FIXTURE.intType, nullable(FIXTURE.booleanType)),
					Arrays.asList("f0", "f1"))),
				"ROW(`f0` INTEGER NOT NULL, `f1` BOOLEAN)"),
			createTestItem("ROW<`f0` INT>",
				nullable(FIXTURE.createStructType(
					Collections.singletonList(nullable(FIXTURE.intType)),
					Collections.singletonList("f0"))),
				"ROW< `f0` INTEGER >"),
			createTestItem("ROW(`f0` INT)",
				nullable(FIXTURE.createStructType(
					Collections.singletonList(nullable(FIXTURE.intType)),
					Collections.singletonList("f0"))),
				"ROW(`f0` INTEGER)"),
			createTestItem("ROW<>",
				nullable(FIXTURE.createStructType(
					Collections.emptyList(),
					Collections.emptyList())),
				"ROW<>"),
			createTestItem("ROW()",
				nullable(FIXTURE.createStructType(
					Collections.emptyList(),
					Collections.emptyList())),
				"ROW()"),
			createTestItem("ROW<f0 INT NOT NULL 'This is a comment.', "
					+ "f1 BOOLEAN 'This as well.'>",
				nullable(FIXTURE.createStructType(
					Arrays.asList(FIXTURE.intType, nullable(FIXTURE.booleanType)),
					Arrays.asList("f0", "f1"))),
				"ROW< `f0` INTEGER NOT NULL 'This is a comment.', "
					+ "`f1` BOOLEAN 'This as well.' >"),

			// Test parse throws error.
			createTestItem("TIMESTAMP WITH ^TIME^ ZONE",
				"(?s).*Encountered \"TIME\" at .*"),
			createTestItem("TIMESTAMP(3) WITH ^TIME^ ZONE",
				"(?s).*Encountered \"TIME\" at .*"),
			createTestItem("^NULL^",
				"(?s).*Encountered \"NULL\" at line 2, column 6..*"),
			createTestItem("cat.db.MyType", null, "`cat`.`db`.`MyType`"),
			createTestItem("`db`.`MyType`", null, "`db`.`MyType`"),
			createTestItem("MyType", null, "`MyType`"),
			createTestItem("ARRAY<MyType>", null, "ARRAY< `MyType` >"),
			createTestItem("ROW<f0 MyType, f1 `c`.`d`.`t`>", null,
				"ROW< `f0` `MyType`, `f1` `c`.`d`.`t` >"),
			createTestItem("^INTERVAL^ YEAR",
				"(?s).*Encountered \"INTERVAL\" at line 2, column 6..*"),
			createTestItem("ANY(^'unknown.class'^, '')",
				"(?s).*Encountered \"\\\\'unknown.class\\\\'\" at line 2, column 10.\n.*"
					+ "Was expecting:\n"
					+ "    <UNSIGNED_INTEGER_LITERAL> ...\n"
					+ ".*"));
	}

	private static TestItem createTestItem(Object... args) {
		assert args.length >= 2;
		final String testExpr = (String) args[0];
		TestItem testItem = TestItem.fromTestExpr(testExpr);
		if (args[1] instanceof String) {
			testItem.withExpectedError((String) args[1]);
		} else if (args[1] instanceof RelDataType) {
			testItem.withExpectedType((RelDataType) args[1]);
		}
		if (args.length == 3) {
			testItem.withExpectedUnparsed((String) args[2]);
		}
		return testItem;
	}

	@Parameterized.Parameter
	public TestItem testItem;

	@Test
	public void testDataTypeParsing() {
		if (testItem.expectedType != null) {
			checkType(testItem.testExpr, testItem.expectedType);
		}
	}

	@Test
	public void testThrowsError() {
		if (testItem.expectedError != null) {
			checkFails(testItem.testExpr, testItem.expectedError);
		}
	}

	@Test
	public void testDataTypeUnparsing() {
		if (testItem.expectedUnparsed != null) {
			checkUnparseTo(testItem.testExpr, testItem.expectedUnparsed);
		}
	}

	private static RelDataType nullable(RelDataType type) {
		return FIXTURE.nullable(type);
	}

	private void checkType(String typeExpr, RelDataType expectedType) {
		this.sql(String.format(DDL_FORMAT, typeExpr)).checkType(expectedType);
	}

	private void checkFails(String typeExpr, String expectedMsgPattern) {
		sql(String.format(DDL_FORMAT, typeExpr)).fails(expectedMsgPattern);
	}

	private void checkUnparseTo(String typeExpr, String expectedUnparsed) {
		sql(String.format(DDL_FORMAT, typeExpr)).unparsedTo(expectedUnparsed);
	}

	private Tester getTester() {
		return new TesterImpl(TestFactory.INSTANCE);
	}

	private Sql sql(String sql) {
		return new Sql(sql);
	}

	//~ Inner Classes ----------------------------------------------------------

	private static class TestItem {
		private final String testExpr;
		@Nullable
		private RelDataType expectedType;
		@Nullable
		private String expectedError;
		@Nullable
		private String expectedUnparsed;

		private TestItem(String testExpr) {
			this.testExpr = testExpr;
		}

		static TestItem fromTestExpr(String testExpr) {
			return new TestItem(testExpr);
		}

		TestItem withExpectedType(RelDataType expectedType) {
			this.expectedType = expectedType;
			return this;
		}

		TestItem withExpectedError(String expectedError) {
			this.expectedError = expectedError;
			return this;
		}

		TestItem withExpectedUnparsed(String expectedUnparsed) {
			this.expectedUnparsed = expectedUnparsed;
			return this;
		}

		@Override
		public String toString() {
			return this.testExpr;
		}
	}

	private class Sql {
		private final String sql;

		Sql(String sql) {
			this.sql = sql;
		}

		public Sql checkType(RelDataType type) {
			getTester().checkType(this.sql, type);
			return this;
		}

		public Sql fails(String expectedMsgPattern) {
			getTester().checkFails(this.sql, expectedMsgPattern);
			return this;
		}

		public Sql unparsedTo(String expectedUnparsed) {
			getTester().checkUnparsed(this.sql, expectedUnparsed);
			return this;
		}
	}

	/**
	 * Callback to control how test actions are performed.
	 */
	protected interface Tester {
		void checkType(String sql, RelDataType type);

		void checkFails(String sql, String expectedMsgPattern);

		void checkUnparsed(String sql, String expectedUnparsed);
	}

	/**
	 * Default implementation of {@link SqlParserTest.Tester}.
	 */
	protected class TesterImpl implements Tester {
		private TestFactory factory;

		TesterImpl(TestFactory factory) {
			this.factory = factory;
		}

		public void checkType(String sql, RelDataType type) {
			final SqlNode sqlNode = parseStmtAndHandleEx(sql);
			assert sqlNode instanceof SqlCreateTable;
			final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
			SqlNodeList columns = sqlCreateTable.getColumnList();
			assert columns.size() == 1;
			RelDataType columnType = ((SqlTableColumn) columns.get(0)).getType()
				.deriveType(factory.getValidator());
			assertEquals(type, columnType);
		}

		private SqlNode parseStmtAndHandleEx(String sql) {
			final SqlNode sqlNode;
			try {
				sqlNode = factory.createParser(sql).parseStmt();
			} catch (SqlParseException e) {
				throw new RuntimeException("Error while parsing SQL: " + sql, e);
			}
			return sqlNode;
		}

		public void checkFails(
			String sql,
			String expectedMsgPattern) {
			SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
			Throwable thrown = null;
			try {
				final SqlNode sqlNode;
				sqlNode = factory.createParser(sap.sql).parseStmt();
				Util.discard(sqlNode);
			} catch (Throwable ex) {
				thrown = ex;
			}

			checkEx(expectedMsgPattern, sap, thrown);
		}

		public void checkUnparsed(String sql, String expectedUnparsed) {
			final SqlNode sqlNode = parseStmtAndHandleEx(sql);
			assert sqlNode instanceof SqlCreateTable;
			final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
			SqlNodeList columns = sqlCreateTable.getColumnList();
			assert columns.size() == 1;
			SqlDataTypeSpec dataTypeSpec = ((SqlTableColumn) columns.get(0)).getType();
			SqlWriter sqlWriter = new SqlPrettyWriter(factory.createSqlDialect(), false);
			dataTypeSpec.unparse(sqlWriter, 0, 0);
			// SqlDataTypeSpec does not take care of the nullable attribute unparse,
			// So we unparse nullable attribute specifically, this unparsing logic should
			// keep sync with SqlTableColumn.
			if (!dataTypeSpec.getNullable()) {
				sqlWriter.keyword("NOT NULL");
			}
			assertEquals(expectedUnparsed, sqlWriter.toSqlString().getSql());
		}

		private void checkEx(String expectedMsgPattern,
				SqlParserUtil.StringAndPos sap,
				Throwable thrown) {
			SqlValidatorTestCase.checkEx(thrown, expectedMsgPattern, sap);
		}
	}

	/**
	 * Factory to supply test components.
	 */
	private static class TestFactory {
		static final Map<String, Object> DEFAULT_OPTIONS = buildDefaultOptions();

		public static final TestFactory INSTANCE = new TestFactory();

		private final Map<String, Object> options;
		private final SqlTestFactory.ValidatorFactory validatorFactory;

		private final RelDataTypeFactory typeFactory;
		private final SqlOperatorTable operatorTable;
		private final SqlValidatorCatalogReader catalogReader;
		private final SqlParser.Config parserConfig;

		TestFactory() {
			this(DEFAULT_OPTIONS, MockCatalogReaderSimple::new, SqlValidatorUtil::newValidator);
		}

		TestFactory(
				Map<String, Object> options,
				SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory,
				SqlTestFactory.ValidatorFactory validatorFactory) {
			this.options = options;
			this.validatorFactory = validatorFactory;
			this.operatorTable =
				createOperatorTable((SqlOperatorTable) options.get("operatorTable"));
			this.typeFactory = createTypeFactory((SqlConformance) options.get("conformance"));
			Boolean caseSensitive = (Boolean) options.get("caseSensitive");
			this.catalogReader = catalogReaderFactory.create(typeFactory, caseSensitive).init();
			this.parserConfig = createParserConfig(options);
		}

		public SqlParser createParser(String sql) {
			return SqlParser.create(new SourceStringReader(sql), parserConfig);
		}

		public SqlDialect createSqlDialect() {
			return new CalciteSqlDialect(SqlDialect.EMPTY_CONTEXT
				.withQuotedCasing(parserConfig.unquotedCasing())
				.withConformance(parserConfig.conformance())
				.withUnquotedCasing(parserConfig.unquotedCasing())
				.withIdentifierQuoteString(parserConfig.quoting().string));
		}

		public RelDataTypeFactory getTypeFactory() {
			return this.typeFactory;
		}

		public SqlValidator getValidator() {
			final SqlConformance conformance =
				(SqlConformance) options.get("conformance");
			final boolean enableTypeCoercion = (boolean) options.get("enableTypeCoercion");
			return validatorFactory.create(operatorTable,
				catalogReader,
				typeFactory,
				conformance)
				.setEnableTypeCoercion(enableTypeCoercion);
		}

		private static SqlOperatorTable createOperatorTable(SqlOperatorTable opTab0) {
			MockSqlOperatorTable opTab = new MockSqlOperatorTable(opTab0);
			MockSqlOperatorTable.addRamp(opTab);
			return opTab;
		}

		private static SqlParser.Config createParserConfig(Map<String, Object> options) {
			return SqlParser.configBuilder()
				.setQuoting((Quoting) options.get("quoting"))
				.setUnquotedCasing((Casing) options.get("unquotedCasing"))
				.setQuotedCasing((Casing) options.get("quotedCasing"))
				.setConformance((SqlConformance) options.get("conformance"))
				.setCaseSensitive((boolean) options.get("caseSensitive"))
				.setParserFactory((SqlParserImplFactory) options.get("parserFactory"))
				.build();
		}

		private static RelDataTypeFactory createTypeFactory(SqlConformance conformance) {
			RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
			if (conformance.shouldConvertRaggedUnionTypesToVarying()) {
				typeSystem = new DelegatingTypeSystem(typeSystem) {
					public boolean shouldConvertRaggedUnionTypesToVarying() {
						return true;
					}
				};
			}
			if (conformance.allowExtendedTrim()) {
				typeSystem = new DelegatingTypeSystem(typeSystem) {
					public boolean allowExtendedTrim() {
						return true;
					}
				};
			}
			return new JavaTypeFactoryImpl(typeSystem);
		}

		private static Map<String, Object> buildDefaultOptions() {
			final Map<String, Object> m = new HashMap<>();
			m.put("quoting", Quoting.BACK_TICK);
			m.put("quotedCasing", Casing.UNCHANGED);
			m.put("unquotedCasing", Casing.UNCHANGED);
			m.put("caseSensitive", true);
			m.put("enableTypeCoercion", false);
			m.put("conformance", SqlConformanceEnum.DEFAULT);
			m.put("operatorTable", SqlStdOperatorTable.instance());
			m.put("parserFactory", FlinkSqlParserImpl.FACTORY);
			return Collections.unmodifiableMap(m);
		}
	}
}
