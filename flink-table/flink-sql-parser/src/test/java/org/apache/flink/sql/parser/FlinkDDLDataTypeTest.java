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
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
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
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.Util;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for all the supported Flink DDL data types. */
class FlinkDDLDataTypeTest {
    private static final Fixture FIXTURE = new Fixture(TestFactory.INSTANCE.getTypeFactory());
    private static final String DDL_FORMAT =
            "create table t1 (\n" + "  f0 %s\n" + ") with (\n" + "  'k1' = 'v1'\n" + ")";

    static Stream<Arguments> testData() {
        return Stream.of(
                createArgumentsTestItem("CHAR", nullable(FIXTURE.char1Type), "CHAR"),
                createArgumentsTestItem("CHAR NOT NULL", FIXTURE.char1Type, "CHAR NOT NULL"),
                createArgumentsTestItem("CHAR   NOT \t\nNULL", FIXTURE.char1Type, "CHAR NOT NULL"),
                createArgumentsTestItem("char not null", FIXTURE.char1Type, "CHAR NOT NULL"),
                createArgumentsTestItem("CHAR NULL", nullable(FIXTURE.char1Type), "CHAR"),
                createArgumentsTestItem("CHAR(33)", nullable(FIXTURE.char33Type), "CHAR(33)"),
                createArgumentsTestItem("VARCHAR", nullable(FIXTURE.varcharType), "VARCHAR"),
                createArgumentsTestItem(
                        "VARCHAR(33)", nullable(FIXTURE.varchar33Type), "VARCHAR(33)"),
                createArgumentsTestItem(
                        "STRING",
                        nullable(FIXTURE.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE)),
                        "STRING"),
                createArgumentsTestItem("BOOLEAN", nullable(FIXTURE.booleanType), "BOOLEAN"),
                createArgumentsTestItem("BINARY", nullable(FIXTURE.binaryType), "BINARY"),
                createArgumentsTestItem("BINARY(33)", nullable(FIXTURE.binary33Type), "BINARY(33)"),
                createArgumentsTestItem("VARBINARY", nullable(FIXTURE.varbinaryType), "VARBINARY"),
                createArgumentsTestItem(
                        "VARBINARY(33)", nullable(FIXTURE.varbinary33Type), "VARBINARY(33)"),
                createArgumentsTestItem(
                        "BYTES",
                        nullable(FIXTURE.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE)),
                        "BYTES"),
                createArgumentsTestItem("DECIMAL", nullable(FIXTURE.decimalType), "DECIMAL"),
                createArgumentsTestItem("DEC", nullable(FIXTURE.decimalType), "DECIMAL"),
                createArgumentsTestItem("NUMERIC", nullable(FIXTURE.decimalType), "DECIMAL"),
                createArgumentsTestItem(
                        "DECIMAL(10)", nullable(FIXTURE.decimalP10S0Type), "DECIMAL(10)"),
                createArgumentsTestItem(
                        "DEC(10)", nullable(FIXTURE.decimalP10S0Type), "DECIMAL(10)"),
                createArgumentsTestItem(
                        "NUMERIC(10)", nullable(FIXTURE.decimalP10S0Type), "DECIMAL(10)"),
                createArgumentsTestItem(
                        "DECIMAL(10, 3)", nullable(FIXTURE.decimalP10S3Type), "DECIMAL(10, 3)"),
                createArgumentsTestItem(
                        "DEC(10, 3)", nullable(FIXTURE.decimalP10S3Type), "DECIMAL(10, 3)"),
                createArgumentsTestItem(
                        "NUMERIC(10, 3)", nullable(FIXTURE.decimalP10S3Type), "DECIMAL(10, 3)"),
                createArgumentsTestItem("TINYINT", nullable(FIXTURE.tinyintType), "TINYINT"),
                createArgumentsTestItem("SMALLINT", nullable(FIXTURE.smallintType), "SMALLINT"),
                createArgumentsTestItem("INTEGER", nullable(FIXTURE.intType), "INTEGER"),
                createArgumentsTestItem("INT", nullable(FIXTURE.intType), "INTEGER"),
                createArgumentsTestItem("BIGINT", nullable(FIXTURE.bigintType), "BIGINT"),
                createArgumentsTestItem("FLOAT", nullable(FIXTURE.floatType), "FLOAT"),
                createArgumentsTestItem("DOUBLE", nullable(FIXTURE.doubleType), "DOUBLE"),
                createArgumentsTestItem("DOUBLE PRECISION", nullable(FIXTURE.doubleType), "DOUBLE"),
                createArgumentsTestItem("DATE", nullable(FIXTURE.dateType), "DATE"),
                createArgumentsTestItem("TIME", nullable(FIXTURE.timeType), "TIME"),
                createArgumentsTestItem(
                        "TIME WITHOUT TIME ZONE", nullable(FIXTURE.timeType), "TIME"),
                createArgumentsTestItem("TIME(3)", nullable(FIXTURE.time3Type), "TIME(3)"),
                createArgumentsTestItem(
                        "TIME(3) WITHOUT TIME ZONE", nullable(FIXTURE.time3Type), "TIME(3)"),
                createArgumentsTestItem("TIMESTAMP", nullable(FIXTURE.timestampType), "TIMESTAMP"),
                createArgumentsTestItem(
                        "TIMESTAMP WITHOUT TIME ZONE",
                        nullable(FIXTURE.timestampType),
                        "TIMESTAMP"),
                createArgumentsTestItem(
                        "TIMESTAMP(3)", nullable(FIXTURE.timestamp3Type), "TIMESTAMP(3)"),
                createArgumentsTestItem(
                        "TIMESTAMP(3) WITHOUT TIME ZONE",
                        nullable(FIXTURE.timestamp3Type),
                        "TIMESTAMP(3)"),
                createArgumentsTestItem(
                        "TIMESTAMP WITH LOCAL TIME ZONE",
                        nullable(FIXTURE.timestampWithLocalTimeZoneType),
                        "TIMESTAMP WITH LOCAL TIME ZONE"),
                createArgumentsTestItem(
                        "TIMESTAMP_LTZ",
                        nullable(FIXTURE.timestampWithLocalTimeZoneType),
                        "TIMESTAMP_LTZ"),
                createArgumentsTestItem(
                        "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                        nullable(FIXTURE.timestamp3WithLocalTimeZoneType),
                        "TIMESTAMP(3) WITH LOCAL TIME ZONE"),
                createArgumentsTestItem(
                        "TIMESTAMP_LTZ(3)",
                        nullable(FIXTURE.timestamp3WithLocalTimeZoneType),
                        "TIMESTAMP_LTZ(3)"),
                createArgumentsTestItem(
                        "ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>",
                        nullable(
                                FIXTURE.createArrayType(
                                        nullable(FIXTURE.timestamp3WithLocalTimeZoneType))),
                        "ARRAY< TIMESTAMP(3) WITH LOCAL TIME ZONE >"),
                createArgumentsTestItem(
                        "ARRAY<TIMESTAMP_LTZ(3)>",
                        nullable(
                                FIXTURE.createArrayType(
                                        nullable(FIXTURE.timestamp3WithLocalTimeZoneType))),
                        "ARRAY< TIMESTAMP_LTZ(3) >"),
                createArgumentsTestItem(
                        "ARRAY<INT NOT NULL>",
                        nullable(FIXTURE.createArrayType(FIXTURE.intType)),
                        "ARRAY< INTEGER NOT NULL >"),
                createArgumentsTestItem(
                        "INT ARRAY",
                        nullable(FIXTURE.createArrayType(nullable(FIXTURE.intType))),
                        "INTEGER ARRAY"),
                createArgumentsTestItem(
                        "INT NOT NULL ARRAY",
                        nullable(FIXTURE.createArrayType(FIXTURE.intType)),
                        "INTEGER NOT NULL ARRAY"),
                createArgumentsTestItem(
                        "INT ARRAY NOT NULL",
                        FIXTURE.createArrayType(nullable(FIXTURE.intType)),
                        "INTEGER ARRAY NOT NULL"),
                createArgumentsTestItem(
                        "MULTISET<INT NOT NULL>",
                        nullable(FIXTURE.createMultisetType(FIXTURE.intType)),
                        "MULTISET< INTEGER NOT NULL >"),
                createArgumentsTestItem(
                        "INT MULTISET",
                        nullable(FIXTURE.createMultisetType(nullable(FIXTURE.intType))),
                        "INTEGER MULTISET"),
                createArgumentsTestItem(
                        "INT NOT NULL MULTISET",
                        nullable(FIXTURE.createMultisetType(FIXTURE.intType)),
                        "INTEGER NOT NULL MULTISET"),
                createArgumentsTestItem(
                        "INT MULTISET NOT NULL",
                        FIXTURE.createMultisetType(nullable(FIXTURE.intType)),
                        "INTEGER MULTISET NOT NULL"),
                createArgumentsTestItem(
                        "MAP<BIGINT, BOOLEAN>",
                        nullable(
                                FIXTURE.createMapType(
                                        nullable(FIXTURE.bigintType),
                                        nullable(FIXTURE.booleanType))),
                        "MAP< BIGINT, BOOLEAN >"),
                createArgumentsTestItem(
                        "ROW<f0 INT NOT NULL, f1 BOOLEAN>",
                        nullable(
                                FIXTURE.createStructType(
                                        Arrays.asList(
                                                FIXTURE.intType, nullable(FIXTURE.booleanType)),
                                        Arrays.asList("f0", "f1"))),
                        "ROW< `f0` INTEGER NOT NULL, `f1` BOOLEAN >"),
                createArgumentsTestItem(
                        "ROW(f0 INT NOT NULL, f1 BOOLEAN)",
                        nullable(
                                FIXTURE.createStructType(
                                        Arrays.asList(
                                                FIXTURE.intType, nullable(FIXTURE.booleanType)),
                                        Arrays.asList("f0", "f1"))),
                        "ROW(`f0` INTEGER NOT NULL, `f1` BOOLEAN)"),
                createArgumentsTestItem(
                        "ROW<`f0` INT>",
                        nullable(
                                FIXTURE.createStructType(
                                        Collections.singletonList(nullable(FIXTURE.intType)),
                                        Collections.singletonList("f0"))),
                        "ROW< `f0` INTEGER >"),
                createArgumentsTestItem(
                        "ROW(`f0` INT)",
                        nullable(
                                FIXTURE.createStructType(
                                        Collections.singletonList(nullable(FIXTURE.intType)),
                                        Collections.singletonList("f0"))),
                        "ROW(`f0` INTEGER)"),
                createArgumentsTestItem(
                        "ROW<>",
                        nullable(
                                FIXTURE.createStructType(
                                        Collections.emptyList(), Collections.emptyList())),
                        "ROW<>"),
                createArgumentsTestItem(
                        "ROW()",
                        nullable(
                                FIXTURE.createStructType(
                                        Collections.emptyList(), Collections.emptyList())),
                        "ROW()"),
                createArgumentsTestItem(
                        "ROW<f0 INT NOT NULL 'This is a comment.', "
                                + "f1 BOOLEAN 'This as well.'>",
                        nullable(
                                FIXTURE.createStructType(
                                        Arrays.asList(
                                                FIXTURE.intType, nullable(FIXTURE.booleanType)),
                                        Arrays.asList("f0", "f1"))),
                        "ROW< `f0` INTEGER NOT NULL 'This is a comment.', "
                                + "`f1` BOOLEAN 'This as well.' >"),
                createArgumentsTestItem(
                        "RAW(    '"
                                + Fixture.RAW_TYPE_INT_CLASS
                                + "'   ,   '"
                                + Fixture.RAW_TYPE_INT_SERIALIZER_STRING
                                + "'   )",
                        nullable(FIXTURE.rawTypeOfInteger),
                        "RAW('"
                                + Fixture.RAW_TYPE_INT_CLASS
                                + "', '"
                                + Fixture.RAW_TYPE_INT_SERIALIZER_STRING
                                + "')"),
                createArgumentsTestItem(
                        "RAW('"
                                + Fixture.RAW_TYPE_INT_CLASS
                                + "', '"
                                + Fixture.RAW_TYPE_INT_SERIALIZER_STRING
                                + "') NOT NULL",
                        FIXTURE.rawTypeOfInteger,
                        "RAW('"
                                + Fixture.RAW_TYPE_INT_CLASS
                                + "', '"
                                + Fixture.RAW_TYPE_INT_SERIALIZER_STRING
                                + "') NOT NULL"),
                createArgumentsTestItem(
                        "RAW('"
                                + Fixture.RAW_TYPE_INT_CLASS
                                + "', '"
                                + Fixture.RAW_TYPE_INT_SERIALIZER_STRING.substring(0, 1)
                                + "' '"
                                + // test literal chain to split long string
                                Fixture.RAW_TYPE_INT_SERIALIZER_STRING.substring(1)
                                + "') NOT NULL",
                        FIXTURE.rawTypeOfInteger),

                // Test parse throws error.
                createArgumentsTestItem(
                        "TIMESTAMP WITH ^TIME^ ZONE", "(?s).*Encountered \"TIME\" at .*"),
                createArgumentsTestItem(
                        "TIMESTAMP(3) WITH ^TIME^ ZONE", "(?s).*Encountered \"TIME\" at .*"),
                createArgumentsTestItem(
                        "^NULL^",
                        "(?s).*Incorrect syntax near the keyword 'NULL' at line 2, column 6..*"),
                createArgumentsTestItem("cat.db.MyType", null, "`cat`.`db`.`MyType`"),
                createArgumentsTestItem("`db`.`MyType`", null, "`db`.`MyType`"),
                createArgumentsTestItem("MyType", null, "`MyType`"),
                createArgumentsTestItem("ARRAY<MyType>", null, "ARRAY< `MyType` >"),
                createArgumentsTestItem(
                        "ROW<f0 MyType, f1 `c`.`d`.`t`>",
                        null,
                        "ROW< `f0` `MyType`, `f1` `c`.`d`.`t` >"),
                createArgumentsTestItem(
                        "^INTERVAL^ YEAR",
                        "(?s).*Incorrect syntax near the keyword 'INTERVAL' at line 2, column 6..*"),
                createArgumentsTestItem(
                        "RAW(^)^",
                        "(?s).*Encountered \"\\)\" at line 2, column 10.\n.*"
                                + "Was expecting one of:\n"
                                + "    <BINARY_STRING_LITERAL> \\.\\.\\.\n"
                                + "    <QUOTED_STRING> \\.\\.\\.\n"
                                + "    <PREFIXED_STRING_LITERAL> \\.\\.\\.\n"
                                + "    <UNICODE_STRING_LITERAL> \\.\\.\\.\n"
                                + ".*"),
                createArgumentsTestItem(
                        "RAW('java.lang.Integer', ^)^",
                        "(?s).*Encountered \"\\)\" at line 2, column 31\\.\n"
                                + "Was expecting one of:\n"
                                + "    <BINARY_STRING_LITERAL> \\.\\.\\.\n"
                                + "    <QUOTED_STRING> \\.\\.\\.\n"
                                + "    <PREFIXED_STRING_LITERAL> \\.\\.\\.\n"
                                + "    <UNICODE_STRING_LITERAL> \\.\\.\\.\n"
                                + ".*"));
    }

    private static Arguments createArgumentsTestItem(Object... args) {
        assertThat(args.length).isGreaterThanOrEqualTo(2);
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
        return of(testItem);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testDataTypeParsing(TestItem testItem) {
        if (testItem.expectedType != null) {
            checkType(testItem.testExpr, testItem.expectedType);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testThrowsError(TestItem testItem) {
        if (testItem.expectedError != null) {
            checkFails(testItem.testExpr, testItem.expectedError);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testDataTypeUnparsing(TestItem testItem) {
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

    // ~ Inner Classes ----------------------------------------------------------

    private static class TestItem {
        private final String testExpr;
        @Nullable private RelDataType expectedType;
        @Nullable private String expectedError;
        @Nullable private String expectedUnparsed;

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

    /** Callback to control how test actions are performed. */
    protected interface Tester {
        void checkType(String sql, RelDataType type);

        void checkFails(String sql, String expectedMsgPattern);

        void checkUnparsed(String sql, String expectedUnparsed);
    }

    /** Default implementation of {@code SqlParserTest.Tester}. */
    protected class TesterImpl implements Tester {
        private TestFactory factory;

        TesterImpl(TestFactory factory) {
            this.factory = factory;
        }

        public void checkType(String sql, RelDataType type) {
            final SqlNode sqlNode = parseStmtAndHandleEx(sql);
            assertThat(sqlNode).isInstanceOf(SqlCreateTable.class);
            final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
            SqlNodeList columns = sqlCreateTable.getColumnList();
            assertThat(columns.size()).isEqualTo(1);
            RelDataType columnType =
                    ((SqlRegularColumn) columns.get(0))
                            .getType()
                            .deriveType(factory.getValidator());
            assertThat(columnType).isEqualTo(type);
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

        public void checkFails(String sql, String expectedMsgPattern) {
            StringAndPos sap = StringAndPos.of(sql);
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
            assertThat(sqlNode).isInstanceOf(SqlCreateTable.class);
            final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
            SqlNodeList columns = sqlCreateTable.getColumnList();
            assertThat(columns.size()).isEqualTo(1);
            SqlDataTypeSpec dataTypeSpec = ((SqlRegularColumn) columns.get(0)).getType();
            SqlWriter sqlWriter = new SqlPrettyWriter(factory.createSqlDialect(), false);
            dataTypeSpec.unparse(sqlWriter, 0, 0);
            // SqlDataTypeSpec does not take care of the nullable attribute unparse,
            // So we unparse nullable attribute specifically, this unparsing logic should
            // keep sync with SqlTableColumn.
            if (dataTypeSpec.getNullable() != null && !dataTypeSpec.getNullable()) {
                sqlWriter.keyword("NOT NULL");
            }
            assertThat(sqlWriter.toSqlString().getSql()).isEqualTo(expectedUnparsed);
        }

        private void checkEx(String expectedMsgPattern, StringAndPos sap, Throwable thrown) {
            SqlTests.checkEx(thrown, expectedMsgPattern, sap, SqlTests.Stage.VALIDATE);
        }
    }

    /** Factory to supply test components. */
    private static class TestFactory {
        static final Map<String, Object> DEFAULT_OPTIONS = buildDefaultOptions();

        public static final TestFactory INSTANCE = new TestFactory();

        private final Map<String, Object> options;
        private final SqlTestFactory.ValidatorFactory validatorFactory;

        private final TestRelDataTypeFactory typeFactory;
        private final SqlOperatorTable operatorTable;
        private final SqlValidatorCatalogReader catalogReader;
        private final SqlParser.Config parserConfig;

        TestFactory() {
            this(DEFAULT_OPTIONS, MockCatalogReaderSimple::create, SqlValidatorUtil::newValidator);
        }

        TestFactory(
                Map<String, Object> options,
                SqlTestFactory.CatalogReaderFactory catalogReaderFactory,
                SqlTestFactory.ValidatorFactory validatorFactory) {
            this.options = options;
            this.validatorFactory = validatorFactory;
            this.operatorTable =
                    createOperatorTable((SqlOperatorTable) options.get("operatorTable"));
            this.typeFactory = createTypeFactory((SqlConformance) options.get("conformance"));
            Boolean caseSensitive = (Boolean) options.get("caseSensitive");
            this.catalogReader = catalogReaderFactory.create(typeFactory, caseSensitive);
            this.parserConfig = createParserConfig(options);
        }

        public SqlParser createParser(String sql) {
            return SqlParser.create(new SourceStringReader(sql), parserConfig);
        }

        public SqlDialect createSqlDialect() {
            return new CalciteSqlDialect(
                    SqlDialect.EMPTY_CONTEXT
                            .withQuotedCasing(parserConfig.unquotedCasing())
                            .withConformance(parserConfig.conformance())
                            .withUnquotedCasing(parserConfig.unquotedCasing())
                            .withIdentifierQuoteString(parserConfig.quoting().string));
        }

        public TestRelDataTypeFactory getTypeFactory() {
            return this.typeFactory;
        }

        public SqlValidator getValidator() {
            final SqlConformance conformance = (SqlConformance) options.get("conformance");
            final boolean enableTypeCoercion = (boolean) options.get("enableTypeCoercion");
            return validatorFactory.create(
                    operatorTable,
                    catalogReader,
                    typeFactory,
                    SqlValidator.Config.DEFAULT
                            .withConformance(conformance)
                            .withTypeCoercionEnabled(enableTypeCoercion));
        }

        private static SqlOperatorTable createOperatorTable(SqlOperatorTable opTab0) {
            MockSqlOperatorTable opTab = MockSqlOperatorTable.of(opTab0);
            opTab.extend();
            return opTab;
        }

        private static SqlParser.Config createParserConfig(Map<String, Object> options) {
            return SqlParser.config()
                    .withQuoting((Quoting) options.get("quoting"))
                    .withUnquotedCasing((Casing) options.get("unquotedCasing"))
                    .withQuotedCasing((Casing) options.get("quotedCasing"))
                    .withConformance((SqlConformance) options.get("conformance"))
                    .withCaseSensitive((boolean) options.get("caseSensitive"))
                    .withParserFactory((SqlParserImplFactory) options.get("parserFactory"));
        }

        private static TestRelDataTypeFactory createTypeFactory(SqlConformance conformance) {
            RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
            if (conformance.shouldConvertRaggedUnionTypesToVarying()) {
                typeSystem =
                        new DelegatingTypeSystem(typeSystem) {
                            public boolean shouldConvertRaggedUnionTypesToVarying() {
                                return true;
                            }
                        };
            }
            if (conformance.allowExtendedTrim()) {
                typeSystem =
                        new DelegatingTypeSystem(typeSystem) {
                            public boolean allowExtendedTrim() {
                                return true;
                            }
                        };
            }
            return new TestRelDataTypeFactory(typeSystem);
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
