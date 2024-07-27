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

package org.apache.flink.table.api.batch.sql.validation;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.calcite.sql.SqlMatchRecognize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Validation test for {@link SqlMatchRecognize}. */
@ExtendWith(ParameterizedTestExtension.class)
class MatchRecognizeValidationTest extends TableTestBase {

    private static final String STREAM = "stream";
    private static final String BATCH = "batch";

    @Parameter private String mode;

    @Parameters(name = "mode = {0}")
    private static Collection<String> parameters() {
        return Arrays.asList(STREAM, BATCH);
    }

    private TableTestUtil util;
    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        util =
                STREAM.equals(mode)
                        ? streamTestUtil(TableConfig.getDefault())
                        : batchTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
        tEnv.executeSql(
                "CREATE TABLE Ticker (\n"
                        + "  `symbol` VARCHAR,\n"
                        + "  `price` INT,\n"
                        + "  `tax` INT,\n"
                        + "  `proctime` as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE MyTable (\n"
                        + "  a BIGINT,\n"
                        + "  b INT,\n"
                        + "  proctime as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true'\n"
                        + ")");
    }

    @AfterEach
    void after() {
        util.getTableEnv().executeSql("DROP TABLE Ticker");
        util.getTableEnv().executeSql("DROP TABLE MyTable");
    }

    /** Function 'MATCH_ROWTIME()' can only be used in MATCH_RECOGNIZE. */
    @TestTemplate
    void testMatchRowTimeInSelect() {
        String sql = "SELECT MATCH_ROWTIME() FROM MyTable";
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> util.verifyExplain(sql));
    }

    /** Function 'MATCH_PROCTIME()' can only be used in MATCH_RECOGNIZE. */
    @TestTemplate
    void testMatchProcTimeInSelect() {
        String sql = "SELECT MATCH_PROCTIME() FROM MyTable";
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> util.verifyExplain(sql));
    }

    @TestTemplate
    void testSortProcessingTimeDesc() {
        if (STREAM.equals(mode)) {
            String sqlQuery =
                    "SELECT *\n"
                            + "FROM Ticker\n"
                            + "MATCH_RECOGNIZE (\n"
                            + "  ORDER BY proctime DESC\n"
                            + "  MEASURES\n"
                            + "    A.symbol AS aSymbol\n"
                            + "  PATTERN (A B)\n"
                            + "  DEFINE\n"
                            + "    A AS A.symbol = 'a'\n"
                            + ") AS T";
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                    .withMessageContaining(
                            "Primary sort order of a streaming table must be ascending on time.");
        }
    }

    @TestTemplate
    void testSortProcessingTimeSecondaryField() {
        if (STREAM.equals(mode)) {
            String sqlQuery =
                    "SELECT *\n"
                            + "FROM Ticker\n"
                            + "MATCH_RECOGNIZE (\n"
                            + "  ORDER BY price, proctime\n"
                            + "  MEASURES\n"
                            + "    A.symbol AS aSymbol\n"
                            + "  PATTERN (A B)\n"
                            + "  DEFINE\n"
                            + "    A AS A.symbol = 'a'\n"
                            + ") AS T";
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                    .withMessageContaining(
                            "You must specify either rowtime or proctime for order by as the first one.");
        }
    }

    @TestTemplate
    void testSortNoOrder() {
        if (STREAM.equals(mode)) {
            String sqlQuery =
                    "SELECT *\n"
                            + "FROM Ticker\n"
                            + "MATCH_RECOGNIZE (\n"
                            + "  MEASURES\n"
                            + "    A.symbol AS aSymbol\n"
                            + "  PATTERN (A B)\n"
                            + "  DEFINE\n"
                            + "    A AS A.symbol = 'a'\n"
                            + ") AS T";
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                    .withMessageContaining(
                            "You must specify either rowtime or proctime for order by.");
        }
    }

    @TestTemplate
    void testUpdatesInUpstreamOperatorNotSupported() {
        if (STREAM.equals(mode)) {
            String sqlQuery =
                    "SELECT *\n"
                            + "FROM (SELECT DISTINCT * FROM Ticker)\n"
                            + "MATCH_RECOGNIZE (\n"
                            + "  ORDER BY proctime\n"
                            + "  MEASURES\n"
                            + "    A.symbol AS aSymbol\n"
                            + "   ONE ROW PER MATCH"
                            + "  PATTERN (A B)\n"
                            + "  DEFINE\n"
                            + "    A AS A.symbol = 'a'\n"
                            + ") AS T";
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                    .withMessageContaining(
                            "Match Recognize doesn't support consuming update changes which is produced by node GroupAggregate(");
        }
    }

    @TestTemplate
    void testAggregatesOnMultiplePatternVariablesNotSupported() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    SUM(A.price + B.tax) AS taxedPrice\n"
                        + "  PATTERN (A B)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining("SQL validation failed.");
    }

    @TestTemplate
    void testAggregatesOnMultiplePatternVariablesNotSupportedInUDAGs() {
        util.addTemporarySystemFunction("weightedAvg", new WeightedAvg());
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    weightedAvg(A.price, B.tax) AS weightedAvg\n"
                        + "  PATTERN (A B)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining("Aggregation must be applied to a single pattern variable");
    }

    @TestTemplate
    void testValidatingAmbiguousColumns() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  PARTITION BY symbol, price\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    A.symbol AS symbol,\n"
                        + "    A.price AS price\n"
                        + "  PATTERN (A)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining("Columns ambiguously defined: {symbol, price}");
    }

    // ***************************************************************************************
    // * Those validations are temporary. We should remove those tests once we support those *
    // * features.                                                                           *
    // ***************************************************************************************

    /** Python Function can not be used in MATCH_RECOGNIZE for now. */
    @TestTemplate
    void testMatchPythonFunction() {
        util.addTemporarySystemFunction("pyFunc", new PythonScalarFunction("pyFunc"));
        String sql =
                "SELECT T.aa as ta\n"
                        + "FROM MyTable\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    A.a as aa,\n"
                        + "    pyFunc(1,2) as bb\n"
                        + "  PATTERN (A B)\n"
                        + "  DEFINE\n"
                        + "    A AS a = 1,\n"
                        + "    B AS b = 'b'\n"
                        + ") AS T";
        assertThatExceptionOfType(TableException.class)
                .isThrownBy(() -> tEnv.executeSql(sql))
                .withMessageContaining(
                        "Python Function can not be used in MATCH_RECOGNIZE for now.");
    }

    @TestTemplate
    void testAllRowsPerMatch() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    A.symbol AS aSymbol\n"
                        + "  ALL ROWS PER MATCH\n"
                        + "  PATTERN (A B)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(TableException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining("All rows per match mode is not supported yet.");
    }

    @TestTemplate
    void testGreedyQuantifierAtTheEndIsNotSupported() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    A.symbol AS aSymbol\n"
                        + "  PATTERN (A B+)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(TableException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining(
                        "Greedy quantifiers are not allowed as the last element of a "
                                + "Pattern yet. Finish your pattern with either a simple variable or reluctant quantifier.");
    }

    @TestTemplate
    void testPatternsProducingEmptyMatchesAreNotSupported() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    A.symbol AS aSymbol\n"
                        + "  PATTERN (A*)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(TableException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining(
                        "Patterns that can produce empty matches are not supported. "
                                + "There must be at least one non-optional state.");
    }

    @TestTemplate
    void testDistinctAggregationsNotSupported() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM Ticker\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    COUNT(DISTINCT A.price) AS price\n"
                        + "  PATTERN (A B)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ") AS T";
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tEnv.executeSql(sqlQuery))
                .withMessageContaining("SQL validation failed.");
    }
}
