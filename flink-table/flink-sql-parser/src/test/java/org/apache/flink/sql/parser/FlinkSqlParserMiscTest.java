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

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Miscellaneous expression and type parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserMiscTest extends FlinkSqlParserTestBase {

    @Test
    void testArrayAgg() {
        sql("select\n"
                        + "  array_agg(ename respect nulls order by deptno, ename) as c1,\n"
                        + "  array_agg(ename order by deptno, ename desc) as c2,\n"
                        + "  array_agg(distinct ename) as c3,\n"
                        + "  array_agg(ename) as c4\n"
                        + "from emp group by gender")
                .ok(
                        "SELECT"
                                + " ARRAY_AGG(`ENAME` ORDER BY `DEPTNO`, `ENAME`) RESPECT NULLS AS `C1`,"
                                + " ARRAY_AGG(`ENAME` ORDER BY `DEPTNO`, `ENAME` DESC) AS `C2`,"
                                + " ARRAY_AGG(DISTINCT `ENAME`) AS `C3`,"
                                + " ARRAY_AGG(`ENAME`) AS `C4`\n"
                                + "FROM `EMP`\n"
                                + "GROUP BY `GENDER`");
    }

    @Test
    void testCastAsMapType() {
        this.expr("cast(a as map<int, int>)").ok("CAST(`A` AS MAP< INTEGER, INTEGER >)");
        this.expr("cast(a as map<int, varchar array>)")
                .ok("CAST(`A` AS MAP< INTEGER, VARCHAR ARRAY >)");
        this.expr("cast(a as map<varchar multiset, map<int, int>>)")
                .ok("CAST(`A` AS MAP< VARCHAR MULTISET, MAP< INTEGER, INTEGER > >)");
    }

    // Override the test because our ROW field type default is nullable,
    // which is different with Calcite.
    @Test
    void testCastAsRowType() {
        final String expr = "cast(a as row(f0 int, f1 varchar))";
        final String expected = "CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))";
        expr(expr).ok(expected);

        final String expr1 = "cast(a as row(f0 int not null, f1 varchar null))";
        final String expected1 = "CAST(`A` AS ROW(`F0` INTEGER NOT NULL, `F1` VARCHAR))";
        expr(expr1).ok(expected1);

        final String expr2 =
                "cast(a as row(f0 row(ff0 int not null, ff1 varchar null) null,"
                        + " f1 timestamp not null))";
        final String expected2 =
                "CAST(`A` AS ROW(`F0` ROW(`FF0` INTEGER NOT NULL, `FF1` VARCHAR),"
                        + " `F1` TIMESTAMP NOT NULL))";
        expr(expr2).ok(expected2);

        final String expr3 = "cast(a as row(f0 bigint not null, f1 decimal null) array)";
        final String expected3 = "CAST(`A` AS ROW(`F0` BIGINT NOT NULL, `F1` DECIMAL) ARRAY)";
        expr(expr3).ok(expected3);

        final String expr4 = "cast(a as row(f0 varchar not null, f1 timestamp null) multiset)";
        final String expected4 = "CAST(`A` AS ROW(`F0` VARCHAR NOT NULL, `F1` TIMESTAMP) MULTISET)";
        expr(expr4).ok(expected4);
    }

    @Test
    void testOuterApplyFunctionFails() {
        final String sql = "select * from dept outer apply ramp(deptno)^)^";
        sql(sql).withConformance(SqlConformanceEnum.SQL_SERVER_2008)
                .fails("(?s).*Encountered \"\\)\" at .*");
    }

    @Test
    void testVariantType() {
        sql("CREATE TABLE t (\n" + "v variant" + "\n)")
                .ok("CREATE TABLE `T` (\n" + "  `V` VARIANT\n" + ")");

        sql("CREATE TABLE t (\n" + "v VARIANT NOT NULL" + "\n)")
                .ok("CREATE TABLE `T` (\n" + "  `V` VARIANT NOT NULL\n" + ")");
    }

    @Test
    void testBitmapType() {
        sql("CREATE TABLE t (\n" + "bm bitmap" + "\n)")
                .ok("CREATE TABLE `T` (\n" + "  `BM` BITMAP\n" + ")");

        sql("CREATE TABLE t (\n" + "bm bitmap NOT NULL" + "\n)")
                .ok("CREATE TABLE `T` (\n" + "  `BM` BITMAP NOT NULL\n" + ")");

        // BITMAP takes no parameters
        sql("CREATE TABLE t (\n" + "bm bitmap^(^1)" + "\n)")
                .fails("(?s).*Encountered \"\\(\" at line 2, column 10.\n.*");

        // BITMAP is a reserved keyword and cannot be used as an identifier without escaping
        sql("CREATE TABLE t (\n" + "^bitmap^ INT" + "\n)")
                .fails("(?s).*Encountered \"bitmap\" at line 2, column 1.\n.*");
    }
}
