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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** EXECUTE, EXPLAIN and statement set parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserExecuteTest extends FlinkSqlParserTestBase {

    @Test
    void testBeginStatementSet() {
        sql("begin statement set").ok("BEGIN STATEMENT SET");
    }

    @Test
    void testEnd() {
        sql("end").ok("END");
    }

    @Test
    void testExecuteStatementSet() {
        sql("execute statement set begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "EXECUTE STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testExplainStatementSet() {
        sql("explain statement set begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "EXPLAIN STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testExplainExecuteStatementSet() {
        sql("explain execute statement set begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "EXPLAIN EXECUTE STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testExecuteStatementSetWithOnConflict() {
        sql("execute statement set begin "
                        + "insert into t1 select * from t2 on conflict do deduplicate; "
                        + "insert into t3 select * from t4 on conflict do nothing; "
                        + "end")
                .ok(
                        "EXECUTE STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + "ON CONFLICT DO DEDUPLICATE\n"
                                + ";\n"
                                + "INSERT INTO `T3`\n"
                                + "SELECT *\n"
                                + "FROM `T4`\n"
                                + "ON CONFLICT DO NOTHING\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testExplainExecuteSelect() {
        sql("explain execute select * from emps").ok("EXPLAIN EXECUTE SELECT *\nFROM `EMPS`");
    }

    @Test
    void testExplainExecuteInsert() {
        sql("explain execute insert into emps1 select * from emps2")
                .ok("EXPLAIN EXECUTE INSERT INTO `EMPS1`\nSELECT *\nFROM `EMPS2`");
    }

    @Test
    void testExplain() {
        String sql = "explain select * from emps";
        String expected = "EXPLAIN SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExecuteSelect() {
        String sql = "execute select * from emps";
        String expected = "EXECUTE SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainPlanFor() {
        String sql = "explain plan for select * from emps";
        String expected = "EXPLAIN SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainChangelogMode() {
        String sql = "explain changelog_mode select * from emps";
        String expected = "EXPLAIN CHANGELOG_MODE SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainEstimatedCost() {
        String sql = "explain estimated_cost select * from emps";
        String expected = "EXPLAIN ESTIMATED_COST SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainUnion() {
        String sql = "explain estimated_cost select * from emps union all select * from emps";
        String expected =
                "EXPLAIN ESTIMATED_COST SELECT *\n"
                        + "FROM `EMPS`\n"
                        + "UNION ALL\n"
                        + "SELECT *\n"
                        + "FROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainJsonFormat() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainWithImpl() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainWithoutImpl() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainWithType() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainAsXml() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testSqlOptions() {
        // SET/RESET are overridden for Flink SQL
    }

    @Test
    void testExplainAsJson() {
        String sql = "explain json_execution_plan select * from emps";
        String expected = "EXPLAIN JSON_EXECUTION_PLAN SELECT *\n" + "FROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainPlanAdvice() {
        String sql = "explain plan_advice select * from emps";
        String expected = "EXPLAIN PLAN_ADVICE SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainAllDetails() {
        String sql =
                "explain changelog_mode,json_execution_plan,estimated_cost,plan_advice select * from emps";
        String expected =
                "EXPLAIN JSON_EXECUTION_PLAN, CHANGELOG_MODE, PLAN_ADVICE, ESTIMATED_COST SELECT *\n"
                        + "FROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainInsert() {
        String expected = "EXPLAIN INSERT INTO `EMPS1`\n" + "SELECT *\n" + "FROM `EMPS2`";
        this.sql("explain plan for insert into emps1 select * from emps2").ok(expected);
    }

    @Test
    void testExecuteInsert() {
        String expected = "EXECUTE INSERT INTO `EMPS1`\n" + "SELECT *\n" + "FROM `EMPS2`";
        this.sql("execute insert into emps1 select * from emps2").ok(expected);
    }

    @Test
    void testExecutePlan() {
        sql("execute plan './test.json'").ok("EXECUTE PLAN './test.json'");
        sql("execute plan '/some/absolute/dir/plan.json'")
                .ok("EXECUTE PLAN '/some/absolute/dir/plan.json'");
        sql("execute plan 'file:///foo/bar/test.json'")
                .ok("EXECUTE PLAN 'file:///foo/bar/test.json'");
    }

    @Test
    void testCompilePlan() {
        sql("compile plan './test.json' for insert into t1 select * from t2")
                .ok(
                        "COMPILE PLAN './test.json' FOR INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`");
        sql("compile plan './test.json' if not exists for insert into t1 select * from t2")
                .ok(
                        "COMPILE PLAN './test.json' IF NOT EXISTS FOR INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`");
        sql("compile plan 'file:///foo/bar/test.json' if not exists for insert into t1 select * from t2")
                .ok(
                        "COMPILE PLAN 'file:///foo/bar/test.json' IF NOT EXISTS FOR INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`");

        sql("compile plan './test.json' for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE PLAN './test.json' FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");
        sql("compile plan './test.json' if not exists for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE PLAN './test.json' IF NOT EXISTS FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");

        sql("compile plan 'file:///foo/bar/test.json' if not exists for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE PLAN 'file:///foo/bar/test.json' IF NOT EXISTS FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testCompileAndExecutePlan() {
        sql("compile and execute plan './test.json' for insert into t1 select * from t2")
                .ok(
                        "COMPILE AND EXECUTE PLAN './test.json' FOR INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`");

        sql("compile and execute plan './test.json' for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE AND EXECUTE PLAN './test.json' FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "SELECT *\n"
                                + "FROM `T3`\n"
                                + ";\n"
                                + "END");
        sql("compile and execute plan 'file:///foo/bar/test.json' for insert into t1 select * from t2")
                .ok(
                        "COMPILE AND EXECUTE PLAN 'file:///foo/bar/test.json' FOR INSERT INTO `T1`\n"
                                + "SELECT *\n"
                                + "FROM `T2`");
    }

    @Test
    void testExplainUpsert() {
        String sql = "explain plan for upsert into emps1 values (1, 2)";
        String expected = "EXPLAIN UPSERT INTO `EMPS1`\n" + "VALUES (ROW(1, 2))";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainPlanForWithExplainDetails() {
        String sql = "explain plan for ^json_execution_plan^ upsert into emps1 values (1, 2)";
        this.sql(sql).fails("Non-query expression encountered in illegal context");
    }

    @Test
    void testExplainDuplicateExplainDetails() {
        String sql = "explain changelog_mode,^changelog_mode^ select * from emps";
        this.sql(sql).fails("Duplicate EXPLAIN DETAIL is not allowed.");
    }

    @Test
    void testAddJar() {
        sql("add Jar './test.sql'").ok("ADD JAR './test.sql'");
        sql("add JAR 'file:///path/to/\nwhatever'").ok("ADD JAR 'file:///path/to/\nwhatever'");
        sql("add JAR 'oss://path/helloworld.go'").ok("ADD JAR 'oss://path/helloworld.go'");
    }

    @Test
    void testRemoveJar() {
        sql("remove Jar './test.sql'").ok("REMOVE JAR './test.sql'");
        sql("remove JAR 'file:///path/to/\nwhatever'")
                .ok("REMOVE JAR 'file:///path/to/\nwhatever'");
        sql("remove JAR 'oss://path/helloworld.go'").ok("REMOVE JAR 'oss://path/helloworld.go'");
    }

    @Test
    void testShowJars() {
        sql("show jars").ok("SHOW JARS");
    }

    @Test
    void testSetReset() {
        sql("SET").same();
        sql("SET 'test-key' = 'test-value'").same();
        sql("RESET").same();
        sql("RESET 'test-key'").same();
    }

    @Test
    void testTryCast() {
        // Simple types
        expr("try_cast(a as timestamp)").ok("(TRY_CAST(`A` AS TIMESTAMP))");
        expr("try_cast('abc' as timestamp)").ok("(TRY_CAST('abc' AS TIMESTAMP))");

        // Complex types
        expr("try_cast(a as row(f0 int, f1 varchar))")
                .ok("(TRY_CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR)))");
        expr("try_cast(a as row(f0 int array, f1 map<string, decimal(10, 2)>, f2 STRING NOT NULL))")
                .ok(
                        "(TRY_CAST(`A` AS ROW(`F0` INTEGER ARRAY, `F1` MAP< STRING, DECIMAL(10, 2) >, `F2` STRING NOT NULL)))");
    }

    @Test
    void testAnalyzeTable() {
        sql("analyze table emp^s^").fails("(?s).*Encountered \"<EOF>\" at line 1, column 18.\n.*");
        sql("analyze table emps compute statistics").ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS");
        sql("analyze table emps partition ^compute^ statistics")
                .fails("(?s).*Encountered \"compute\" at line 1, column 30.\n.*");
        sql("analyze table emps partition(^)^ compute statistics")
                .fails("(?s).*Encountered \"\\)\" at line 1, column 30.\n.*");
        sql("analyze table emps partition(x='ab') compute statistics")
                .ok("ANALYZE TABLE `EMPS` PARTITION (`X` = 'ab') COMPUTE STATISTICS");
        sql("analyze table emps partition(x='ab', y='bc') compute statistics")
                .ok("ANALYZE TABLE `EMPS` PARTITION (`X` = 'ab', `Y` = 'bc') COMPUTE STATISTICS");
        sql("analyze table emps compute statistics for column^s^")
                .fails("(?s).*Encountered \"<EOF>\" at line 1, column 49.\n.*");
        sql("analyze table emps compute statistics for columns a")
                .ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS FOR COLUMNS `A`");
        sql("analyze table emps compute statistics for columns a, b")
                .ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS FOR COLUMNS `A`, `B`");
        sql("analyze table emps compute statistics for all columns")
                .ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x, y) compute statistics for all columns")
                .ok("ANALYZE TABLE `EMPS` PARTITION (`X`, `Y`) COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x='ab', y) compute statistics for all columns")
                .ok(
                        "ANALYZE TABLE `EMPS` PARTITION (`X` = 'ab', `Y`) COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x, y='cd') compute statistics for all columns")
                .ok(
                        "ANALYZE TABLE `EMPS` PARTITION (`X`, `Y` = 'cd') COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x=^,^ y) compute statistics for all columns")
                .fails("(?s).*Encountered \"\\,\" at line 1, column 32.\n.*");
    }

    @Test
    void testExplainCreateTableNoSupported() {
        this.sql("EXPLAIN CREATE TABLE t (id int^)^")
                .fails(
                        "Unsupported CREATE OR REPLACE statement for EXPLAIN\\. The statement must define a query using the AS clause \\(i\\.e\\. CTAS/RTAS statements\\)\\.");
    }
}
