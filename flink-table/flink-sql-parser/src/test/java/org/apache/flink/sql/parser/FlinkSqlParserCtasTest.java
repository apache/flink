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

/** CREATE TABLE AS SELECT and job parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserCtasTest extends FlinkSqlParserTestBase {

    @Test
    void testExplainCreateTableAsSelect() {
        this.sql("EXPLAIN CREATE TABLE t AS SELECT * FROM b")
                .ok("EXPLAIN CREATE TABLE `T`\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testExplainCreateOrReplaceTableAsSelect() {
        this.sql("EXPLAIN CREATE OR REPLACE TABLE t AS SELECT * FROM b")
                .ok("EXPLAIN CREATE OR REPLACE TABLE `T`\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testExplainReplaceTableAsSelect() {
        this.sql("EXPLAIN REPLACE TABLE t AS SELECT * FROM b")
                .ok("EXPLAIN REPLACE TABLE `T`\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testCreateTableAsSelectWithoutOptions() {
        sql("CREATE TABLE t AS SELECT * FROM b").ok("CREATE TABLE `T`\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testCreateTableAsSelectWithOptions() {
        sql("CREATE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                .ok("CREATE TABLE `T`\nWITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testCreateTableAsSelectWithCreateTableLike() {
        sql("CREATE TABLE t (col1 string) WITH ('test' = 'zm') like b ^AS^ SELECT col1 FROM b")
                .fails("(?s).*Encountered \"AS\" at line 1, column 58.*");
    }

    @Test
    void testCreateTableAsSelectWithTmpTable() {
        sql("CREATE TEMPORARY TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE TABLE AS SELECT syntax does not support to create temporary table yet."));
    }

    @Test
    void testCreateTableAsSelectWithExplicitColumns() {
        sql("CREATE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());
    }

    @Test
    void testCreateTableAsSelectWithWatermark() {
        sql("CREATE TABLE t (watermark FOR col1 AS col1 - interval '3' second) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());
    }

    @Test
    void testCreateTableAsSelectWithConstraints() {
        sql("CREATE TABLE t (PRIMARY KEY (col1) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        sql("CREATE TABLE t (PRIMARY KEY (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls "
                                                + "if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        sql("CREATE TABLE t (PRIMARY KEY (col1), PRIMARY KEY (col2) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("Duplicate primary key definition"));

        sql("CREATE TABLE t (UNIQUE (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testCreateTableAsSelectWithDistribution() {
        sql("CREATE TABLE t DISTRIBUTED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());
    }

    @Test
    void testCreateTableAsSelectWithPartitionKey() {
        sql("CREATE TABLE t PARTITIONED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());
    }

    @Test
    void testCreateTableAsSelectWithColumnIdentifiers() {
        // test with only column identifiers
        sql("CREATE TABLE t (col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test mix of column identifiers and column with types is not allowed
        sql("CREATE TABLE t (col1, col2 ^int^) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .fails("(?s).*Encountered \"int\" at line 1, column 28.*");
    }

    @Test
    void testUnsupportedCreateTableStatementsWithColumnIdentifiers() {
        String expectedErrorMsg =
                "Columns identifiers without types in the schema are "
                        + "supported on CTAS/RTAS statements only.";

        sql("CREATE TABLE t ^(a, h^) WITH " + "('connector' = 'kafka', 'kafka.topic' = 'log.test')")
                .fails(expectedErrorMsg);

        sql("CREATE TABLE t ^(a, h^) WITH "
                        + "('connector' = 'kafka', 'kafka.topic' = 'log.test') "
                        + "LIKE parent_table")
                .fails(expectedErrorMsg);
    }

    @Test
    void testReplaceTableAsSelectWithColumnIdentifiers() {
        // test with only column identifiers
        sql("REPLACE TABLE t (col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test mix of column identifiers and column with types is not allowed
        sql("REPLACE TABLE t (col1, col2 ^int^) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .fails("(?s).*Encountered \"int\" at line 1, column 29.*");
    }

    @Test
    void testReplaceTableAsSelect() {
        // test replace table as select without options
        sql("REPLACE TABLE t AS SELECT * FROM b").ok("REPLACE TABLE `T`\nAS\nSELECT *\nFROM `B`");

        // test replace table as select with options
        sql("REPLACE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                .ok("REPLACE TABLE `T`\nWITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `B`");

        // test replace table as select with tmp table
        sql("REPLACE TEMPORARY TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "REPLACE TABLE AS SELECT syntax does not support temporary table yet."));

        // test replace table as select with explicit columns
        sql("REPLACE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test replace table as select with watermark
        sql("REPLACE TABLE t (watermark FOR ts AS ts - interval '3' second) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test replace table as select with constraints
        sql("REPLACE TABLE t (PRIMARY KEY (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls "
                                                + "if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        sql("REPLACE TABLE t (PRIMARY KEY (col1), PRIMARY KEY (col2) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("Duplicate primary key definition"));

        sql("REPLACE TABLE t (UNIQUE (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        // test replace table as select with partition key
        sql("REPLACE TABLE t PARTITIONED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test replace table as select with distribution
        sql("REPLACE TABLE t DISTRIBUTED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());
    }

    @Test
    void testCreateOrReplaceTableAsSelect() {
        // test create or replace table as select without options
        sql("CREATE OR REPLACE TABLE t AS SELECT * FROM b")
                .ok("CREATE OR REPLACE TABLE `T`\nAS\nSELECT *\nFROM `B`");

        // test create or replace table as select with options
        sql("CREATE OR REPLACE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                .ok(
                        "CREATE OR REPLACE TABLE `T`\nWITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `B`");

        // test create or replace table as select with create table like
        sql("CREATE OR REPLACE TABLE t (col1 string) WITH ('test' = 'zm') like b ^AS^ SELECT col1 FROM b")
                .fails("(?s).*Encountered \"AS\" at line 1, column 69.*");

        // test create or replace table as select with tmp table
        sql("CREATE OR REPLACE TEMPORARY TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support temporary table yet."));

        // test create or replace table as select with explicit columns
        sql("CREATE OR REPLACE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test create or replace table as select with watermark
        sql("CREATE OR REPLACE TABLE t (watermark FOR ts AS ts - interval '3' second) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        // test create or replace table as select with constraints
        sql("CREATE OR REPLACE TABLE t (PRIMARY KEY (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls "
                                                + "if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        sql("CREATE OR REPLACE TABLE t (PRIMARY KEY (col1), PRIMARY KEY (col2) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("Duplicate primary key definition"));

        sql("CREATE OR REPLACE TABLE t (UNIQUE (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        // test create or replace table as select with partition key
        sql("CREATE OR REPLACE TABLE t PARTITIONED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());

        sql("CREATE OR REPLACE TABLE t DISTRIBUTED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().ok());
    }

    @Test
    void testShowJobs() {
        sql("show jobs").ok("SHOW JOBS");
    }

    @Test
    void testStopJob() {
        sql("STOP JOB 'myjob'").same();
        sql("STOP JOB 'myjob' WITH SAVEPOINT").same();
        sql("STOP JOB 'myjob' WITH SAVEPOINT WITH DRAIN").same();
        sql("STOP JOB 'myjob' ^WITH DRAIN^")
                .fails("WITH DRAIN could only be used after WITH SAVEPOINT.");
        sql("STOP JOB 'myjob' ^WITH DRAIN^ WITH SAVEPOINT")
                .fails("WITH DRAIN could only be used after WITH SAVEPOINT.");
    }

    @Test
    void testDescribeJob() {
        sql("DESCRIBE JOB 'myjob'").same();
        sql("DESC JOB 'myjob'").ok("DESCRIBE JOB 'myjob'");
    }

    @Test
    void testTruncateTable() {
        sql("truncate table t1").ok("TRUNCATE TABLE `T1`");
    }

    @Test
    void testCreateTableAsWithUsingConnectionFails() {
        final String sql =
                "^CREATE^ TABLE t1\n"
                        + "USING CONNECTION cat1.db1.conn1\n"
                        + "WITH ('connector' = 'jdbc')\n"
                        + "AS SELECT 1 AS a";
        sql(sql).fails(
                        "(?s).*USING CONNECTION clause is not supported with "
                                + "CREATE TABLE AS SELECT or REPLACE TABLE AS SELECT statements\\..*");
    }

    @Test
    void testReplaceTableAsWithUsingConnectionFails() {
        final String sql =
                "REPLACE TABLE t1\n"
                        + "^USING^ CONNECTION cat1.db1.conn1\n"
                        + "WITH ('connector' = 'jdbc')\n"
                        + "AS SELECT 1 AS a";
        sql(sql).fails("(?s).*Encountered \"USING\" at line 2, column 1.\n.*");
    }

    @Test
    void testCreateOrReplaceTableAsWithUsingConnectionFails() {
        final String sql =
                "^CREATE^ OR REPLACE TABLE t1\n"
                        + "USING CONNECTION cat1.db1.conn1\n"
                        + "WITH ('connector' = 'jdbc')\n"
                        + "AS SELECT 1 AS a";
        sql(sql).fails(
                        "(?s).*USING CONNECTION clause is not supported with "
                                + "CREATE TABLE AS SELECT or REPLACE TABLE AS SELECT statements\\..*");
    }
}
