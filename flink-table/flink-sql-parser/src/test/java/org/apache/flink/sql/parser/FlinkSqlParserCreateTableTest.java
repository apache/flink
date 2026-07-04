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

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** CREATE TABLE, DROP TABLE and INSERT parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserCreateTableTest extends FlinkSqlParserTestBase {

    @Test
    void testCreateTable() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  meta STRING METADATA, \n"
                        + "  my_meta STRING METADATA FROM 'meta', \n"
                        + "  my_meta STRING METADATA FROM 'meta' VIRTUAL, \n"
                        + "  meta STRING METADATA VIRTUAL, \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  `META` STRING METADATA,\n"
                        + "  `MY_META` STRING METADATA FROM 'meta',\n"
                        + "  `MY_META` STRING METADATA FROM 'meta' VIRTUAL,\n"
                        + "  `META` STRING METADATA VIRTUAL,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithDistribution() {
        final String sql = buildDistributionInput("DISTRIBUTED BY HASH(a, h) INTO 6 BUCKETS");
        final String expected =
                buildDistributionOutput("DISTRIBUTED BY HASH(`A`, `H`) INTO 6 BUCKETS\n");
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithRangeDistribution() {
        final String sql = buildDistributionInput("DISTRIBUTED BY RANGE(a, h) INTO 6 BUCKETS\n");
        final String expected =
                buildDistributionOutput("DISTRIBUTED BY RANGE(`A`, `H`) INTO 6 BUCKETS\n");
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithRandomDistribution() {
        final String sql = buildDistributionInput("DISTRIBUTED BY ^RANDOM^(a, h) INTO 6 BUCKETS\n");
        sql(sql).fails("(?s).*Encountered \"RANDOM\" at line 7, column 16.*");
    }

    @Test
    void testCreateTableWithDistributionNoAlgorithm() {
        final String sql = buildDistributionInput("DISTRIBUTED BY (a, h) INTO 6 BUCKETS\n");
        final String expected =
                buildDistributionOutput("DISTRIBUTED BY (`A`, `H`) INTO 6 BUCKETS\n");
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithDistributionAlgorithmWithoutBuckets() {
        final String sql = buildDistributionInput("DISTRIBUTED BY RANGE(a, h)\n");
        final String expected = buildDistributionOutput("DISTRIBUTED BY RANGE(`A`, `H`)\n");
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithDistributionNoAlgorithmWithoutBuckets() {
        final String sql = buildDistributionInput("DISTRIBUTED BY (a, h)\n");
        final String expected = buildDistributionOutput("DISTRIBUTED BY (`A`, `H`)\n");
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithDistributionIntoBuckets() {
        final String sql = buildDistributionInput("DISTRIBUTED INTO 3 BUCKETS\n");
        final String expected = buildDistributionOutput("DISTRIBUTED INTO 3 BUCKETS\n");
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithDistributionIntoNegativeBuckets() {
        final String sql = buildDistributionInput("DISTRIBUTED INTO ^-^3 BUCKETS\n");
        sql(sql).fails("(?s).*Encountered \"-\" at line 7, column 18.*");
    }

    @Test
    void testCreateTableWithDistributionIntoDecimalBuckets() {
        final String sql = buildDistributionInput("DISTRIBUTED INTO ^3.2^ BUCKETS\n");
        sql(sql).fails("(?s).*Bucket count must be a positive integer.*");
    }

    @Test
    void testCreateTableWithBadDistribution() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  meta STRING METADATA, \n"
                        + "  my_meta STRING METADATA FROM 'meta', \n"
                        + "  my_meta STRING METADATA FROM 'meta' VIRTUAL, \n"
                        + "  meta STRING METADATA VIRTUAL, \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "DISTRIBUTED \n"
                        + "  ^with^ (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).fails("(?s).*Encountered \"with\" at line 15, column 3.*");
    }

    @Test
    void testCreateTableWithDistributionIfNotExists() {
        final String sql =
                "CREATE TABLE if not exists tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(a, h) INTO 6 BUCKETS"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE IF NOT EXISTS `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(`A`, `H`) INTO 6 BUCKETS\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableIfNotExists() {
        final String sql =
                "CREATE TABLE IF NOT EXISTS tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE IF NOT EXISTS `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithComment() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint comment 'test column comment AAA.',\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  meta STRING METADATA COMMENT 'c1', \n"
                        + "  my_meta STRING METADATA FROM 'meta' COMMENT 'c2', \n"
                        + "  my_meta STRING METADATA FROM 'meta' VIRTUAL COMMENT 'c3', \n"
                        + "  meta STRING METADATA VIRTUAL COMMENT 'c4', \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "comment 'test table comment ABC.'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT COMMENT 'test column comment AAA.',\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  `META` STRING METADATA COMMENT 'c1',\n"
                        + "  `MY_META` STRING METADATA FROM 'meta' COMMENT 'c2',\n"
                        + "  `MY_META` STRING METADATA FROM 'meta' VIRTUAL COMMENT 'c3',\n"
                        + "  `META` STRING METADATA VIRTUAL COMMENT 'c4',\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'test table comment ABC.'\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithCommentOnComputedColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint comment 'test column comment AAA.',\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1) comment 'test computed column.', \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "comment 'test table comment ABC.'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT COMMENT 'test column comment AAA.',\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)) COMMENT 'test computed column.',\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'test table comment ABC.'\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testTableConstraints() {
        final String sql1 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b),\n"
                        + "  UNIQUE (h, g)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected1 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`),\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql1)
                .ok(expected1)
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                                + "ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        final String sql2 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED,\n"
                        + "  UNIQUE (h, g)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected2 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED,\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql2)
                .ok(expected2)
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        final String sql3 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expectParsed =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        final String expectValidated =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT NOT NULL,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR NOT NULL,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql3).ok(expectParsed).node(validated(expectValidated));
    }

    @Test
    void testColumnConstraints() {
        final String sql1 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key,\n"
                        + "  h varchar unique,\n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected1 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY,\n"
                        + "  `H` VARCHAR UNIQUE,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql1)
                .ok(expected1)
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                                + "ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        final String sql2 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key not enforced,\n"
                        + "  h varchar unique,\n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected2 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY NOT ENFORCED,\n"
                        + "  `H` VARCHAR UNIQUE,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql2)
                .ok(expected2)
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        final String sql3 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key not enforced,\n"
                        + "  h varchar,\n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expectParsed =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY NOT ENFORCED,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        final String expectValidated =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql3).ok(expectParsed).node(validated(expectValidated));
    }

    @Test
    void testUniqueTableConstraint() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED,\n"
                        + "  UNIQUE (h, g)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED,\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
        sql(sql).node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testTableConstraintsWithEnforcement() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key enforced comment 'test column comment AAA.',\n"
                        + "  h varchar constraint ct1 unique not enforced,\n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar constraint ct2 unique,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  unique (g, ts) not enforced"
                        + ") with (\n"
                        + "    'connector' = 'kafka',\n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY ENFORCED COMMENT 'test column comment AAA.',\n"
                        + "  `H` VARCHAR CONSTRAINT `CT1` UNIQUE NOT ENFORCED,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR CONSTRAINT `CT2` UNIQUE,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  UNIQUE (`G`, `TS`) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testDuplicatePk() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint comment 'test column comment AAA.',\n"
                        + "  h varchar constraint ct1 primary key,\n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  constraint ct2 primary key (b, h)"
                        + ") with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).node(new ValidationMatcher().fails("Duplicate primary key definition"));
    }

    @Test
    void testCreateTableWithWatermark() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  ts timestamp(3),\n"
                        + "  id varchar, \n"
                        + "  watermark FOR ts AS ts - interval '3' second\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `TS` TIMESTAMP(3),\n"
                        + "  `ID` VARCHAR,\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithWatermarkOnComputedColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  log_ts varchar,\n"
                        + "  ts as to_timestamp(log_ts), \n"
                        + "  WATERMARK FOR ts AS ts + interval '1' second\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `LOG_TS` VARCHAR,\n"
                        + "  `TS` AS `TO_TIMESTAMP`(`LOG_TS`),\n"
                        + "  WATERMARK FOR `TS` AS (`TS` + INTERVAL '1' SECOND)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithWatermarkOnNestedField() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  f1 row<q1 bigint, q2 row<t1 timestamp, t2 varchar>, q3 boolean>,\n"
                        + "  WATERMARK FOR f1.q2.t1 AS NOW()\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `F1` ROW< `Q1` BIGINT, `Q2` ROW< `T1` TIMESTAMP, `T2` VARCHAR >, `Q3` BOOLEAN >,\n"
                        + "  WATERMARK FOR `F1`.`Q2`.`T1` AS `NOW`()\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithMultipleWatermark() {
        String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  f0 bigint,\n"
                        + "  f1 varchar,\n"
                        + "  f2 boolean,\n"
                        + "  WATERMARK FOR f0 AS NOW(),\n"
                        + "  ^WATERMARK^ FOR f1 AS NOW()\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).fails("Multiple WATERMARK declarations are not supported yet.");
    }

    @Test
    void testCreateTableWithQueryWatermarkExpression() {
        String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  f0 bigint,\n"
                        + "  f1 varchar,\n"
                        + "  f2 boolean,\n"
                        + "  WATERMARK FOR f0 AS ^(^SELECT f1 FROM tbl1)\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).fails("Query expression encountered in illegal context");
    }

    @Test
    void testCreateTableWithComplexType() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a ARRAY<bigint>, \n"
                        + "  b MAP<int, varchar>,\n"
                        + "  c ROW<cc0 int, cc1 float, cc2 varchar>,\n"
                        + "  d MULTISET<varchar>,\n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` ARRAY< BIGINT >,\n"
                        + "  `B` MAP< INTEGER, VARCHAR >,\n"
                        + "  `C` ROW< `CC0` INTEGER, `CC1` FLOAT, `CC2` VARCHAR >,\n"
                        + "  `D` MULTISET< VARCHAR >,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'asd' = 'data'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithNestedComplexType() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a ARRAY<ARRAY<bigint>>, \n"
                        + "  b MAP<MAP<int, varchar>, ARRAY<varchar>>,\n"
                        + "  c ROW<cc0 ARRAY<int>, cc1 float, cc2 varchar>,\n"
                        + "  d MULTISET<ARRAY<int>>,\n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` ARRAY< ARRAY< BIGINT > >,\n"
                        + "  `B` MAP< MAP< INTEGER, VARCHAR >, ARRAY< VARCHAR > >,\n"
                        + "  `C` ROW< `CC0` ARRAY< INTEGER >, `CC1` FLOAT, `CC2` VARCHAR >,\n"
                        + "  `D` MULTISET< ARRAY< INTEGER > >,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'asd' = 'data'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithUserDefinedType() {
        final String sql =
                "create table t(\n"
                        + "  a catalog1.db1.MyType1,\n"
                        + "  b db2.MyType2\n"
                        + ") with (\n"
                        + "  'k1' = 'v1',\n"
                        + "  'k2' = 'v2'\n"
                        + ")";
        final String expected =
                "CREATE TABLE `T` (\n"
                        + "  `A` `CATALOG1`.`DB1`.`MYTYPE1`,\n"
                        + "  `B` `DB2`.`MYTYPE2`\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'k1' = 'v1',\n"
                        + "  'k2' = 'v2'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testInvalidComputedColumn() {
        final String sql0 =
                "CREATE TABLE t1 (\n"
                        + "  a bigint, \n"
                        + "  b varchar,\n"
                        + "  toTimestamp^(^b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expect0 =
                "(?s).*Encountered \"\\(\" at line 4, column 14.\n"
                        + "Was expecting one of:\n"
                        + "    \"AS\" ...\n"
                        + "    \".\" ...\n"
                        + "    \"STRING\" ...\n"
                        + ".*";
        sql(sql0).fails(expect0);
        // Sub-query computed column expression is forbidden.
        final String sql1 =
                "CREATE TABLE t1 (\n"
                        + "  a bigint, \n"
                        + "  b varchar,\n"
                        + "  c as ^(^select max(d) from t2), \n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expect1 = "(?s).*Query expression encountered in illegal context.*";
        sql(sql1).fails(expect1);
    }

    @Test
    void testColumnSqlString() {
        final String sql =
                "CREATE TABLE sls_stream (\n"
                        + "  a bigint, \n"
                        + "  f as a + 1, \n"
                        + "  b varchar,\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  proc as PROCTIME(),\n"
                        + "  c int,\n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expected =
                "`A`, (`A` + 1) AS `F`, `B`, "
                        + "`TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss') AS `TS`, "
                        + "`PROCTIME`() AS `PROC`, `C`";
        sql(sql).node(new ValidationMatcher().expectColumnSql(expected));
    }

    @Test
    void testCreateTableWithMinusInOptionKey() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") with (\n"
                        + "  'a-b-c-d124' = 'ab',\n"
                        + "  'a.b.1.c' = 'aabb',\n"
                        + "  'a.b-c-connector.e-f.g' = 'ada',\n"
                        + "  'a.b-c-d.e-1231.g' = 'ada',\n"
                        + "  'a.b-c-d.*' = 'adad')\n";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'a-b-c-d124' = 'ab',\n"
                        + "  'a.b.1.c' = 'aabb',\n"
                        + "  'a.b-c-connector.e-f.g' = 'ada',\n"
                        + "  'a.b-c-d.e-1231.g' = 'ada',\n"
                        + "  'a.b-c-d.*' = 'adad'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithOptionKeyAsIdentifier() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") with (\n"
                        + "  ^a^.b.c = 'ab',\n"
                        + "  a.b.c1 = 'aabb')\n";
        sql(sql).fails("(?s).*Encountered \"a\" at line 6, column 3.\n.*");
    }

    @Test
    void testCreateTableLikeWithoutOption() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "LIKE `PARENT_TABLE`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableLikeWithConstraints() {
        final String sql1 =
                "create table source_table(\n"
                        + "  a int primary key,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table";
        sql(sql1)
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED "
                                                + "controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        final String sql2 =
                "create table source_table(\n"
                        + "  a int primary key,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  primary key(a) not enforced\n"
                        + ")\n"
                        + "LIKE parent_table";

        sql(sql2).node(new ValidationMatcher().fails("Duplicate primary key definition"));

        final String sql3 =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  unique (a)\n"
                        + ")\n"
                        + "LIKE parent_table";

        sql(sql3).node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testCreateTableWithLikeClause() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table (\n"
                        + "   INCLUDING ALL\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   EXCLUDING PARTITIONS\n"
                        + "   INCLUDING GENERATED\n"
                        + "   INCLUDING METADATA\n"
                        + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "LIKE `PARENT_TABLE` (\n"
                        + "  INCLUDING ALL\n"
                        + "  OVERWRITING OPTIONS\n"
                        + "  EXCLUDING PARTITIONS\n"
                        + "  INCLUDING GENERATED\n"
                        + "  INCLUDING METADATA\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithLikeClauseIncludingDistribution() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table (\n"
                        + "   INCLUDING ALL\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   INCLUDING DISTRIBUTION\n"
                        + "   EXCLUDING PARTITIONS\n"
                        + "   INCLUDING GENERATED\n"
                        + "   INCLUDING METADATA\n"
                        + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "LIKE `PARENT_TABLE` (\n"
                        + "  INCLUDING ALL\n"
                        + "  OVERWRITING OPTIONS\n"
                        + "  INCLUDING DISTRIBUTION\n"
                        + "  EXCLUDING PARTITIONS\n"
                        + "  INCLUDING GENERATED\n"
                        + "  INCLUDING METADATA\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithLikeClauseExcludingDistribution() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table (\n"
                        + "   INCLUDING ALL\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   EXCLUDING DISTRIBUTION\n"
                        + "   INCLUDING PARTITIONS\n"
                        + "   INCLUDING GENERATED\n"
                        + "   INCLUDING METADATA\n"
                        + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "LIKE `PARENT_TABLE` (\n"
                        + "  INCLUDING ALL\n"
                        + "  OVERWRITING OPTIONS\n"
                        + "  EXCLUDING DISTRIBUTION\n"
                        + "  INCLUDING PARTITIONS\n"
                        + "  INCLUDING GENERATED\n"
                        + "  INCLUDING METADATA\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithLikeClauseWithoutColumns() {
        final String sql =
                ""
                        + "create TEMPORARY table source_table (\n"
                        + "   WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                        + ") with (\n"
                        + "  'scan.startup.mode' = 'specific-offsets',\n"
                        + "  'scan.startup.specific-offsets' = 'partition:0,offset:1169129'\n"
                        + ") like t_order_course (\n"
                        + "   OVERWRITING  WATERMARKS\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   EXCLUDING CONSTRAINTS\n"
                        + ")";
        final String expected =
                "CREATE TEMPORARY TABLE `SOURCE_TABLE` (\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '5' SECOND)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'scan.startup.mode' = 'specific-offsets',\n"
                        + "  'scan.startup.specific-offsets' = 'partition:0,offset:1169129'\n"
                        + ")\n"
                        + "LIKE `T_ORDER_COURSE` (\n"
                        + "  OVERWRITING WATERMARKS\n"
                        + "  OVERWRITING OPTIONS\n"
                        + "  EXCLUDING CONSTRAINTS\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTemporaryTable() {
        final String sql =
                "create temporary table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") with (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        final String expected =
                "CREATE TEMPORARY TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithNoColumns() {
        final String sql =
                "create table source_table with (\n" + "  'x' = 'y',\n" + "  'abc' = 'def'\n" + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE`\nWITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithOnlyWaterMark() {
        final String sql =
                "create table source_table (\n"
                        + "  watermark FOR ts AS ts - interval '3' second\n"
                        + ") with (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testDropTable() {
        final String sql = "DROP table catalog1.db1.tbl1";
        final String expected = "DROP TABLE `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropIfExists() {
        final String sql = "DROP table IF EXISTS catalog1.db1.tbl1";
        final String expected = "DROP TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testTemporaryDropTable() {
        final String sql = "DROP temporary table catalog1.db1.tbl1";
        final String expected = "DROP TEMPORARY TABLE `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropTemporaryIfExists() {
        final String sql = "DROP temporary table IF EXISTS catalog1.db1.tbl1";
        final String expected = "DROP TEMPORARY TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testInsertPartitionSpecs() {
        final String sql1 = "insert into emps partition (x='ab', y='bc') (x,y) select * from emps";
        final String expected =
                "INSERT INTO `EMPS` "
                        + "PARTITION (`X` = 'ab', `Y` = 'bc')\n"
                        + "(`X`, `Y`)\n"
                        + "SELECT *\n"
                        + "FROM `EMPS`";
        sql(sql1).ok(expected);
        final String sql2 =
                "insert into emp\n"
                        + "partition(empno='1', job='job')\n"
                        + "(empno, ename, job, mgr, hiredate,\n"
                        + "  sal, comm, deptno, slacker)\n"
                        + "select 'nom', 0, timestamp '1970-01-01 00:00:00',\n"
                        + "  1, 1, 1, false\n"
                        + "from (values 'a')";
        sql(sql2)
                .ok(
                        "INSERT INTO `EMP` "
                                + "PARTITION (`EMPNO` = '1', `JOB` = 'job')\n"
                                + "(`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,"
                                + " `COMM`, `DEPTNO`, `SLACKER`)\n"
                                + "SELECT 'nom', 0, TIMESTAMP '1970-01-01 00:00:00', 1, 1, 1, FALSE\n"
                                + "FROM (VALUES (ROW('a')))");
        final String sql3 =
                "insert into empnullables\n"
                        + "partition(ename='b')\n"
                        + "(empno, ename)\n"
                        + "select 1 from (values 'a')";
        sql(sql3)
                .ok(
                        "INSERT INTO `EMPNULLABLES` "
                                + "PARTITION (`ENAME` = 'b')\n"
                                + "(`EMPNO`, `ENAME`)\n"
                                + "SELECT 1\n"
                                + "FROM (VALUES (ROW('a')))");
    }

    @Test
    void testInsertCaseSensitivePartitionSpecs() {
        final String expected =
                "INSERT INTO `emps` "
                        + "PARTITION (`x` = 'ab', `y` = 'bc')\n"
                        + "(`x`, `y`)\n"
                        + "SELECT *\n"
                        + "FROM `EMPS`";
        sql("insert into \"emps\" "
                        + "partition (\"x\"='ab', \"y\"='bc')(\"x\",\"y\") select * from emps")
                .ok(expected);
    }

    @Test
    void testInsertExtendedColumnAsStaticPartition1() {
        final String expected =
                "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) "
                        + "PARTITION (`Z` = 'ab')\n"
                        + "(`X`, `Y`)\n"
                        + "SELECT *\n"
                        + "FROM `EMPS`";
        sql("insert into emps(z boolean) partition (z='ab') (x,y) select * from emps").ok(expected);
    }

    @Test
    void testInsertExtendedColumnAsStaticPartition2() {
        assertThatThrownBy(
                        () ->
                                sql("insert into emps(x, y, z boolean) partition (z='ab') select * from emps")
                                        .node(
                                                new ValidationMatcher()
                                                        .fails(
                                                                "Extended columns not allowed under the current SQL conformance level")))
                .isInstanceOf(SqlParseException.class);
    }

    @Test
    void testInsertOverwrite() {
        // non-partitioned
        final String sql = "INSERT OVERWRITE myDB.myTbl SELECT * FROM src";
        final String expected = "INSERT OVERWRITE `MYDB`.`MYTBL`\n" + "SELECT *\n" + "FROM `SRC`";
        sql(sql).ok(expected);

        // partitioned
        final String sql1 = "INSERT OVERWRITE myTbl PARTITION (p1='v1',p2='v2') SELECT * FROM src";
        final String expected1 =
                "INSERT OVERWRITE `MYTBL` "
                        + "PARTITION (`P1` = 'v1', `P2` = 'v2')\n"
                        + "\n"
                        + "SELECT *\n"
                        + "FROM `SRC`";
        sql(sql1).ok(expected1);
    }

    @Test
    void testInvalidUpsertOverwrite() {
        sql("UPSERT ^OVERWRITE^ myDB.myTbl SELECT * FROM src")
                .fails("OVERWRITE expression is only used with INSERT statement.");
    }

    @Test
    void testInsertOnConflict() {
        // ON CONFLICT DO ERROR
        sql("INSERT INTO t1 SELECT * FROM t2 ON CONFLICT DO ERROR")
                .ok("INSERT INTO `T1`\nSELECT *\nFROM `T2`\nON CONFLICT DO ERROR");

        // ON CONFLICT DO NOTHING
        sql("INSERT INTO t1 SELECT * FROM t2 ON CONFLICT DO NOTHING")
                .ok("INSERT INTO `T1`\nSELECT *\nFROM `T2`\nON CONFLICT DO NOTHING");

        // ON CONFLICT DO DEDUPLICATE
        sql("INSERT INTO t1 SELECT * FROM t2 ON CONFLICT DO DEDUPLICATE")
                .ok("INSERT INTO `T1`\nSELECT *\nFROM `T2`\nON CONFLICT DO DEDUPLICATE");

        // ON CONFLICT with partition
        sql("INSERT INTO t1 PARTITION (p='v') SELECT * FROM t2 ON CONFLICT DO ERROR")
                .ok(
                        "INSERT INTO `T1` PARTITION (`P` = 'v')\n\nSELECT *\nFROM `T2`\nON CONFLICT DO ERROR");

        // ON CONFLICT with INSERT OVERWRITE (should work)
        sql("INSERT OVERWRITE t1 SELECT * FROM t2 ON CONFLICT DO NOTHING")
                .ok("INSERT OVERWRITE `T1`\nSELECT *\nFROM `T2`\nON CONFLICT DO NOTHING");

        // Invalid ON CONFLICT strategy
        sql("INSERT INTO t1 SELECT * FROM t2 ON CONFLICT DO ^UPDATE^")
                .fails("(?s).*Encountered \"UPDATE\" at line 1, column 48.\n.*");
    }

    private String buildDistributionInput(final String distributionClause) {
        return "CREATE TABLE tbl1 (\n"
                + "  a bigint,\n"
                + "  h varchar, \n"
                + "  b varchar,\n"
                + "  PRIMARY KEY (a, b)\n"
                + ")\n"
                + distributionClause
                + "  with (\n"
                + "    'connector' = 'kafka', \n"
                + "    'kafka.topic' = 'log.test'\n"
                + ")\n";
    }

    @Test
    void testCreateTableUsingConnection() {
        final String sql =
                "CREATE TABLE orders (\n"
                        + "  order_id INT,\n"
                        + "  customer_id INT,\n"
                        + "  amount DECIMAL(10, 2)\n"
                        + ") USING CONNECTION mycat.mydb.mysql_prod\n"
                        + "WITH (\n"
                        + "  'connector' = 'jdbc',\n"
                        + "  'tables' = 'orders'\n"
                        + ")";
        final String expected =
                "CREATE TABLE `ORDERS` (\n"
                        + "  `ORDER_ID` INTEGER,\n"
                        + "  `CUSTOMER_ID` INTEGER,\n"
                        + "  `AMOUNT` DECIMAL(10, 2)\n"
                        + ")\n"
                        + "USING CONNECTION `MYCAT`.`MYDB`.`MYSQL_PROD`\n"
                        + "WITH (\n"
                        + "  'connector' = 'jdbc',\n"
                        + "  'tables' = 'orders'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableUsingConnectionWithPartitionAndDistribution() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar,\n"
                        + "  b varchar\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(a) INTO 3 BUCKETS\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "USING CONNECTION cat1.db1.conn1\n"
                        + "WITH (\n"
                        + "  'connector' = 'jdbc'\n"
                        + ")";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `B` VARCHAR\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(`A`) INTO 3 BUCKETS\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "USING CONNECTION `CAT1`.`DB1`.`CONN1`\n"
                        + "WITH (\n"
                        + "  'connector' = 'jdbc'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableLikeUsingConnection() {
        final String sql =
                "CREATE TABLE t1 (\n"
                        + "  a INT\n"
                        + ")\n"
                        + "USING CONNECTION cat1.db1.conn1\n"
                        + "WITH ('connector' = 'jdbc')\n"
                        + "LIKE base_table";
        final String expected =
                "CREATE TABLE `T1` (\n"
                        + "  `A` INTEGER\n"
                        + ")\n"
                        + "USING CONNECTION `CAT1`.`DB1`.`CONN1`\n"
                        + "WITH (\n"
                        + "  'connector' = 'jdbc'\n"
                        + ")\n"
                        + "LIKE `BASE_TABLE`";
        sql(sql).ok(expected);
    }

    private String buildDistributionOutput(final String distributionClause) {
        return "CREATE TABLE `TBL1` (\n"
                + "  `A` BIGINT,\n"
                + "  `H` VARCHAR,\n"
                + "  `B` VARCHAR,\n"
                + "  PRIMARY KEY (`A`, `B`)\n"
                + ")\n"
                + distributionClause
                + "WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'kafka.topic' = 'log.test'\n"
                + ")";
    }
}
