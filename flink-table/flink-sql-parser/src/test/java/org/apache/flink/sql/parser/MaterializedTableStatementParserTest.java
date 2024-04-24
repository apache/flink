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

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.parser.SqlParserFixture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Sql parser test for materialized related syntax. * */
@Execution(CONCURRENT)
public class MaterializedTableStatementParserTest {

    @Test
    void testCreateMaterializedTable() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source";
        final String expected =
                "CREATE MATERIALIZED TABLE `TBL1`\n"
                        + "(\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`";
        sql(sql).ok(expected);

        final String sql2 =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, h, t m FROM source";
        final String expected2 =
                "CREATE MATERIALIZED TABLE `TBL1`\n"
                        + "(\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`";

        sql(sql2).ok(expected2);

        final String sql3 =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' DAY\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, h, t m FROM source";
        final String expected3 =
                "CREATE MATERIALIZED TABLE `TBL1`\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' DAY\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`";
        sql(sql3).ok(expected3);
    }

    @Test
    void testCreateMaterializedTableWithUnsupportedFreshnessInterval() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = ^123^\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails(
                        "CREATE MATERIALIZED TABLE only supports interval type FRESHNESS, please refer to the materialized table document.");

        final String sql2 =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "^AS^ SELECT a, b, h, t m FROM source";

        sql(sql2)
                .fails(
                        "Encountered \"AS\" at line 11, column 1.\n"
                                + "Was expecting:\n"
                                + "    \"FRESHNESS\" ...\n"
                                + "    ");
    }

    @Test
    void testCreateMaterializedTableWithoutAsQuery() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL^\n^";
        sql(sql).fails(
                        "Encountered \"<EOF>\" at line 12, column 20.\n"
                                + "Was expecting:\n"
                                + "    \"AS\" ...\n"
                                + "    ");
    }

    @Test
    void testCreateTemporaryMaterializedTable() {
        final String sql =
                "CREATE TEMPORARY ^MATERIALIZED^ TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails("CREATE TEMPORARY MATERIALIZED TABLE is not supported.");
    }

    @Test
    void testReplaceMaterializedTable() {
        final String sql =
                "CREATE OR REPLACE ^MATERIALIZED^ TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails("REPLACE MATERIALIZED TABLE is not supported.");
    }

    public SqlParserFixture fixture() {
        return SqlParserFixture.DEFAULT.withConfig(
                c -> c.withParserFactory(FlinkSqlParserImpl.FACTORY));
    }

    protected SqlParserFixture sql(String sql) {
        return this.fixture().sql(sql);
    }

    protected SqlParserFixture expr(String sql) {
        return this.sql(sql).expression(true);
    }
}
