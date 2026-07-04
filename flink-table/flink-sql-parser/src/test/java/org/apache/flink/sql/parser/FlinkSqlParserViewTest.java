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

import org.apache.flink.sql.parser.ddl.view.SqlCreateView;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** VIEW, FUNCTION and MODULE parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserViewTest extends FlinkSqlParserTestBase {

    @Test
    void testCreateView() {
        final String sql = "create view v as select col1 from tbl";
        final String expected = "CREATE VIEW `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewWithInvalidFieldList() {
        final String expected =
                "(?s).*Encountered \"\\)\" at line 1, column 15.\n"
                        + "Was expecting one of:\n"
                        + ".*\n"
                        + ".*\n"
                        + ".*\n"
                        + ".*\n"
                        + ".*";
        sql("CREATE VIEW V(^)^ AS SELECT * FROM TBL").fails(expected);
    }

    @Test
    void testCreateViewWithComment() {
        final String sql = "create view v COMMENT 'this is a view' as select col1 from tbl";
        final String expected =
                "CREATE VIEW `V`\n"
                        + "COMMENT 'this is a view'\n"
                        + "AS\n"
                        + "SELECT `COL1`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewWithFieldNames() {
        final String sql = "create view v(col1, col2) as select col3, col4 from tbl";
        final String expected =
                "CREATE VIEW `V` (`COL1`, `COL2`)\n"
                        + "AS\n"
                        + "SELECT `COL3`, `COL4`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewWithFieldNamesWithToString() throws SqlParseException {
        final String sql = "create view v(col1, col2) as select col3, col4 from tbl";
        final String expected =
                "CREATE VIEW `V` (`COL1`, `COL2`)\n"
                        + "AS\n"
                        + "SELECT `COL3`, `COL4`\n"
                        + "FROM `TBL`";
        final SqlNode node = sql(sql).parser().parseQuery();
        assertThat(node).isInstanceOf(SqlCreateView.class).hasToString(expected);
    }

    @Test
    void testCreateViewWithInvalidName() {
        final String sql = "create view v(^*^) COMMENT 'this is a view' as select col1 from tbl";
        final String expected = "(?s).*Encountered \"\\*\" at line 1, column 15.*";

        sql(sql).fails(expected);
    }

    @Test
    void testCreateTemporaryView() {
        final String sql = "create temporary view v as select col1 from tbl";
        final String expected =
                "CREATE TEMPORARY VIEW `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTemporaryViewIfNotExists() {
        final String sql = "create temporary view if not exists v as select col1 from tbl";
        final String expected =
                "CREATE TEMPORARY VIEW IF NOT EXISTS `V`\n"
                        + "AS\n"
                        + "SELECT `COL1`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewIfNotExists() {
        final String sql = "create view if not exists v as select col1 from tbl";
        final String expected =
                "CREATE VIEW IF NOT EXISTS `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropView() {
        final String sql = "DROP VIEW IF EXISTS view_name";
        final String expected = "DROP VIEW IF EXISTS `VIEW_NAME`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropTemporaryView() {
        final String sql = "DROP TEMPORARY VIEW IF EXISTS view_name";
        final String expected = "DROP TEMPORARY VIEW IF EXISTS `VIEW_NAME`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterView() {
        sql("ALTER VIEW v1 RENAME TO v2").ok("ALTER VIEW `V1` RENAME TO `V2`");
        sql("ALTER VIEW v1 AS SELECT c1, c2 FROM tbl")
                .ok("ALTER VIEW `V1`\n" + "AS\n" + "SELECT `C1`, `C2`\n" + "FROM `TBL`");
    }

    @Test
    void testShowViews() {
        sql("show views").ok("SHOW VIEWS");
        sql("show views not like '%'").ok("SHOW VIEWS NOT LIKE '%'");

        sql("show views from db1").ok("SHOW VIEWS FROM `DB1`");
        sql("show views in db1").ok("SHOW VIEWS IN `DB1`");

        sql("show views from catalog1.db1").ok("SHOW VIEWS FROM `CATALOG1`.`DB1`");
        sql("show views in catalog1.db1").ok("SHOW VIEWS IN `CATALOG1`.`DB1`");

        sql("show views from db1 like '%'").ok("SHOW VIEWS FROM `DB1` LIKE '%'");
        sql("show views in db1 like '%'").ok("SHOW VIEWS IN `DB1` LIKE '%'");

        sql("show views from catalog1.db1 like '%'")
                .ok("SHOW VIEWS FROM `CATALOG1`.`DB1` LIKE '%'");
        sql("show views in catalog1.db1 like '%'").ok("SHOW VIEWS IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show views from db1 not like '%'").ok("SHOW VIEWS FROM `DB1` NOT LIKE '%'");
        sql("show views in db1 not like '%'").ok("SHOW VIEWS IN `DB1` NOT LIKE '%'");

        sql("show views from catalog1.db1 not like '%'")
                .ok("SHOW VIEWS FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show views in catalog1.db1 not like '%'")
                .ok("SHOW VIEWS IN `CATALOG1`.`DB1` NOT LIKE '%'");

        sql("show views ^db1^").fails("(?s).*Encountered \"db1\" at line 1, column 12.\n.*");
        sql("show views ^catalog1^.db1")
                .fails("(?s).*Encountered \"catalog1\" at line 1, column 12.\n.*");

        sql("show views ^search^ db1")
                .fails("(?s).*Encountered \"search\" at line 1, column 12.\n.*");

        sql("show views from db1 ^likes^ '%t'")
                .fails("(?s).*Encountered \"likes\" at line 1, column 21.\n.*");
    }

    @Test
    void testShowPartitions() {
        sql("show partitions c1.d1.tbl").ok("SHOW PARTITIONS `C1`.`D1`.`TBL`");
        sql("show partitions tbl partition (p=1)").ok("SHOW PARTITIONS `TBL` PARTITION (`P` = 1)");
    }

    @Test
    void testCreateTableWithNakedTableName() {
        String sql = "CREATE TABLE tbl1";
        sql(sql).node(new ValidationMatcher());
    }

    @Test
    void testCreateViewWithEmptyFields() {
        String sql = "CREATE VIEW v1 AS SELECT 1";
        sql(sql).ok("CREATE VIEW `V1`\n" + "AS\n" + "SELECT 1");
    }

    @Test
    void testCreateFunction() {
        sql("create function catalog1.db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function catalog1.db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function if not exists catalog1.db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION IF NOT EXISTS `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language java")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA");

        sql("create temporary system function  function1 as 'org.apache.flink.function.function1' language scala")
                .ok(
                        "CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA");

        // Temporary system function always belongs to the system and current session.
        sql("create temporary system function catalog1^.^db1.function1 as 'org.apache.flink.function.function1'")
                .fails("(?s).*Encountered \".\" at.*");

        sql("create ^system^ function function1 as 'org.apache.flink.function.function1'")
                .fails(
                        "CREATE SYSTEM FUNCTION is not supported, "
                                + "system functions can only be registered as temporary "
                                + "functions, you can use CREATE TEMPORARY SYSTEM FUNCTION instead.");

        // test creating functions with either jar or artifact
        for (String usageType : List.of("JAR", "ARTIFACT")) {
            sql("create temporary function function1 as 'org.apache.flink.function.function1' language java using "
                            + usageType
                            + " 'file:///path/to/test.jar'")
                    .ok(
                            "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING "
                                    + usageType
                                    + " 'file:///path/to/test.jar'");

            sql("create temporary function function1 as 'org.apache.flink.function.function1' language scala using "
                            + usageType
                            + " '/path/to/test.jar'")
                    .ok(
                            "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA USING "
                                    + usageType
                                    + " '/path/to/test.jar'");

            sql("create temporary system function function1 as 'org.apache.flink.function.function1' language scala using "
                            + usageType
                            + " '/path/to/test.jar'")
                    .ok(
                            "CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA USING "
                                    + usageType
                                    + " '/path/to/test.jar'");

            sql("create function function1 as 'org.apache.flink.function.function1' language java using "
                            + usageType
                            + " 'file:///path/to/test.jar', jar 'hdfs:///path/to/test2.jar'")
                    .ok(
                            "CREATE FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING "
                                    + usageType
                                    + " 'file:///path/to/test.jar', JAR 'hdfs:///path/to/test2.jar'");

            sql("create temporary function function1 as 'org.apache.flink.function.function1' language ^sql^ using "
                            + usageType
                            + " 'file:///path/to/test.jar'")
                    .fails(
                            "CREATE FUNCTION USING JAR/ARTIFACT syntax is not applicable to SQL language.");

            sql("create temporary function function1 as 'org.apache.flink.function.function1' language ^python^ using "
                            + usageType
                            + " 'file:///path/to/test.jar'")
                    .fails(
                            "CREATE FUNCTION USING JAR/ARTIFACT syntax is not applicable to PYTHON language.");

            sql("create function function1 as 'org.apache.flink.function.function1' language java using "
                            + usageType
                            + " 'file:///path/to/test.jar' WITH ('k1' = 'v1', 'k2' = 'v2')")
                    .ok(
                            "CREATE FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING "
                                    + usageType
                                    + " 'file:///path/to/test.jar'\nWITH (\n"
                                    + "  'k1' = 'v1',\n"
                                    + "  'k2' = 'v2'\n"
                                    + ")");

            sql("create temporary function function1 as 'org.apache.flink.function.function1' language java using "
                            + usageType
                            + " 'file:///path/to/test.jar' WITH ('k1' = 'v1', 'k2' = 'v2')")
                    .ok(
                            "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING "
                                    + usageType
                                    + " 'file:///path/to/test.jar'\nWITH (\n"
                                    + "  'k1' = 'v1',\n"
                                    + "  'k2' = 'v2'\n"
                                    + ")");
        }

        // test mixing jar and artifact keywords
        sql("create function function1 as 'org.apache.flink.function.function1' language java using jar 'file:///path/to/test.jar', artifact 'hdfs:///path/to/test2.jar'")
                .ok(
                        "CREATE FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING JAR 'file:///path/to/test.jar', ARTIFACT 'hdfs:///path/to/test2.jar'");

        sql("create function function1 as 'org.apache.flink.function.function1' language java using artifact 'file:///path/to/test.jar', jar 'hdfs:///path/to/test2.jar'")
                .ok(
                        "CREATE FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING ARTIFACT 'file:///path/to/test.jar', JAR 'hdfs:///path/to/test2.jar'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language java using ^file^ 'file:///path/to/test'")
                .fails(
                        "Encountered \"file\" at line 1, column 98.\n"
                                + "Was expecting one of:\n"
                                + "    \"ARTIFACT\" ...\n"
                                + "    \"JAR\" ...\n"
                                + "    .*");
    }

    @Test
    void testDropTemporaryFunction() {
        sql("drop temporary function catalog1.db1.function1")
                .ok("DROP TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary system function catalog1.db1.function1")
                .ok("DROP TEMPORARY SYSTEM FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary function if exists catalog1.db1.function1")
                .ok("DROP TEMPORARY FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary system function if exists catalog1.db1.function1")
                .ok("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");
    }

    @Test
    void testLoadModule() {
        sql("load module core").ok("LOAD MODULE `CORE`");

        sql("load module dummy with ('k1' = 'v1', 'k2' = 'v2')")
                .ok(
                        "LOAD MODULE `DUMMY`"
                                + "\nWITH (\n"
                                + "  'k1' = 'v1',\n"
                                + "  'k2' = 'v2'\n"
                                + ")");

        sql("load module ^'core'^")
                .fails("(?s).*Encountered \"\\\\'core\\\\'\" at line 1, column 13.\n.*");
    }

    @Test
    void testUnloadModule() {
        sql("unload module core").ok("UNLOAD MODULE `CORE`");

        sql("unload module ^'core'^")
                .fails("(?s).*Encountered \"\\\\'core\\\\'\" at line 1, column 15.\n.*");
    }

    @Test
    void testUseModules() {
        sql("use modules core").ok("USE MODULES `CORE`");

        sql("use modules x, y, z").ok("USE MODULES `X`, `Y`, `Z`");

        sql("use modules x^,^").fails("(?s).*Encountered \"<EOF>\" at line 1, column 14.\n.*");

        sql("use modules ^'core'^")
                .fails("(?s).*Encountered \"\\\\'core\\\\'\" at line 1, column 13.\n.*");
    }

    @Test
    void testShowModules() {
        sql("show modules").ok("SHOW MODULES");

        sql("show full modules").ok("SHOW FULL MODULES");
    }
}
