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

/** MODEL parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserModelTest extends FlinkSqlParserTestBase {

    @Test
    void testShowModels() {
        sql("show models").ok("SHOW MODELS");
        sql("show models from db1").ok("SHOW MODELS FROM `DB1`");
        sql("show models from catalog1.db1").ok("SHOW MODELS FROM `CATALOG1`.`DB1`");
        sql("show models in db1").ok("SHOW MODELS IN `DB1`");
        sql("show models in catalog1.db1").ok("SHOW MODELS IN `CATALOG1`.`DB1`");
    }

    @Test
    void testDropModel() {
        sql("drop model m1").ok("DROP MODEL `M1`");
        sql("drop model db1.m1").ok("DROP MODEL `DB1`.`M1`");
        sql("drop model catalog1.db1.m1").ok("DROP MODEL `CATALOG1`.`DB1`.`M1`");
    }

    @Test
    void testDropTemporaryModel() {
        sql("drop temporary model m1").ok("DROP TEMPORARY MODEL `M1`");
        sql("drop temporary model if exists m1").ok("DROP TEMPORARY MODEL IF EXISTS `M1`");
    }

    @Test
    void testDropModelIfExists() {
        sql("drop model if exists catalog1.db1.m1")
                .ok("DROP MODEL IF EXISTS `CATALOG1`.`DB1`.`M1`");
    }

    @Test
    void testAlterModelSet() {
        final String sql = "alter model m1 set ('key1' = 'value1','key2' = 'value2')";
        final String expected =
                "ALTER MODEL `M1` SET (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2' = 'value2'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterModelIfExists() {
        final String sql = "alter model if exists m1 set ('key1' = 'value1','key2' = 'value2')";
        final String expected =
                "ALTER MODEL IF EXISTS `M1` SET (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2' = 'value2'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterModelRename() {
        final String sql = "alter model m1 rename to m2";
        final String expected = "ALTER MODEL `M1` RENAME TO `M2`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterModelRenameIfExists() {
        final String sql = "alter model if exists m1 rename to m2";
        final String expected = "ALTER MODEL IF EXISTS `M1` RENAME TO `M2`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterModelReset() {
        final String sql = "alter model m1 reset ('key1', 'key2')";
        final String expected = "ALTER MODEL `M1` RESET (\n  'key1',\n  'key2'\n)";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterModelResetIfExists() {
        final String sql = "alter model if exists m1 reset ('key1', 'key2')";
        final String expected = "ALTER MODEL IF EXISTS `M1` RESET (\n  'key1',\n  'key2'\n)";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateModel() {
        sql("create model m1\n"
                        + " INPUT(col1 INT, col2 STRING)\n"
                        + " OUTPUT(label DOUBLE)\n"
                        + " COMMENT 'model_comment'\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " )\n")
                .ok(
                        "CREATE MODEL `M1` INPUT (\n"
                                + "  `COL1` INTEGER,\n"
                                + "  `COL2` STRING\n"
                                + ") OUTPUT (\n"
                                + "  `LABEL` DOUBLE\n"
                                + ")\n"
                                + "COMMENT 'model_comment'"
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")");
    }

    @Test
    void testCreateModelIfNotExists() {
        sql("create model if not exists m1\n"
                        + " INPUT(col1 INT, col2 STRING)\n"
                        + " OUTPUT(label DOUBLE)\n"
                        + " COMMENT 'model_comment'\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " )\n")
                .ok(
                        "CREATE MODEL IF NOT EXISTS `M1` INPUT (\n"
                                + "  `COL1` INTEGER,\n"
                                + "  `COL2` STRING\n"
                                + ") OUTPUT (\n"
                                + "  `LABEL` DOUBLE\n"
                                + ")\n"
                                + "COMMENT 'model_comment'"
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")");
    }

    @Test
    void testCreateModelAs() {
        sql("create model m1\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " ) as select f1, f2 from t1\n")
                .ok(
                        "CREATE MODEL `M1`"
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")\n"
                                + "AS\n"
                                + "SELECT `F1`, `F2`\n"
                                + "FROM `T1`");
    }

    @Test
    void testCreateModelAsIfNotExists() {
        sql("create model if not exists m1\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " ) as select f1, f2 from t1\n")
                .ok(
                        "CREATE MODEL IF NOT EXISTS `M1`"
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")\n"
                                + "AS\n"
                                + "SELECT `F1`, `F2`\n"
                                + "FROM `T1`");
    }

    @Test
    void testCreateModelAsWithInput() {
        sql("create model if not exists m1\n"
                        + " INPUT(col1 INT, col2 STRING)\n"
                        + " OUTPUT(label DOUBLE)\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " ) as select f1, f2 from t1\n")
                .ok(
                        "CREATE MODEL IF NOT EXISTS `M1` INPUT (\n"
                                + "  `COL1` INTEGER,\n"
                                + "  `COL2` STRING\n"
                                + ") OUTPUT (\n"
                                + "  `LABEL` DOUBLE\n"
                                + ")"
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")\n"
                                + "AS\n"
                                + "SELECT `F1`, `F2`\n"
                                + "FROM `T1`")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE MODEL AS SELECT syntax does not support to specify explicit input columns."));
    }

    @Test
    void testCreateModelAsWithOutput() {
        sql("create model if not exists m1\n"
                        + " OUTPUT(label DOUBLE)\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " ) as select f1, f2 from t1\n")
                .ok(
                        "CREATE MODEL IF NOT EXISTS `M1` OUTPUT (\n"
                                + "  `LABEL` DOUBLE\n"
                                + ")"
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")\n"
                                + "AS\n"
                                + "SELECT `F1`, `F2`\n"
                                + "FROM `T1`")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE MODEL AS SELECT syntax does not support to specify explicit output columns."));
    }

    @Test
    void testModelInFunction() {
        sql("select * from table(ml_predict(TABLE my_table, MODEL my_model))")
                .ok(
                        "SELECT *\n"
                                + "FROM TABLE(`ML_PREDICT`((TABLE `MY_TABLE`), MODEL `MY_MODEL`))");
    }

    @Test
    void testModelInFunctionWithoutTable() {
        sql("select * from func(TABLE my_table, MODEL cat.db.my_model)")
                .ok(
                        "SELECT *\n"
                                + "FROM TABLE(`FUNC`((TABLE `MY_TABLE`), MODEL `CAT`.`DB`.`MY_MODEL`))");
    }

    @Test
    void testModelInFunctionNamedArgs() {
        sql("select * from table(ml_predict(INPUT => TABLE my_table, model => MODEL my_model))")
                .ok(
                        "SELECT *\n"
                                + "FROM TABLE(`ML_PREDICT`(`INPUT` => (TABLE `MY_TABLE`), `MODEL` => (MODEL `MY_MODEL`)))");
    }
}
