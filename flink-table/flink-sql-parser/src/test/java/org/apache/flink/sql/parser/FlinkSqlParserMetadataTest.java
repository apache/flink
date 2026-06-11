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

/** SHOW and DESCRIBE metadata parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserMetadataTest extends FlinkSqlParserTestBase {

    @Test
    void testAlterFunction() {
        sql("alter function function1 as 'org.apache.flink.function.function1'")
                .ok("ALTER FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("alter temporary function function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("alter temporary function function1 as 'org.apache.flink.function.function1' language scala")
                .ok(
                        "ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA");

        sql("alter temporary system function function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("alter temporary system function function1 as 'org.apache.flink.function.function1' language java")
                .ok(
                        "ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA");
    }

    @Test
    void testShowFunctions() {
        sql("show functions").ok("SHOW FUNCTIONS");
        sql("show user functions").ok("SHOW USER FUNCTIONS");

        sql("show functions like '%'").ok("SHOW FUNCTIONS LIKE '%'");
        sql("show functions not like '%'").ok("SHOW FUNCTIONS NOT LIKE '%'");
        sql("show user functions like '%'").ok("SHOW USER FUNCTIONS LIKE '%'");
        sql("show user functions not like '%'").ok("SHOW USER FUNCTIONS NOT LIKE '%'");

        sql("show functions from db1").ok("SHOW FUNCTIONS FROM `DB1`");
        sql("show user functions from db1").ok("SHOW USER FUNCTIONS FROM `DB1`");
        sql("show functions in db1").ok("SHOW FUNCTIONS IN `DB1`");
        sql("show user functions in db1").ok("SHOW USER FUNCTIONS IN `DB1`");

        sql("show functions from catalog1.db1").ok("SHOW FUNCTIONS FROM `CATALOG1`.`DB1`");
        sql("show user functions from catalog1.db1")
                .ok("SHOW USER FUNCTIONS FROM `CATALOG1`.`DB1`");
        sql("show functions in catalog1.db1").ok("SHOW FUNCTIONS IN `CATALOG1`.`DB1`");
        sql("show user functions in catalog1.db1").ok("SHOW USER FUNCTIONS IN `CATALOG1`.`DB1`");

        sql("show functions from db1 like '%'").ok("SHOW FUNCTIONS FROM `DB1` LIKE '%'");
        sql("show user functions from db1 like '%'").ok("SHOW USER FUNCTIONS FROM `DB1` LIKE '%'");
        sql("show functions in db1 ilike '%'").ok("SHOW FUNCTIONS IN `DB1` ILIKE '%'");
        sql("show user functions in db1 ilike '%'").ok("SHOW USER FUNCTIONS IN `DB1` ILIKE '%'");

        sql("show functions from catalog1.db1 ilike '%'")
                .ok("SHOW FUNCTIONS FROM `CATALOG1`.`DB1` ILIKE '%'");
        sql("show user functions from catalog1.db1 ilike '%'")
                .ok("SHOW USER FUNCTIONS FROM `CATALOG1`.`DB1` ILIKE '%'");
        sql("show functions in catalog1.db1 like '%'")
                .ok("SHOW FUNCTIONS IN `CATALOG1`.`DB1` LIKE '%'");
        sql("show user functions in catalog1.db1 like '%'")
                .ok("SHOW USER FUNCTIONS IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show functions from db1 not like '%'").ok("SHOW FUNCTIONS FROM `DB1` NOT LIKE '%'");
        sql("show user functions from db1 not like '%'")
                .ok("SHOW USER FUNCTIONS FROM `DB1` NOT LIKE '%'");
        sql("show functions in db1 not ilike '%'").ok("SHOW FUNCTIONS IN `DB1` NOT ILIKE '%'");
        sql("show user functions in db1 not ilike '%'")
                .ok("SHOW USER FUNCTIONS IN `DB1` NOT ILIKE '%'");

        sql("show functions from catalog1.db1 not like '%'")
                .ok("SHOW FUNCTIONS FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show user functions from catalog1.db1 not like '%'")
                .ok("SHOW USER FUNCTIONS FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show functions in catalog1.db1 not ilike '%'")
                .ok("SHOW FUNCTIONS IN `CATALOG1`.`DB1` NOT ILIKE '%'");
        sql("show user functions in catalog1.db1 not ilike '%'")
                .ok("SHOW USER FUNCTIONS IN `CATALOG1`.`DB1` NOT ILIKE '%'");

        sql("show functions ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 16.\n.*");
        sql("show functions not ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 20" + ".\n" + ".*");
        sql("show functions ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 16.\n.*");
        sql("show functions not ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 20" + ".\n" + ".*");
    }

    @Test
    void testShowProcedures() {
        sql("show procedures").ok("SHOW PROCEDURES");
        sql("show procedures not like '%'").ok("SHOW PROCEDURES NOT LIKE '%'");

        sql("show procedures from db1").ok("SHOW PROCEDURES FROM `DB1`");
        sql("show procedures in db1").ok("SHOW PROCEDURES IN `DB1`");

        sql("show procedures from catalog1.db1").ok("SHOW PROCEDURES FROM `CATALOG1`.`DB1`");
        sql("show procedures in catalog1.db1").ok("SHOW PROCEDURES IN `CATALOG1`.`DB1`");

        sql("show procedures from db1 like '%'").ok("SHOW PROCEDURES FROM `DB1` LIKE '%'");
        sql("show procedures in db1 ilike '%'").ok("SHOW PROCEDURES IN `DB1` ILIKE '%'");

        sql("show procedures from catalog1.db1 Ilike '%'")
                .ok("SHOW PROCEDURES FROM `CATALOG1`.`DB1` ILIKE '%'");
        sql("show procedures in catalog1.db1 like '%'")
                .ok("SHOW PROCEDURES IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show procedures from db1 not like '%'").ok("SHOW PROCEDURES FROM `DB1` NOT LIKE '%'");
        sql("show procedures in db1 not ilike '%'").ok("SHOW PROCEDURES IN `DB1` NOT ILIKE '%'");

        sql("show procedures from catalog1.db1 not like '%'")
                .ok("SHOW PROCEDURES FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show procedures in catalog1.db1 not ilike '%'")
                .ok("SHOW PROCEDURES IN `CATALOG1`.`DB1` NOT ILIKE '%'");

        sql("show procedures ^db1^").fails("(?s).*Encountered \"db1\" at line 1, column 17.\n.*");
        sql("show procedures ^catalog1^.db1")
                .fails("(?s).*Encountered \"catalog1\" at line 1, column 17.\n.*");

        sql("show procedures ^search^ db1")
                .fails("(?s).*Encountered \"search\" at line 1, column 17.\n.*");

        sql("show procedures from db1 ^likes^ '%t'")
                .fails("(?s).*Encountered \"likes\" at line 1, column 26.\n.*");
    }

    @Test
    void testShowTables() {
        sql("show tables").ok("SHOW TABLES");
        sql("show tables not like '%'").ok("SHOW TABLES NOT LIKE '%'");

        sql("show tables from db1").ok("SHOW TABLES FROM `DB1`");
        sql("show tables in db1").ok("SHOW TABLES IN `DB1`");

        sql("show tables from catalog1.db1").ok("SHOW TABLES FROM `CATALOG1`.`DB1`");
        sql("show tables in catalog1.db1").ok("SHOW TABLES IN `CATALOG1`.`DB1`");

        sql("show tables from db1 like '%'").ok("SHOW TABLES FROM `DB1` LIKE '%'");
        sql("show tables in db1 like '%'").ok("SHOW TABLES IN `DB1` LIKE '%'");

        sql("show tables from catalog1.db1 like '%'")
                .ok("SHOW TABLES FROM `CATALOG1`.`DB1` LIKE '%'");
        sql("show tables in catalog1.db1 like '%'").ok("SHOW TABLES IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show tables from db1 not like '%'").ok("SHOW TABLES FROM `DB1` NOT LIKE '%'");
        sql("show tables in db1 not like '%'").ok("SHOW TABLES IN `DB1` NOT LIKE '%'");

        sql("show tables from catalog1.db1 not like '%'")
                .ok("SHOW TABLES FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show tables in catalog1.db1 not like '%'")
                .ok("SHOW TABLES IN `CATALOG1`.`DB1` NOT LIKE '%'");

        sql("show tables ^db1^").fails("(?s).*Encountered \"db1\" at line 1, column 13.\n.*");
        sql("show tables ^catalog1^.db1")
                .fails("(?s).*Encountered \"catalog1\" at line 1, column 13.\n.*");

        sql("show tables ^search^ db1")
                .fails("(?s).*Encountered \"search\" at line 1, column 13.\n.*");

        sql("show tables from db1 ^likes^ '%t'")
                .fails("(?s).*Encountered \"likes\" at line 1, column 22.\n.*");
    }

    @Test
    void testShowCreateModel() {
        sql("show create model m1").ok("SHOW CREATE MODEL `M1`");
        sql("show create model catalog1.db1.m1").ok("SHOW CREATE MODEL `CATALOG1`.`DB1`.`M1`");
    }

    @Test
    void testShowCreateTable() {
        sql("show create table tbl").ok("SHOW CREATE TABLE `TBL`");
        sql("show create table catalog1.db1.tbl").ok("SHOW CREATE TABLE `CATALOG1`.`DB1`.`TBL`");
    }

    @Test
    void testShowCreateView() {
        sql("show create view v1").ok("SHOW CREATE VIEW `V1`");
        sql("show create view db1.v1").ok("SHOW CREATE VIEW `DB1`.`V1`");
        sql("show create view catalog1.db1.v1").ok("SHOW CREATE VIEW `CATALOG1`.`DB1`.`V1`");
    }

    @Test
    void testDescribeTable() {
        sql("describe tbl").ok("DESCRIBE `TBL`");
        sql("describe catalog1.db1.tbl").ok("DESCRIBE `CATALOG1`.`DB1`.`TBL`");
        sql("describe extended db1").ok("DESCRIBE EXTENDED `DB1`");

        sql("desc tbl").ok("DESCRIBE `TBL`");
        sql("desc catalog1.db1.tbl").ok("DESCRIBE `CATALOG1`.`DB1`.`TBL`");
        sql("desc extended db1").ok("DESCRIBE EXTENDED `DB1`");
    }

    @Test
    void testDescribeModel() {
        sql("describe model mdl").ok("DESCRIBE MODEL `MDL`");
        sql("describe model catalog1.db1.mdl").ok("DESCRIBE MODEL `CATALOG1`.`DB1`.`MDL`");

        sql("desc model mdl").ok("DESCRIBE MODEL `MDL`");
        sql("desc model catalog1.db1.mdl").ok("DESCRIBE MODEL `CATALOG1`.`DB1`.`MDL`");
    }

    @Test
    void testDescribeFunction() {
        sql("describe function fn").ok("DESCRIBE FUNCTION `FN`");
        sql("describe function catalog1.db1.fn").ok("DESCRIBE FUNCTION `CATALOG1`.`DB1`.`FN`");
        sql("describe function extended fn").ok("DESCRIBE FUNCTION EXTENDED `FN`");

        sql("desc function fn").ok("DESCRIBE FUNCTION `FN`");
        sql("desc function catalog1.db1.fn").ok("DESCRIBE FUNCTION `CATALOG1`.`DB1`.`FN`");
        sql("desc function extended fn").ok("DESCRIBE FUNCTION EXTENDED `FN`");
    }

    @Test
    void testShowColumns() {
        sql("show columns from tbl").ok("SHOW COLUMNS FROM `TBL`");
        sql("show columns in tbl").ok("SHOW COLUMNS IN `TBL`");

        sql("show columns from db1.tbl").ok("SHOW COLUMNS FROM `DB1`.`TBL`");
        sql("show columns in db1.tbl").ok("SHOW COLUMNS IN `DB1`.`TBL`");

        sql("show columns from catalog1.db1.tbl").ok("SHOW COLUMNS FROM `CATALOG1`.`DB1`.`TBL`");
        sql("show columns in catalog1.db1.tbl").ok("SHOW COLUMNS IN `CATALOG1`.`DB1`.`TBL`");

        sql("show columns from tbl like '%'").ok("SHOW COLUMNS FROM `TBL` LIKE '%'");
        sql("show columns in tbl like '%'").ok("SHOW COLUMNS IN `TBL` LIKE '%'");

        sql("show columns from db1.tbl like '%'").ok("SHOW COLUMNS FROM `DB1`.`TBL` LIKE '%'");
        sql("show columns in db1.tbl like '%'").ok("SHOW COLUMNS IN `DB1`.`TBL` LIKE '%'");

        sql("show columns from catalog1.db1.tbl like '%'")
                .ok("SHOW COLUMNS FROM `CATALOG1`.`DB1`.`TBL` LIKE '%'");
        sql("show columns in catalog1.db1.tbl like '%'")
                .ok("SHOW COLUMNS IN `CATALOG1`.`DB1`.`TBL` LIKE '%'");

        sql("show columns from tbl not like '%'").ok("SHOW COLUMNS FROM `TBL` NOT LIKE '%'");
        sql("show columns in tbl not like '%'").ok("SHOW COLUMNS IN `TBL` NOT LIKE '%'");

        sql("show columns from db1.tbl not like '%'")
                .ok("SHOW COLUMNS FROM `DB1`.`TBL` NOT LIKE '%'");
        sql("show columns in db1.tbl not like '%'").ok("SHOW COLUMNS IN `DB1`.`TBL` NOT LIKE '%'");

        sql("show columns from catalog1.db1.tbl not like '%'")
                .ok("SHOW COLUMNS FROM `CATALOG1`.`DB1`.`TBL` NOT LIKE '%'");
        sql("show columns in catalog1.db1.tbl not like '%'")
                .ok("SHOW COLUMNS IN `CATALOG1`.`DB1`.`TBL` NOT LIKE '%'");
    }
}
