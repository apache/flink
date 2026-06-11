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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Locale;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Catalog and database parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserCatalogTest extends FlinkSqlParserTestBase {

    @Test
    void testShowCatalogs() {
        sql("show catalogs").ok("SHOW CATALOGS");

        sql("show catalogs like '%'").ok("SHOW CATALOGS LIKE '%'");
        sql("show catalogs not like '%'").ok("SHOW CATALOGS NOT LIKE '%'");

        sql("show catalogs ilike '%'").ok("SHOW CATALOGS ILIKE '%'");
        sql("show catalogs not ilike '%'").ok("SHOW CATALOGS NOT ILIKE '%'");

        sql("show catalogs ^likes^").fails("(?s).*Encountered \"likes\" at line 1, column 15.\n.*");
        sql("show catalogs not ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 19" + ".\n" + ".*");
        sql("show catalogs ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 15.\n.*");
        sql("show catalogs not ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 19" + ".\n" + ".*");
    }

    @Test
    void testShowCurrentCatalog() {
        sql("show current catalog").ok("SHOW CURRENT CATALOG");
    }

    @Test
    void testDescribeCatalog() {
        sql("describe catalog a").ok("DESCRIBE CATALOG `A`");
        sql("describe catalog extended a").ok("DESCRIBE CATALOG EXTENDED `A`");

        sql("desc catalog a").ok("DESCRIBE CATALOG `A`");
        sql("desc catalog extended a").ok("DESCRIBE CATALOG EXTENDED `A`");
    }

    @Test
    void testAlterCatalog() {
        sql("alter catalog a set ('k1'='v1', 'k2'='v2')")
                .ok("ALTER CATALOG `A` SET (\n" + "  'k1' = 'v1',\n" + "  'k2' = 'v2'\n" + ")");
        sql("alter catalog a reset ('k1')").ok("ALTER CATALOG `A` RESET (\n" + "  'k1'\n" + ")");
        sql("alter catalog a comment 'comment1'").ok("ALTER CATALOG `A` COMMENT 'comment1'");
    }

    // END

    @Test
    void testUseCatalog() {
        sql("use catalog a").ok("USE CATALOG `A`");
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testCreateCatalog(boolean ifNotExists, boolean comment) {
        final String ifNotExistsClause = ifNotExists ? "if not exists " : "";
        final String commentClause = comment ? "\ncomment 'HELLO'" : "";

        sql("create catalog "
                        + ifNotExistsClause
                        + "c1"
                        + commentClause
                        + "\nWITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " )\n")
                .ok(
                        "CREATE CATALOG "
                                + ifNotExistsClause.toUpperCase(Locale.ROOT)
                                + "`C1`"
                                + commentClause.toUpperCase(Locale.ROOT)
                                + "\nWITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")");
    }

    @Test
    void testShowCreateCatalog() {
        sql("show create catalog c1").ok("SHOW CREATE CATALOG `C1`");
    }

    @Test
    void testDropCatalog() {
        sql("drop catalog c1").ok("DROP CATALOG `C1`");
    }

    @Test
    void testShowDataBases() {
        sql("show databases").ok("SHOW DATABASES");

        sql("show databases like '%'").ok("SHOW DATABASES LIKE '%'");
        sql("show databases not like '%'").ok("SHOW DATABASES NOT LIKE '%'");

        sql("show databases from c1").ok("SHOW DATABASES FROM `C1`");
        sql("show databases in c1").ok("SHOW DATABASES IN `C1`");

        sql("show databases from c1 like '%'").ok("SHOW DATABASES FROM `C1` LIKE '%'");
        sql("show databases from c1 ilike '%'").ok("SHOW DATABASES FROM `C1` ILIKE '%'");
        sql("show databases in c1 like '%'").ok("SHOW DATABASES IN `C1` LIKE '%'");
        sql("show databases in c1 ilike '%'").ok("SHOW DATABASES IN `C1` ILIKE '%'");

        sql("show databases from c1 not like '%'").ok("SHOW DATABASES FROM `C1` NOT LIKE '%'");
        sql("show databases from c1 not ilike '%'").ok("SHOW DATABASES FROM `C1` NOT ILIKE '%'");
        sql("show databases in c1 not like '%'").ok("SHOW DATABASES IN `C1` NOT LIKE '%'");
        sql("show databases in c1 not ilike '%'").ok("SHOW DATABASES IN `C1` NOT ILIKE '%'");

        sql("show databases ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 16.\n.*");
        sql("show databases not ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 20" + ".\n" + ".*");
        sql("show databases ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 16.\n.*");
        sql("show databases not ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 20" + ".\n" + ".*");
    }

    @Test
    void testShowCurrentDatabase() {
        sql("show current database").ok("SHOW CURRENT DATABASE");
    }

    @Test
    void testUseDataBase() {
        sql("use default_db").ok("USE `DEFAULT_DB`");
        sql("use defaultCatalog.default_db").ok("USE `DEFAULTCATALOG`.`DEFAULT_DB`");
    }

    @Test
    void testCreateDatabase() {
        sql("create database db1").ok("CREATE DATABASE `DB1`");
        sql("create database if not exists db1").ok("CREATE DATABASE IF NOT EXISTS `DB1`");
        sql("create database catalog1.db1").ok("CREATE DATABASE `CATALOG1`.`DB1`");
        final String sql = "create database db1 comment 'test create database'";
        final String expected = "CREATE DATABASE `DB1`\n" + "COMMENT 'test create database'";
        sql(sql).ok(expected);
        final String sql1 =
                "create database db1 comment 'test create database'"
                        + "with ( 'key1' = 'value1', 'key2.a' = 'value2.a')";
        final String expected1 =
                "CREATE DATABASE `DB1`\n"
                        + "COMMENT 'test create database'"
                        + "\nWITH (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2.a' = 'value2.a'\n"
                        + ")";
        sql(sql1).ok(expected1);
    }

    @Test
    void testDropDatabase() {
        sql("drop database db1").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database catalog1.db1").ok("DROP DATABASE `CATALOG1`.`DB1` RESTRICT");
        sql("drop database db1 RESTRICT").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database db1 CASCADE").ok("DROP DATABASE `DB1` CASCADE");
    }

    @Test
    void testAlterDatabase() {
        final String sql = "alter database db1 set ('key1' = 'value1','key2.a' = 'value2.a')";
        final String expected =
                "ALTER DATABASE `DB1` SET (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2.a' = 'value2.a'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testDescribeDatabase() {
        sql("describe database db1").ok("DESCRIBE DATABASE `DB1`");
        sql("describe database catalog1.db1").ok("DESCRIBE DATABASE `CATALOG1`.`DB1`");
        sql("describe database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");

        sql("desc database db1").ok("DESCRIBE DATABASE `DB1`");
        sql("desc database catalog1.db1").ok("DESCRIBE DATABASE `CATALOG1`.`DB1`");
        sql("desc database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");
    }
}
