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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test for {@link MySQLCatalog}. */
public class MySQLCatalogTest extends MySQLCatalogTestBase {
    // ------ databases ------

    @Test
    public void testGetDb_DatabaseNotExistException() throws Exception {
        String databaseNotExist = "nonexistent";
        exception.expect(DatabaseNotExistException.class);
        exception.expectMessage(
                String.format("Database %s does not exist in Catalog", databaseNotExist));
        catalog.getDatabase(databaseNotExist);
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertEquals(Collections.singletonList(TEST_DB), actual);
    }

    @Test
    public void testDbExists() throws Exception {
        String databaseNotExist = "nonexistent";
        assertFalse(catalog.databaseExists(databaseNotExist));
        assertTrue(catalog.databaseExists(TEST_DB));
    }

    // ------ tables ------

    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        assertEquals(
                Arrays.asList(
                        TEST_TABLE_ALL_TYPES,
                        TEST_SINK_TABLE_ALL_TYPES_WITH_YEAR_TYPE,
                        TEST_SINK_TABLE_ALL_TYPES_WITHOUT_YEAR_TYPE,
                        TEST_TABLE_SINK_FROM_GROUPED_BY),
                actual);
    }

    @Test
    public void testListTables_DatabaseNotExistException() throws DatabaseNotExistException {
        String anyDatabase = "anyDatabase";
        exception.expect(DatabaseNotExistException.class);
        exception.expectMessage(
                String.format("Database %s does not exist in Catalog", anyDatabase));
        catalog.listTables(anyDatabase);
    }

    @Test
    public void testTableExists() {
        String tableNotExist = "nonexist";
        assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist)));
        assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, TEST_TABLE_ALL_TYPES)));
    }

    @Test
    public void testGetTables_TableNotExistException() throws TableNotExistException {
        String anyTableNotExist = "anyTable";
        exception.expect(TableNotExistException.class);
        exception.expectMessage(
                String.format(
                        "Table (or view) %s.%s does not exist in Catalog",
                        TEST_DB, anyTableNotExist));
        catalog.getTable(new ObjectPath(TEST_DB, anyTableNotExist));
    }

    @Test
    public void testGetTables_TableNotExistException_NoDb() throws TableNotExistException {
        String databaseNotExist = "nonexistdb";
        String tableNotExist = "anyTable";
        exception.expect(TableNotExistException.class);
        exception.expectMessage(
                String.format(
                        "Table (or view) %s.%s does not exist in Catalog",
                        databaseNotExist, tableNotExist));
        catalog.getTable(new ObjectPath(databaseNotExist, tableNotExist));
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        // Test `test`.`t_without_geo_types`
        CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE_ALL_TYPES));
        assertEquals(table.getSchema(), TABLESCHEMA);
    }
}
