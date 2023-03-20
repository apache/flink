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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Class for unit tests to run on catalogs. */
@ExtendWith(TestLoggerExtension.class)
public abstract class CatalogTest {
    protected static final String IS_STREAMING = "is_streaming";

    protected final String db1 = "db1";
    protected final String db2 = "db2";

    protected final String t1 = "t1";
    protected final String t2 = "t2";
    protected final String t3 = "t3";
    protected final ObjectPath path1 = new ObjectPath(db1, t1);
    protected final ObjectPath path2 = new ObjectPath(db2, t2);
    protected final ObjectPath path3 = new ObjectPath(db1, t2);
    protected final ObjectPath path4 = new ObjectPath(db1, t3);
    protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
    protected final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

    public static final String TEST_CATALOG_NAME = "test-catalog";

    protected static final String TEST_COMMENT = "test comment";

    protected static Catalog catalog;

    @AfterEach
    void cleanup() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, true);
        }
        if (catalog.tableExists(path2)) {
            catalog.dropTable(path2, true);
        }
        if (catalog.tableExists(path3)) {
            catalog.dropTable(path3, true);
        }
        if (catalog.tableExists(path4)) {
            catalog.dropTable(path4, true);
        }
        if (catalog.functionExists(path1)) {
            catalog.dropFunction(path1, true);
        }
        if (catalog.databaseExists(db1)) {
            catalog.dropDatabase(db1, true, false);
        }
        if (catalog.databaseExists(db2)) {
            catalog.dropDatabase(db2, true, false);
        }
    }

    @AfterAll
    static void closeup() {
        if (catalog != null) {
            catalog.close();
        }
    }

    // ------ databases ------

    @Test
    public void testCreateDb() throws Exception {
        assertThat(catalog.databaseExists(db1)).isFalse();

        CatalogDatabase cd = createDb();
        catalog.createDatabase(db1, cd, false);

        assertThat(catalog.databaseExists(db1)).isTrue();
        CatalogTestUtil.checkEquals(cd, catalog.getDatabase(db1));
    }

    @Test
    public void testCreateDb_DatabaseAlreadyExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(() -> catalog.createDatabase(db1, createDb(), false))
                .isInstanceOf(DatabaseAlreadyExistException.class)
                .hasMessage("Database db1 already exists in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testCreateDb_DatabaseAlreadyExist_ignored() throws Exception {
        CatalogDatabase cd1 = createDb();
        catalog.createDatabase(db1, cd1, false);
        List<String> dbs = catalog.listDatabases();

        CatalogTestUtil.checkEquals(cd1, catalog.getDatabase(db1));
        assertThat(dbs).hasSize(2);
        assertThat(new HashSet<>(dbs))
                .isEqualTo(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabase())));

        catalog.createDatabase(db1, createAnotherDb(), true);

        CatalogTestUtil.checkEquals(cd1, catalog.getDatabase(db1));
        assertThat(dbs).containsExactlyInAnyOrder(db1, catalog.getDefaultDatabase());
    }

    @Test
    public void testGetDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.getDatabase("nonexistent"))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage(
                        "Database nonexistent does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testDropDb() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThat(catalog.databaseExists(db1)).isTrue();

        catalog.dropDatabase(db1, false, true);

        assertThat(catalog.databaseExists(db1)).isFalse();
    }

    @Test
    public void testDropDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.dropDatabase(db1, false, false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testDropDb_DatabaseNotExist_Ignore() throws Exception {
        catalog.dropDatabase(db1, true, false);
    }

    @Test
    public void testDropDb_DatabaseNotEmptyException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        assertThatThrownBy(() -> catalog.dropDatabase(db1, true, false))
                .isInstanceOf(DatabaseNotEmptyException.class)
                .hasMessage("Database db1 in catalog test-catalog is not empty.");
    }

    @Test
    public void testAlterDb() throws Exception {
        CatalogDatabase db = createDb();
        catalog.createDatabase(db1, db, false);

        CatalogDatabase newDb = createAnotherDb();
        catalog.alterDatabase(db1, newDb, false);

        assertThat(
                        catalog.getDatabase(db1)
                                .getProperties()
                                .entrySet()
                                .containsAll(db.getProperties().entrySet()))
                .isFalse();
        CatalogTestUtil.checkEquals(newDb, catalog.getDatabase(db1));
    }

    @Test
    public void testAlterDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.alterDatabase("nonexistent", createDb(), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage(
                        "Database nonexistent does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testAlterDb_DatabaseNotExist_ignored() throws Exception {
        catalog.alterDatabase("nonexistent", createDb(), true);

        assertThat(catalog.databaseExists("nonexistent")).isFalse();
    }

    @Test
    public void testDbExists() throws Exception {
        assertThat(catalog.databaseExists("nonexistent")).isFalse();

        catalog.createDatabase(db1, createDb(), false);

        assertThat(catalog.databaseExists(db1)).isTrue();
    }

    // ------ tables ------

    @Test
    public void testCreateTable_Streaming() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createStreamingTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
    }

    @Test
    public void testCreateTable_Batch() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        // Non-partitioned table
        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);

        CatalogBaseTable tableCreated = catalog.getTable(path1);

        CatalogTestUtil.checkEquals(table, (CatalogTable) tableCreated);
        assertThat(tableCreated.getDescription().isPresent()).isTrue();
        assertThat(tableCreated.getDescription().get()).isEqualTo(TEST_COMMENT);

        List<String> tables = catalog.listTables(db1);

        assertThat(tables).hasSize(1);
        assertThat(tables.get(0)).isEqualTo(path1.getObjectName());

        catalog.dropTable(path1, false);
    }

    @Test
    public void testCreatePartitionedTable_Batch() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        // Partitioned table
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

        List<String> tables = catalog.listTables(db1);

        assertThat(tables).hasSize(1);
        assertThat(tables.get(0)).isEqualTo(path1.getObjectName());
    }

    @Test
    public void testCreateTable_DatabaseNotExistException() {
        assertThat(catalog.databaseExists(db1)).isFalse();

        assertThatThrownBy(() -> catalog.createTable(nonExistObjectPath, createTable(), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testCreateTable_TableAlreadyExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        assertThatThrownBy(() -> catalog.createTable(path1, createTable(), false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage(
                        "Table (or view) db1.t1 already exists in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testCreateTable_TableAlreadyExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

        catalog.createTable(path1, createAnotherTable(), true);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
    }

    @Test
    public void testGetTable_TableNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(() -> catalog.getTable(nonExistObjectPath))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) db1.nonexist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testGetTable_TableNotExistException_NoDb() {
        assertThatThrownBy(() -> catalog.getTable(nonExistObjectPath))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) db1.nonexist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testDropTable_nonPartitionedTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        assertThat(catalog.tableExists(path1)).isTrue();

        catalog.dropTable(path1, false);

        assertThat(catalog.tableExists(path1)).isFalse();
    }

    @Test
    public void testDropTable_TableNotExistException() {
        assertThatThrownBy(() -> catalog.dropTable(nonExistDbPath, false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) non.exist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testDropTable_TableNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.dropTable(nonExistObjectPath, true);
    }

    @Test
    public void testAlterTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        // Non-partitioned table
        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

        CatalogTable newTable = createAnotherTable();
        catalog.alterTable(path1, newTable, false);

        assertThat(catalog.getTable(path1)).isNotEqualTo(table);
        CatalogTestUtil.checkEquals(newTable, (CatalogTable) catalog.getTable(path1));

        catalog.dropTable(path1, false);

        // View
        CatalogView view = createView();
        catalog.createTable(path3, view, false);

        CatalogTestUtil.checkEquals(view, (CatalogView) catalog.getTable(path3));

        CatalogView newView = createAnotherView();
        catalog.alterTable(path3, newView, false);

        assertThat(catalog.getTable(path3)).isNotEqualTo(view);
        CatalogTestUtil.checkEquals(newView, (CatalogView) catalog.getTable(path3));
    }

    @Test
    public void testAlterPartitionedTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        // Partitioned table
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

        CatalogTable newTable = createAnotherPartitionedTable();
        catalog.alterTable(path1, newTable, false);

        CatalogTestUtil.checkEquals(newTable, (CatalogTable) catalog.getTable(path1));
    }

    @Test
    public void testAlterTable_differentTypedTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);

        assertThatThrownBy(() -> catalog.alterTable(path1, new TestView(), false))
                .isInstanceOf(CatalogException.class)
                .hasMessage(
                        "Table types don't match. Existing table is 'TABLE' and new table is 'VIEW'.");
    }

    @Test
    public void testAlterTable_TableNotExistException() {
        assertThatThrownBy(() -> catalog.alterTable(nonExistDbPath, createTable(), false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) non.exist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testAlterTable_TableNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.alterTable(nonExistObjectPath, createTable(), true);

        assertThat(catalog.tableExists(nonExistObjectPath)).isFalse();
    }

    @Test
    public void testRenameTable_nonPartitionedTable() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

        catalog.renameTable(path1, t2, false);

        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path3));
        assertThat(catalog.tableExists(path1)).isFalse();
    }

    @Test
    public void testRenameTable_TableNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(() -> catalog.renameTable(path1, t2, false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) db1.t1 does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testRenameTable_TableNotExistException_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.renameTable(path1, t2, true);
    }

    @Test
    public void testRenameTable_TableAlreadyExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);
        catalog.createTable(path3, createAnotherTable(), false);

        assertThatThrownBy(() -> catalog.renameTable(path1, t2, false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage(
                        "Table (or view) db1.t2 already exists in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testListTables() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        catalog.createTable(path1, createTable(), false);
        catalog.createTable(path3, createTable(), false);
        catalog.createTable(path4, createView(), false);

        assertThat(catalog.listTables(db1)).hasSize(3);
        assertThat(catalog.listViews(db1)).hasSize(1);
    }

    @Test
    public void testTableExists() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThat(catalog.tableExists(path1)).isFalse();

        catalog.createTable(path1, createTable(), false);

        assertThat(catalog.tableExists(path1)).isTrue();
    }

    // ------ views ------

    @Test
    public void testCreateView() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThat(catalog.tableExists(path1)).isFalse();

        CatalogView view = createView();
        catalog.createTable(path1, view, false);

        assertThat(catalog.getTable(path1)).isInstanceOf(CatalogView.class);
        CatalogTestUtil.checkEquals(view, (CatalogView) catalog.getTable(path1));
    }

    @Test
    public void testCreateView_DatabaseNotExistException() {
        assertThat(catalog.databaseExists(db1)).isFalse();

        assertThatThrownBy(() -> catalog.createTable(nonExistObjectPath, createView(), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testCreateView_TableAlreadyExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createView(), false);

        assertThatThrownBy(() -> catalog.createTable(path1, createView(), false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage(
                        "Table (or view) db1.t1 already exists in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testCreateView_TableAlreadyExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogView view = createView();
        catalog.createTable(path1, view, false);

        assertThat(catalog.getTable(path1)).isInstanceOf(CatalogView.class);
        CatalogTestUtil.checkEquals(view, (CatalogView) catalog.getTable(path1));

        catalog.createTable(path1, createAnotherView(), true);

        assertThat(catalog.getTable(path1)).isInstanceOf(CatalogView.class);
        CatalogTestUtil.checkEquals(view, (CatalogView) catalog.getTable(path1));
    }

    @Test
    public void testDropView() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createView(), false);

        assertThat(catalog.tableExists(path1)).isTrue();

        catalog.dropTable(path1, false);

        assertThat(catalog.tableExists(path1)).isFalse();
    }

    @Test
    public void testAlterView() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogView view = createView();
        catalog.createTable(path1, view, false);

        CatalogTestUtil.checkEquals(view, (CatalogView) catalog.getTable(path1));

        CatalogView newView = createAnotherView();
        catalog.alterTable(path1, newView, false);

        assertThat(catalog.getTable(path1)).isInstanceOf(CatalogView.class);
        CatalogTestUtil.checkEquals(newView, (CatalogView) catalog.getTable(path1));
    }

    @Test
    public void testAlterView_TableNotExistException() throws Exception {
        assertThatThrownBy(() -> catalog.alterTable(nonExistDbPath, createTable(), false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) non.exist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testAlterView_TableNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.alterTable(nonExistObjectPath, createView(), true);

        assertThat(catalog.tableExists(nonExistObjectPath)).isFalse();
    }

    @Test
    public void testListView() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThat(catalog.listTables(db1)).isEmpty();

        catalog.createTable(path1, createView(), false);
        catalog.createTable(path3, createTable(), false);

        assertThat(catalog.listTables(db1)).hasSize(2);
        assertThat(new HashSet<>(catalog.listTables(db1)))
                .isEqualTo(
                        new HashSet<>(Arrays.asList(path1.getObjectName(), path3.getObjectName())));
        assertThat(catalog.listViews(db1))
                .isEqualTo(Collections.singletonList(path1.getObjectName()));
    }

    @Test
    public void testRenameView() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createView(), false);

        assertThat(catalog.tableExists(path1)).isTrue();

        catalog.renameTable(path1, t2, false);

        assertThat(catalog.tableExists(path1)).isFalse();
        assertThat(catalog.tableExists(path3)).isTrue();
    }

    // ------ functions ------

    @Test
    public void testCreateFunction() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThat(catalog.functionExists(path1)).isFalse();

        catalog.createFunction(path1, createFunction(), false);

        assertThat(catalog.functionExists(path1)).isTrue();
    }

    @Test
    public void testCreatePythonFunction() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogFunction pythonFunction = createPythonFunction();
        catalog.createFunction(path1, createPythonFunction(), false);

        CatalogFunction actual = catalog.getFunction(path1);
        checkEquals(pythonFunction, actual);
    }

    @Test
    public void testCreateFunction_DatabaseNotExistException() {
        assertThat(catalog.databaseExists(db1)).isFalse();

        assertThatThrownBy(() -> catalog.createFunction(path1, createFunction(), false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testCreateFunction_FunctionAlreadyExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createFunction(path1, createFunction(), false);

        assertThat(catalog.functionExists(path1)).isTrue();

        // test 'ignoreIfExist' flag
        catalog.createFunction(path1, createAnotherFunction(), true);

        assertThatThrownBy(() -> catalog.createFunction(path1, createFunction(), false))
                .isInstanceOf(FunctionAlreadyExistException.class)
                .hasMessage("Function db1.t1 already exists in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testAlterFunction() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogFunction func = createFunction();
        catalog.createFunction(path1, func, false);

        checkEquals(func, catalog.getFunction(path1));

        CatalogFunction newFunc = createAnotherFunction();
        catalog.alterFunction(path1, newFunc, false);
        CatalogFunction actual = catalog.getFunction(path1);

        assertThat(func.getClassName().equals(actual.getClassName())).isFalse();
        checkEquals(newFunc, actual);
    }

    @Test
    public void testAlterPythonFunction() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogFunction func = createFunction();
        catalog.createFunction(path1, func, false);

        checkEquals(func, catalog.getFunction(path1));

        CatalogFunction newFunc = createPythonFunction();
        catalog.alterFunction(path1, newFunc, false);
        CatalogFunction actual = catalog.getFunction(path1);

        assertThat(func.getClassName().equals(actual.getClassName())).isFalse();
        checkEquals(newFunc, actual);
    }

    @Test
    public void testAlterFunction_FunctionNotExistException() {
        assertThatThrownBy(() -> catalog.alterFunction(nonExistObjectPath, createFunction(), false))
                .isInstanceOf(FunctionNotExistException.class)
                .hasMessage(
                        "Function db1.nonexist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testAlterFunction_FunctionNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.alterFunction(nonExistObjectPath, createFunction(), true);

        assertThat(catalog.functionExists(nonExistObjectPath)).isFalse();
    }

    @Test
    public void testListFunctions() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogFunction func = createFunction();
        catalog.createFunction(path1, func, false);

        assertThat(catalog.listFunctions(db1).get(0)).isEqualTo(path1.getObjectName());
    }

    @Test
    public void testListFunctions_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.listFunctions(db1))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database db1 does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testGetFunction_FunctionNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(() -> catalog.getFunction(nonExistObjectPath))
                .isInstanceOf(FunctionNotExistException.class)
                .hasMessage(
                        "Function db1.nonexist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testGetFunction_FunctionNotExistException_NoDb() {
        assertThatThrownBy(() -> catalog.getFunction(nonExistObjectPath))
                .isInstanceOf(FunctionNotExistException.class)
                .hasMessage(
                        "Function db1.nonexist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testDropFunction() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createFunction(path1, createFunction(), false);

        assertThat(catalog.functionExists(path1)).isTrue();

        catalog.dropFunction(path1, false);

        assertThat(catalog.functionExists(path1)).isFalse();
    }

    @Test
    public void testDropFunction_FunctionNotExistException() {
        assertThatThrownBy(() -> catalog.dropFunction(nonExistDbPath, false))
                .isInstanceOf(FunctionNotExistException.class)
                .hasMessage(
                        "Function non.exist does not exist in Catalog " + TEST_CATALOG_NAME + ".");
    }

    @Test
    public void testDropFunction_FunctionNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.dropFunction(nonExistObjectPath, true);
        catalog.dropDatabase(db1, false, false);
    }

    // ------ partitions ------

    @Test
    public void testCreatePartition() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);

        assertThat(catalog.listPartitions(path1)).isEmpty();

        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

        assertThat(catalog.listPartitions(path1)).containsExactly(createPartitionSpec());
        assertThat(catalog.listPartitions(path1, createPartitionSpecSubset()))
                .containsExactly(createPartitionSpec());
        CatalogTestUtil.checkEquals(
                createPartition(), catalog.getPartition(path1, createPartitionSpec()));

        catalog.createPartition(path1, createAnotherPartitionSpec(), createPartition(), false);

        assertThat(catalog.listPartitions(path1))
                .isEqualTo(Arrays.asList(createPartitionSpec(), createAnotherPartitionSpec()));
        assertThat(catalog.listPartitions(path1, createPartitionSpecSubset()))
                .isEqualTo(Arrays.asList(createPartitionSpec(), createAnotherPartitionSpec()));
        CatalogTestUtil.checkEquals(
                createPartition(), catalog.getPartition(path1, createAnotherPartitionSpec()));
    }

    @Test
    public void testCreatePartition_TableNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(
                        () ->
                                catalog.createPartition(
                                        path1, createPartitionSpec(), createPartition(), false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        String.format(
                                "Table (or view) %s does not exist in Catalog %s.",
                                path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testCreatePartition_TableNotPartitionedException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        assertThatThrownBy(
                        () ->
                                catalog.createPartition(
                                        path1, createPartitionSpec(), createPartition(), false))
                .isInstanceOf(TableNotPartitionedException.class)
                .hasMessage(
                        String.format(
                                "Table %s in catalog %s is not partitioned.",
                                path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testCreatePartition_PartitionSpecInvalidException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
        assertThatThrownBy(
                        () ->
                                catalog.createPartition(
                                        path1, partitionSpec, createPartition(), false))
                .isInstanceOf(PartitionSpecInvalidException.class)
                .hasMessage(
                        String.format(
                                "PartitionSpec %s does not match partition keys %s of table %s in catalog %s.",
                                partitionSpec,
                                table.getPartitionKeys(),
                                path1.getFullName(),
                                TEST_CATALOG_NAME));
    }

    @Test
    public void testCreatePartition_PartitionAlreadyExistsException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        CatalogPartition partition = createPartition();
        catalog.createPartition(path1, createPartitionSpec(), partition, false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();

        assertThatThrownBy(
                        () ->
                                catalog.createPartition(
                                        path1, partitionSpec, createPartition(), false))
                .isInstanceOf(PartitionAlreadyExistsException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s already exists.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testCreatePartition_PartitionAlreadyExists_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        catalog.createPartition(path1, partitionSpec, createPartition(), false);
        catalog.createPartition(path1, partitionSpec, createPartition(), true);
    }

    @Test
    public void testDropPartition() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

        assertThat(catalog.listPartitions(path1)).containsExactly(createPartitionSpec());

        catalog.dropPartition(path1, createPartitionSpec(), false);

        assertThat(catalog.listPartitions(path1)).isEmpty();
    }

    @Test
    public void testDropPartition_TableNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(() -> catalog.dropPartition(path1, partitionSpec, false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testDropPartition_TableNotPartitioned() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(() -> catalog.dropPartition(path1, partitionSpec, false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testDropPartition_PartitionSpecInvalid() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
        assertThatThrownBy(() -> catalog.dropPartition(path1, partitionSpec, false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testDropPartition_PartitionNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(() -> catalog.dropPartition(path1, partitionSpec, false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testDropPartition_PartitionNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.dropPartition(path1, createPartitionSpec(), true);
    }

    @Test
    public void testAlterPartition() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

        assertThat(catalog.listPartitions(path1)).containsExactly(createPartitionSpec());
        CatalogPartition cp = catalog.getPartition(path1, createPartitionSpec());
        CatalogTestUtil.checkEquals(createPartition(), cp);
        assertThat(cp.getProperties().get("k")).isNull();

        CatalogPartition another = createPartition();
        another.getProperties().put("k", "v");

        catalog.alterPartition(path1, createPartitionSpec(), another, false);

        assertThat(catalog.listPartitions(path1)).containsExactly(createPartitionSpec());

        cp = catalog.getPartition(path1, createPartitionSpec());

        CatalogTestUtil.checkEquals(another, cp);
        assertThat(cp.getProperties().get("k")).isEqualTo("v");
    }

    @Test
    public void testAlterPartition_TableNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(
                        () ->
                                catalog.alterPartition(
                                        path1, partitionSpec, createPartition(), false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testAlterPartition_TableNotPartitioned() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(
                        () ->
                                catalog.alterPartition(
                                        path1, partitionSpec, createPartition(), false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testAlterPartition_PartitionSpecInvalid() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
        assertThatThrownBy(
                        () ->
                                catalog.alterPartition(
                                        path1, partitionSpec, createPartition(), false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testAlterPartition_PartitionNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(
                        () ->
                                catalog.alterPartition(
                                        path1, partitionSpec, createPartition(), false))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testAlterPartition_PartitionNotExist_ignored() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.alterPartition(path1, createPartitionSpec(), createPartition(), true);
    }

    @Test
    public void testGetPartition_TableNotExist() throws Exception {
        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(() -> catalog.getPartition(path1, partitionSpec))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testGetPartition_TableNotPartitioned() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(() -> catalog.getPartition(path1, partitionSpec))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testGetPartition_PartitionSpecInvalid_invalidPartitionSpec() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
        assertThatThrownBy(() -> catalog.getPartition(path1, partitionSpec))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testGetPartition_PartitionSpecInvalid_sizeNotEqual() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createPartitionedTable();
        catalog.createTable(path1, table, false);

        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(
                        new HashMap<String, String>() {
                            {
                                put("second", "bob");
                            }
                        });
        assertThatThrownBy(() -> catalog.getPartition(path1, partitionSpec))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testGetPartition_PartitionNotExist() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);

        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        assertThatThrownBy(() -> catalog.getPartition(path1, partitionSpec))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        String.format(
                                "Partition %s of table %s in catalog %s does not exist.",
                                partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
    }

    @Test
    public void testPartitionExists() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

        assertThat(catalog.partitionExists(path1, createPartitionSpec())).isTrue();
        assertThat(catalog.partitionExists(path2, createPartitionSpec())).isFalse();
        assertThat(
                        catalog.partitionExists(
                                ObjectPath.fromString("non.exist"), createPartitionSpec()))
                .isFalse();
    }

    @Test
    public void testListPartitionPartialSpec() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);
        catalog.createPartition(path1, createAnotherPartitionSpec(), createPartition(), false);

        assertThat(catalog.listPartitions(path1, createPartitionSpecSubset())).hasSize(2);
        assertThat(catalog.listPartitions(path1, createAnotherPartitionSpecSubset())).hasSize(1);
    }

    // ------ table and column stats ------

    @Test
    public void testGetTableStats_TableNotExistException() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        assertThatThrownBy(() -> catalog.getTableStatistics(path1))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) db1.t1 does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testGetPartitionStats() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, createPartitionedTable(), false);
        catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);
        CatalogTableStatistics tableStatistics =
                catalog.getPartitionStatistics(path1, createPartitionSpec());
        assertThat(tableStatistics.getFileCount()).isEqualTo(-1);
        assertThat(tableStatistics.getRawDataSize()).isEqualTo(-1);
        assertThat(tableStatistics.getTotalSize()).isEqualTo(-1);
        assertThat(tableStatistics.getRowCount()).isEqualTo(-1);
    }

    @Test
    public void testAlterTableStats() throws Exception {
        // Non-partitioned table
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable table = createTable();
        catalog.createTable(path1, table, false);
        CatalogTableStatistics tableStats = new CatalogTableStatistics(100, 10, 1000, 10000);
        catalog.alterTableStatistics(path1, tableStats, false);
        CatalogTableStatistics actual = catalog.getTableStatistics(path1);

        // we don't check fileCount and totalSize here for hive will automatically calc and set to
        // real num.
        assertThat(actual.getRowCount()).isEqualTo(tableStats.getRowCount());
        assertThat(actual.getRawDataSize()).isEqualTo(tableStats.getRawDataSize());
    }

    @Test
    public void testAlterTableStats_partitionedTable() throws Exception {
        // alterTableStats() should do nothing for partitioned tables
        // getTableStats() should return unknown column stats for partitioned tables
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable catalogTable = createPartitionedTable();
        catalog.createTable(path1, catalogTable, false);

        CatalogTableStatistics stats = new CatalogTableStatistics(100, 1, 1000, 10000);

        catalog.alterTableStatistics(path1, stats, false);

        assertThat(catalog.getTableStatistics(path1)).isEqualTo(CatalogTableStatistics.UNKNOWN);
    }

    @Test
    public void testAlterPartitionTableStats() throws Exception {
        catalog.createDatabase(db1, createDb(), false);
        CatalogTable catalogTable = createPartitionedTable();
        catalog.createTable(path1, catalogTable, false);
        CatalogPartitionSpec partitionSpec = createPartitionSpec();
        catalog.createPartition(path1, partitionSpec, createPartition(), true);
        CatalogTableStatistics stats = new CatalogTableStatistics(100, 1, 1000, 10000);
        catalog.alterPartitionStatistics(path1, partitionSpec, stats, false);
        CatalogTableStatistics actual = catalog.getPartitionStatistics(path1, partitionSpec);
        assertThat(actual.getRowCount()).isEqualTo(stats.getRowCount());
        assertThat(actual.getRawDataSize()).isEqualTo(stats.getRawDataSize());
    }

    @Test
    public void testAlterTableStats_TableNotExistException() {
        assertThatThrownBy(
                        () ->
                                catalog.alterTableStatistics(
                                        new ObjectPath(catalog.getDefaultDatabase(), "nonexist"),
                                        CatalogTableStatistics.UNKNOWN,
                                        false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        "Table (or view) default.nonexist does not exist in Catalog "
                                + TEST_CATALOG_NAME
                                + ".");
    }

    @Test
    public void testAlterTableStats_TableNotExistException_ignore() throws Exception {
        catalog.alterTableStatistics(
                new ObjectPath("non", "exist"), CatalogTableStatistics.UNKNOWN, true);
    }

    // ------ utilities ------

    /**
     * Create a CatalogDatabase instance by specific catalog implementation.
     *
     * @return a CatalogDatabase instance
     */
    public abstract CatalogDatabase createDb();

    /**
     * Create another CatalogDatabase instance by specific catalog implementation.
     *
     * @return another CatalogDatabase instance
     */
    public abstract CatalogDatabase createAnotherDb();

    /**
     * Create a CatalogTable instance by specific catalog implementation.
     *
     * @return a CatalogTable instance
     */
    public abstract CatalogTable createTable();

    /**
     * Create another CatalogTable instance by specific catalog implementation.
     *
     * @return another CatalogTable instance
     */
    public abstract CatalogTable createAnotherTable();

    /**
     * Create a streaming CatalogTable instance by specific catalog implementation.
     *
     * @return a streaming CatalogTable instance
     */
    public abstract CatalogTable createStreamingTable();

    /**
     * Create a partitioned CatalogTable instance by specific catalog implementation.
     *
     * @return a streaming CatalogTable instance
     */
    public abstract CatalogTable createPartitionedTable();

    /**
     * Create another partitioned CatalogTable instance by specific catalog implementation.
     *
     * @return another partitioned CatalogTable instance
     */
    public abstract CatalogTable createAnotherPartitionedTable();

    /**
     * Create a CatalogView instance by specific catalog implementation.
     *
     * @return a CatalogView instance
     */
    public abstract CatalogView createView();

    /**
     * Create another CatalogView instance by specific catalog implementation.
     *
     * @return another CatalogView instance
     */
    public abstract CatalogView createAnotherView();

    /**
     * Create a CatalogFunction instance by specific catalog implementation.
     *
     * @return a CatalogFunction instance
     */
    protected abstract CatalogFunction createFunction();

    /** Create a Python CatalogFunction instance by specific catalog implementation. */
    protected abstract CatalogFunction createPythonFunction();

    /**
     * Create another CatalogFunction instance by specific catalog implementation.
     *
     * @return another CatalogFunction instance
     */
    protected abstract CatalogFunction createAnotherFunction();

    /**
     * Creates a CatalogPartition by specific catalog implementation.
     *
     * @return a CatalogPartition
     */
    public abstract CatalogPartition createPartition();

    protected ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING())),
                Collections.emptyList(),
                null);
    }

    protected ResolvedSchema createAnotherSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING()),
                        Column.physical("second", DataTypes.STRING()),
                        Column.physical("third", DataTypes.STRING())),
                Collections.emptyList(),
                null);
    }

    protected List<String> createPartitionKeys() {
        return Arrays.asList("second", "third");
    }

    protected CatalogPartitionSpec createPartitionSpec() {
        return new CatalogPartitionSpec(
                new HashMap<String, String>() {
                    {
                        put("third", "2000");
                        put("second", "bob");
                    }
                });
    }

    protected CatalogPartitionSpec createAnotherPartitionSpec() {
        return new CatalogPartitionSpec(
                new HashMap<String, String>() {
                    {
                        put("third", "2010");
                        put("second", "bob");
                    }
                });
    }

    protected CatalogPartitionSpec createPartitionSpecSubset() {
        return new CatalogPartitionSpec(
                new HashMap<String, String>() {
                    {
                        put("second", "bob");
                    }
                });
    }

    protected CatalogPartitionSpec createAnotherPartitionSpecSubset() {
        return new CatalogPartitionSpec(
                new HashMap<String, String>() {
                    {
                        put("third", "2000");
                    }
                });
    }

    protected CatalogPartitionSpec createInvalidPartitionSpecSubset() {
        return new CatalogPartitionSpec(
                new HashMap<String, String>() {
                    {
                        put("third", "2010");
                    }
                });
    }

    /** Test table used to assert on a different table. */
    public static class TestView implements ResolvedCatalogBaseTable<CatalogView> {

        @Override
        public TableKind getTableKind() {
            return TableKind.VIEW;
        }

        @Override
        public Map<String, String> getOptions() {
            return null;
        }

        @Override
        public CatalogView getOrigin() {
            return null;
        }

        @Override
        public ResolvedSchema getResolvedSchema() {
            return null;
        }

        @Override
        public TableSchema getSchema() {
            return TableSchema.builder().build();
        }

        @Override
        public String getComment() {
            return null;
        }

        @Override
        public CatalogBaseTable copy() {
            return null;
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.empty();
        }
    }

    // ------ equality check utils ------
    // Can be overridden by sub test class

    protected void checkEquals(CatalogFunction f1, CatalogFunction f2) {
        assertThat(f2.getClassName()).isEqualTo(f1.getClassName());
        assertThat(f2.isGeneric()).isEqualTo(f1.isGeneric());
        assertThat(f2.getFunctionLanguage()).isEqualTo(f1.getFunctionLanguage());
    }

    protected void checkEquals(CatalogColumnStatistics cs1, CatalogColumnStatistics cs2) {
        CatalogTestUtil.checkEquals(cs1, cs2);
    }
}
