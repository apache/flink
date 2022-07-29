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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** E2E test for {@link MySqlCatalog}. */
@RunWith(Parameterized.class)
public class MySqlCatalogITCase extends MySqlCatalogTestBase {

    private static final List<Row> ALL_TYPES_ROWS =
            Lists.newArrayList(
                    Row.ofKind(
                            RowKind.INSERT,
                            1L,
                            -1L,
                            new BigDecimal(1),
                            null,
                            true,
                            null,
                            "hello",
                            Date.valueOf("2021-08-04").toLocalDate(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            new BigDecimal(-1),
                            new BigDecimal(1),
                            -1.0d,
                            1.0d,
                            "enum2",
                            -9.1f,
                            9.1f,
                            -1,
                            1L,
                            -1,
                            1L,
                            null,
                            "col_longtext",
                            null,
                            -1,
                            1,
                            "col_mediumtext",
                            new BigDecimal(-99),
                            new BigDecimal(99),
                            -1.0d,
                            1.0d,
                            "set_ele1",
                            Short.parseShort("-1"),
                            1,
                            "col_text",
                            Time.valueOf("10:32:34").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            "col_tinytext",
                            Byte.parseByte("-1"),
                            Short.parseShort("1"),
                            null,
                            "col_varchar",
                            Timestamp.valueOf("2021-08-04 01:54:16.463").toLocalDateTime(),
                            Time.valueOf("09:33:43").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16.463").toLocalDateTime(),
                            null),
                    Row.ofKind(
                            RowKind.INSERT,
                            2L,
                            -1L,
                            new BigDecimal(1),
                            null,
                            true,
                            null,
                            "hello",
                            Date.valueOf("2021-08-04").toLocalDate(),
                            Timestamp.valueOf("2021-08-04 01:53:19").toLocalDateTime(),
                            new BigDecimal(-1),
                            new BigDecimal(1),
                            -1.0d,
                            1.0d,
                            "enum2",
                            -9.1f,
                            9.1f,
                            -1,
                            1L,
                            -1,
                            1L,
                            null,
                            "col_longtext",
                            null,
                            -1,
                            1,
                            "col_mediumtext",
                            new BigDecimal(-99),
                            new BigDecimal(99),
                            -1.0d,
                            1.0d,
                            "set_ele1,set_ele12",
                            Short.parseShort("-1"),
                            1,
                            "col_text",
                            Time.valueOf("10:32:34").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:53:19").toLocalDateTime(),
                            "col_tinytext",
                            Byte.parseByte("-1"),
                            Short.parseShort("1"),
                            null,
                            "col_varchar",
                            Timestamp.valueOf("2021-08-04 01:53:19.098").toLocalDateTime(),
                            Time.valueOf("09:33:43").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:53:19.098").toLocalDateTime(),
                            null));

    private final MySqlCatalog catalog;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // Use mysql catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    public MySqlCatalogITCase(String version) {
        catalog = CATALOGS.get(version);
    }

    @Parameterized.Parameters(name = "version = {0}")
    public static String[] params() {
        return DOCKER_IMAGE_NAMES.toArray(new String[0]);
    }

    // ------ databases ------

    @Test
    public void testGetDb_DatabaseNotExistException() throws Exception {
        String databaseNotExist = "nonexistent";
        assertThatThrownBy(() -> catalog.getDatabase(databaseNotExist))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog",
                                        databaseNotExist)));
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertThat(actual).containsExactly(TEST_DB, TEST_DB2);
    }

    @Test
    public void testDbExists() throws Exception {
        String databaseNotExist = "nonexistent";
        assertThat(catalog.databaseExists(databaseNotExist)).isFalse();
        assertThat(catalog.databaseExists(TEST_DB)).isTrue();
    }

    // ------ tables ------

    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        assertThat(actual)
                .isEqualTo(
                        Arrays.asList(
                                TEST_TABLE_ALL_TYPES,
                                TEST_SINK_TABLE_ALL_TYPES,
                                TEST_TABLE_SINK_FROM_GROUPED_BY,
                                TEST_TABLE_PK));
    }

    @Test
    public void testListTables_DatabaseNotExistException() throws DatabaseNotExistException {
        String anyDatabase = "anyDatabase";
        assertThatThrownBy(() -> catalog.listTables(anyDatabase))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog", anyDatabase)));
    }

    @Test
    public void testTableExists() {
        String tableNotExist = "nonexist";
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist))).isFalse();
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, TEST_TABLE_ALL_TYPES))).isTrue();
    }

    @Test
    public void testGetTables_TableNotExistException() throws TableNotExistException {
        String anyTableNotExist = "anyTable";
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath(TEST_DB, anyTableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        TEST_DB, anyTableNotExist)));
    }

    @Test
    public void testGetTables_TableNotExistException_NoDb() throws TableNotExistException {
        String databaseNotExist = "nonexistdb";
        String tableNotExist = "anyTable";
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath(databaseNotExist, tableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        databaseNotExist, tableNotExist)));
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE_ALL_TYPES));
        assertThat(table.getUnresolvedSchema()).isEqualTo(TABLE_SCHEMA);
    }

    @Test
    public void testGetTablePrimaryKey() throws TableNotExistException {
        // test the PK of test.t_user
        Schema tableSchemaTestPK1 =
                Schema.newBuilder()
                        .column("uid", DataTypes.BIGINT().notNull())
                        .column("col_bigint", DataTypes.BIGINT())
                        .primaryKeyNamed("PRIMARY", Collections.singletonList("uid"))
                        .build();
        CatalogBaseTable tablePK1 = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE_PK));
        assertThat(tableSchemaTestPK1.getPrimaryKey().get())
                .isEqualTo(tablePK1.getUnresolvedSchema().getPrimaryKey().get());

        // test the PK of test2.t_user
        Schema tableSchemaTestPK2 =
                Schema.newBuilder()
                        .column("pid", DataTypes.INT().notNull())
                        .column("col_varchar", DataTypes.VARCHAR(255))
                        .primaryKeyNamed("PRIMARY", Collections.singletonList("pid"))
                        .build();
        CatalogBaseTable tablePK2 = catalog.getTable(new ObjectPath(TEST_DB2, TEST_TABLE_PK));
        assertThat(tableSchemaTestPK2.getPrimaryKey().get())
                .isEqualTo(tablePK2.getUnresolvedSchema().getPrimaryKey().get());
    }

    // ------ test select query. ------

    @Test
    public void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select pid from %s", TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(
                        Lists.newArrayList(
                                Row.ofKind(RowKind.INSERT, 1L), Row.ofKind(RowKind.INSERT, 2L)));
    }

    @Test
    public void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());

        assertThat(results).isEqualTo(ALL_TYPES_ROWS);
    }

    @Test
    public void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`",
                                                TEST_DB, TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(ALL_TYPES_ROWS);
    }

    @Test
    public void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s.%s.`%s`",
                                                TEST_CATALOG_NAME,
                                                catalog.getDefaultDatabase(),
                                                TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(ALL_TYPES_ROWS);
    }

    @Test
    public void testSelectToInsert() throws Exception {

        String sql =
                String.format(
                        "insert into `%s` select * from `%s`",
                        TEST_SINK_TABLE_ALL_TYPES, TEST_TABLE_ALL_TYPES);
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_SINK_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(ALL_TYPES_ROWS);
    }

    @Test
    public void testGroupByInsert() throws Exception {
        // Changes primary key for the next record.
        tEnv.executeSql(
                        String.format(
                                "insert into `%s` select max(`pid`) `pid`, `col_bigint` from `%s` "
                                        + "group by `col_bigint` ",
                                TEST_TABLE_SINK_FROM_GROUPED_BY, TEST_TABLE_ALL_TYPES))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                TEST_TABLE_SINK_FROM_GROUPED_BY))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(Lists.newArrayList(Row.ofKind(RowKind.INSERT, 2L, -1L)));
    }
}
