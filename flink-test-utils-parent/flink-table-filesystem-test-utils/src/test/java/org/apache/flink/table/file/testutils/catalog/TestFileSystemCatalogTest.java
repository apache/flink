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

package org.apache.flink.table.file.testutils.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TestSchemaResolver;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.refresh.RefreshHandler;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PATH;
import static org.apache.flink.table.file.testutils.catalog.TestFileSystemCatalog.DATA_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link TestFileSystemCatalog}. */
public class TestFileSystemCatalogTest extends TestFileSystemCatalogTestBase {

    private static final List<Column> CREATE_COLUMNS =
            Arrays.asList(
                    Column.physical("id", DataTypes.BIGINT()),
                    Column.physical("name", DataTypes.VARCHAR(20)),
                    Column.physical("age", DataTypes.INT()),
                    Column.physical("tss", DataTypes.TIMESTAMP(3)),
                    Column.physical("partition", DataTypes.VARCHAR(10)));
    private static final UniqueConstraint CONSTRAINTS =
            UniqueConstraint.primaryKey("primary_constraint", Collections.singletonList("id"));
    private static final List<String> PARTITION_KEYS = Collections.singletonList("partition");

    private static final ResolvedSchema CREATE_RESOLVED_SCHEMA =
            new ResolvedSchema(CREATE_COLUMNS, Collections.emptyList(), CONSTRAINTS);

    private static final Schema CREATE_SCHEMA =
            Schema.newBuilder().fromResolvedSchema(CREATE_RESOLVED_SCHEMA).build();

    private static final Map<String, String> EXPECTED_OPTIONS = new HashMap<>();

    static {
        EXPECTED_OPTIONS.put("source.monitor-interval", "5S");
        EXPECTED_OPTIONS.put("auto-compaction", "true");
    }

    private static final ResolvedCatalogTable EXPECTED_CATALOG_TABLE =
            new ResolvedCatalogTable(
                    CatalogTable.newBuilder()
                            .schema(CREATE_SCHEMA)
                            .comment("test table")
                            .partitionKeys(PARTITION_KEYS)
                            .options(EXPECTED_OPTIONS)
                            .build(),
                    CREATE_RESOLVED_SCHEMA);

    private static final String DEFINITION_QUERY = "SELECT id, region, county FROM T";
    private static final IntervalFreshness FRESHNESS = IntervalFreshness.ofMinute("3");
    private static final ResolvedCatalogMaterializedTable EXPECTED_CATALOG_MATERIALIZED_TABLE =
            new ResolvedCatalogMaterializedTable(
                    CatalogMaterializedTable.newBuilder()
                            .schema(CREATE_SCHEMA)
                            .comment("test materialized table")
                            .partitionKeys(PARTITION_KEYS)
                            .options(EXPECTED_OPTIONS)
                            .definitionQuery(DEFINITION_QUERY)
                            .freshness(FRESHNESS)
                            .logicalRefreshMode(
                                    CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC)
                            .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                            .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                            .build(),
                    CREATE_RESOLVED_SCHEMA);

    private static final TestRefreshHandler REFRESH_HANDLER =
            new TestRefreshHandler("jobID: xxx, clusterId: yyy");

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertThat(actual.contains(TEST_DEFAULT_DATABASE)).isTrue();
        assertThat(actual.contains(NONE_EXIST_DATABASE)).isFalse();
    }

    @Test
    public void testDatabaseExists() {
        assertThat(catalog.databaseExists(TEST_DEFAULT_DATABASE)).isTrue();
        assertThat(catalog.databaseExists(NONE_EXIST_DATABASE)).isFalse();
    }

    @Test
    public void testCreateAndDropDatabase() throws Exception {
        CatalogDatabase expected = new CatalogDatabaseImpl(Collections.emptyMap(), null);
        catalog.createDatabase("db1", expected, true);

        CatalogDatabase actual = catalog.getDatabase("db1");
        assertThat(catalog.listDatabases().contains("db1")).isTrue();
        assertThat(actual.getProperties()).isEqualTo(expected.getProperties());

        // create exist database
        assertThrows(
                DatabaseAlreadyExistException.class,
                () -> catalog.createDatabase("db1", expected, false));

        // drop exist database
        catalog.dropDatabase("db1", true);
        assertThat(catalog.listDatabases().contains("db1")).isFalse();

        // drop non-exist database
        assertThrows(
                DatabaseNotExistException.class,
                () -> catalog.dropDatabase(NONE_EXIST_DATABASE, false));
    }

    @Test
    public void testCreateDatabaseWithOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("k1", "v1");
        options.put("k2", "v2");

        assertThrows(
                CatalogException.class,
                () -> catalog.createDatabase("db1", new CatalogDatabaseImpl(options, null), true));
    }

    @Test
    public void testCreateAndGetCatalogTable() throws Exception {
        ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
        // test create table
        catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

        // test table exist
        assertThat(catalog.tableExists(tablePath)).isTrue();

        Map<String, String> expectedOptions = new HashMap<>(EXPECTED_OPTIONS);
        expectedOptions.put(
                PATH.key(),
                String.format(
                        "%s/%s/%s/%s",
                        tempFile.getAbsolutePath(),
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName(),
                        DATA_PATH));

        // test get table
        CatalogBaseTable actualTable = catalog.getTable(tablePath);
        // validate table type
        assertThat(actualTable.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.TABLE);

        CatalogTable actualCatalogTable = (CatalogTable) actualTable;
        // validate schema
        assertThat(actualCatalogTable.getUnresolvedSchema().resolve(new TestSchemaResolver()))
                .isEqualTo(CREATE_RESOLVED_SCHEMA);
        // validate partition key
        assertThat(actualCatalogTable.getPartitionKeys()).isEqualTo(PARTITION_KEYS);
        // validate options
        assertThat(actualCatalogTable.getOptions()).isEqualTo(expectedOptions);

        // test create exist table
        assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, false));
    }

    @Test
    public void testCreateAndGetCatalogMaterializedTable() throws Exception {
        ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb2");
        // test create materialized table
        catalog.createTable(tablePath, EXPECTED_CATALOG_MATERIALIZED_TABLE, true);

        // test materialized table exist
        assertThat(catalog.tableExists(tablePath)).isTrue();

        Map<String, String> expectedOptions = new HashMap<>(EXPECTED_OPTIONS);
        expectedOptions.put(
                PATH.key(),
                String.format(
                        "%s/%s/%s/%s",
                        tempFile.getAbsolutePath(),
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName(),
                        DATA_PATH));

        // test get materialized table
        CatalogBaseTable actualTable = catalog.getTable(tablePath);
        // validate table type
        assertThat(actualTable.getTableKind())
                .isEqualTo(CatalogBaseTable.TableKind.MATERIALIZED_TABLE);

        CatalogMaterializedTable actualMaterializedTable = (CatalogMaterializedTable) actualTable;
        // validate schema
        assertThat(actualMaterializedTable.getUnresolvedSchema().resolve(new TestSchemaResolver()))
                .isEqualTo(CREATE_RESOLVED_SCHEMA);
        // validate partition key
        assertThat(actualMaterializedTable.getPartitionKeys()).isEqualTo(PARTITION_KEYS);
        // validate options
        assertThat(actualMaterializedTable.getOptions()).isEqualTo(expectedOptions);
        // validate definition query
        assertThat(actualMaterializedTable.getDefinitionQuery()).isEqualTo(DEFINITION_QUERY);
        // validate freshness
        assertThat(actualMaterializedTable.getDefinitionFreshness()).isEqualTo(FRESHNESS);
        // validate logical refresh mode
        assertThat(actualMaterializedTable.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC);
        // validate refresh mode
        assertThat(actualMaterializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.CONTINUOUS);
        // validate refresh status
        assertThat(actualMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.INITIALIZING);
        // validate refresh handler
        assertThat(actualMaterializedTable.getRefreshHandlerDescription())
                .isEqualTo(Optional.empty());
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNull();

        // test create exist materialized table
        assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(tablePath, EXPECTED_CATALOG_MATERIALIZED_TABLE, false));
    }

    @Test
    public void testCreateAndGetGenericTable() throws Exception {
        ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
        // test create datagen table
        Map<String, String> options = new HashMap<>();
        options.put("connector", "datagen");
        options.put("number-of-rows", "10");
        ResolvedCatalogTable datagenResolvedTable =
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(CREATE_SCHEMA)
                                .comment("test generic table")
                                .options(options)
                                .build(),
                        CREATE_RESOLVED_SCHEMA);

        catalog.createTable(tablePath, datagenResolvedTable, true);

        // test table exist
        assertThat(catalog.tableExists(tablePath)).isTrue();

        // test get table
        CatalogBaseTable actualTable = catalog.getTable(tablePath);

        // validate table type
        assertThat(actualTable.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.TABLE);
        // validate schema
        assertThat(actualTable.getUnresolvedSchema().resolve(new TestSchemaResolver()))
                .isEqualTo(CREATE_RESOLVED_SCHEMA);
        // validate options
        assertThat(actualTable.getOptions()).isEqualTo(options);

        // test create exist table
        assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(tablePath, datagenResolvedTable, false));
    }

    @Test
    public void testListTable() throws Exception {
        ObjectPath tablePath1 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
        ObjectPath tablePath2 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb2");

        // create table
        catalog.createTable(tablePath1, EXPECTED_CATALOG_TABLE, true);
        catalog.createTable(tablePath2, EXPECTED_CATALOG_MATERIALIZED_TABLE, true);

        // test list table
        List<String> tables = catalog.listTables(TEST_DEFAULT_DATABASE);
        assertThat(tables.contains(tablePath1.getObjectName())).isTrue();
        assertThat(tables.contains(tablePath2.getObjectName())).isTrue();

        // test list non-exist database table
        assertThrows(
                DatabaseNotExistException.class, () -> catalog.listTables(NONE_EXIST_DATABASE));
    }

    @Test
    public void testAlterCatalogTable() throws Exception {
        ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
        // test create table
        catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

        // test table exist
        assertThat(catalog.tableExists(tablePath)).isTrue();

        // alter table options
        Map<String, String> options = new HashMap<>();
        options.put("auto-compaction", "true");
        options.put("sink.parallelism", "5");
        ResolvedCatalogTable updatedResolvedTable = EXPECTED_CATALOG_TABLE.copy(options);
        catalog.alterTable(tablePath, updatedResolvedTable, Collections.emptyList(), false);

        // test get table
        CatalogBaseTable actualTable = catalog.getTable(tablePath);
        // validate table type
        assertThat(actualTable.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.TABLE);

        // validate options
        Map<String, String> expectedOptions = new HashMap<>(options);
        expectedOptions.put(
                PATH.key(),
                String.format(
                        "%s/%s/%s/%s",
                        tempFile.getAbsolutePath(),
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName(),
                        DATA_PATH));
        assertThat(actualTable.getOptions()).isEqualTo(expectedOptions);
    }

    @Test
    public void testAlterCatalogMaterializedTable() throws Exception {
        ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb2");
        // test create materialized table
        catalog.createTable(tablePath, EXPECTED_CATALOG_MATERIALIZED_TABLE, true);

        // test materialized table exist
        assertThat(catalog.tableExists(tablePath)).isTrue();

        // alter materialized table refresh handler
        ResolvedCatalogMaterializedTable updatedMaterializedTable =
                EXPECTED_CATALOG_MATERIALIZED_TABLE.copy(
                        CatalogMaterializedTable.RefreshStatus.ACTIVATED,
                        REFRESH_HANDLER.asSummaryString(),
                        REFRESH_HANDLER.toBytes());
        catalog.alterTable(tablePath, updatedMaterializedTable, Collections.emptyList(), false);

        // test get materialized table
        CatalogBaseTable actualTable = catalog.getTable(tablePath);
        // validate table type
        assertThat(actualTable.getTableKind())
                .isEqualTo(CatalogBaseTable.TableKind.MATERIALIZED_TABLE);

        CatalogMaterializedTable actualMaterializedTable = (CatalogMaterializedTable) actualTable;
        // validate refresh status
        assertThat(actualMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);
        // validate refresh handler
        assertThat(actualMaterializedTable.getRefreshHandlerDescription().get())
                .isEqualTo(REFRESH_HANDLER.asSummaryString());
        assertThat(actualMaterializedTable.getSerializedRefreshHandler())
                .isEqualTo(REFRESH_HANDLER.toBytes());
    }

    @Test
    public void testDropTable() throws Exception {
        ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
        // create table
        catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

        // test drop table
        catalog.dropTable(tablePath, true);
        assertThat(catalog.tableExists(tablePath)).isFalse();

        // drop non-exist table
        assertThrows(
                TableNotExistException.class,
                () -> catalog.dropTable(new ObjectPath(TEST_DEFAULT_DATABASE, "non_exist"), false));
    }

    private static class TestRefreshHandler implements RefreshHandler {

        private final String handlerString;

        public TestRefreshHandler(String handlerString) {
            this.handlerString = handlerString;
        }

        @Override
        public String asSummaryString() {
            return "test refresh handler";
        }

        public byte[] toBytes() {
            return handlerString.getBytes();
        }
    }
}
