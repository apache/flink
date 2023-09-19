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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for no default catalog and/or database. */
public class UnknownCatalogTest {

    public static final String BUILTIN_CATALOG = "cat";
    private static final String BUILTIN_DATABASE = "db";
    public static final EnvironmentSettings ENVIRONMENT_SETTINGS =
            EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .withBuiltInCatalogName(BUILTIN_CATALOG)
                    .withBuiltInDatabaseName(BUILTIN_DATABASE)
                    .build();
    public static final ResolvedSchema EXPECTED_SCHEMA =
            ResolvedSchema.of(Column.physical("i", INT()), Column.physical("s", STRING()));

    public static final ResolvedSchema CURRENT_TIMESTAMP_EXPECTED_SCHEMA =
            ResolvedSchema.of(Column.physical("CURRENT_TIMESTAMP", TIMESTAMP_LTZ(3).notNull()));

    @Test
    public void testUnsetCatalogWithSelectCurrentTimestamp() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(null);
        Table table = tEnv.sqlQuery("SELECT CURRENT_TIMESTAMP");

        assertThat(table.getResolvedSchema()).isEqualTo(CURRENT_TIMESTAMP_EXPECTED_SCHEMA);
    }

    @Test
    public void testSetCatalogUnsetDatabaseWithSelectCurrentTimestamp() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(BUILTIN_CATALOG);
        tEnv.useDatabase(null);
        Table table = tEnv.sqlQuery("SELECT CURRENT_TIMESTAMP");

        assertThat(table.getResolvedSchema()).isEqualTo(CURRENT_TIMESTAMP_EXPECTED_SCHEMA);
    }

    @Test
    public void testSetCatalogWithSelectCurrentTimestamp() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(BUILTIN_CATALOG);
        Table table = tEnv.sqlQuery("SELECT CURRENT_TIMESTAMP");

        assertThat(table.getResolvedSchema()).isEqualTo(CURRENT_TIMESTAMP_EXPECTED_SCHEMA);
    }

    @Test
    public void testUnsetCatalogWithShowFunctions() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(null);

        TableResult table = tEnv.executeSql("SHOW FUNCTIONS");
        final List<Row> functions = CollectionUtil.iteratorToList(table.collect());

        // check it has some built-in functions
        assertThat(functions).hasSizeGreaterThan(0);
    }

    @Test
    public void testUnsetCatalogWithFullyQualified() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(null);
        final String tablePath = String.format("%s.%s.%s", BUILTIN_CATALOG, BUILTIN_DATABASE, "tb");
        registerTable(tEnv, tablePath);

        Table table = tEnv.sqlQuery(String.format("SELECT * FROM %s", tablePath));

        assertThat(table.getResolvedSchema()).isEqualTo(EXPECTED_SCHEMA);
    }

    @Test
    public void testUnsetCatalogWithSingleIdentifier() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(null);
        final String tableName = "tb";
        final String tablePath =
                String.format("%s.%s.%s", BUILTIN_CATALOG, BUILTIN_DATABASE, tableName);
        registerTable(tEnv, tablePath);

        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT * FROM " + tableName))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(String.format("Object '%s' not found", tableName));
    }

    @Test
    public void testUsingUnknownDatabaseWithDatabaseQualified() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);
        tEnv.useDatabase(null);

        final String tableName = "tb";
        final String tablePath =
                String.format("%s.%s.%s", BUILTIN_CATALOG, BUILTIN_DATABASE, tableName);
        registerTable(tEnv, tablePath);

        Table table =
                tEnv.sqlQuery(String.format("SELECT * FROM %s.%s", BUILTIN_DATABASE, tableName));

        assertThat(table.getResolvedSchema()).isEqualTo(EXPECTED_SCHEMA);
    }

    @Test
    public void testUsingUnknownDatabaseWithSingleIdentifier() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);
        tEnv.useDatabase(null);

        final String tableName = "tb";
        final String tablePath =
                String.format("%s.%s.%s", BUILTIN_CATALOG, BUILTIN_DATABASE, tableName);
        registerTable(tEnv, tablePath);

        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT * FROM " + tableName))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(String.format("Object '%s' not found", tableName));
    }

    @Test
    public void testUnsetCatalogWithAlterTable() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useCatalog(null);
        final String tableName = "tb";
        final String tablePath =
                String.format("%s.%s.%s", BUILTIN_CATALOG, BUILTIN_DATABASE, tableName);
        registerTable(tEnv, tablePath);

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format("ALTER TABLE %s ADD (f STRING)", tableName)))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A current catalog has not been set. Please use a fully qualified"
                                + " identifier (such as 'my_catalog.my_database.my_table') or set a"
                                + " current catalog using 'USE CATALOG my_catalog'.");
    }

    @Test
    public void testUnsetDatabaseWithAlterTable() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        tEnv.useDatabase(null);
        final String tableName = "tb";
        final String tablePath =
                String.format("%s.%s.%s", BUILTIN_CATALOG, BUILTIN_DATABASE, tableName);
        registerTable(tEnv, tablePath);

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format("ALTER TABLE %s ADD (f STRING)", tableName)))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A current database has not been set. Please use a fully qualified"
                                + " identifier (such as 'my_database.my_table' or"
                                + " 'my_catalog.my_database.my_table') or set a current database"
                                + " using 'USE my_database'.");
    }

    @Test
    public void testUnsetDatabaseComingFromCatalogWithAlterTable() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(ENVIRONMENT_SETTINGS);

        final String catalogName = "custom";
        final NullDefaultDatabaseCatalog catalog = new NullDefaultDatabaseCatalog(catalogName);
        catalog.createDatabase(
                BUILTIN_DATABASE, new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);
        final String tableName = "tb";
        final String tablePath =
                String.format("%s.%s.%s", catalogName, BUILTIN_DATABASE, tableName);
        registerTable(tEnv, tablePath);

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format("ALTER TABLE %s ADD (f STRING)", tableName)))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A current database has not been set. Please use a fully qualified"
                                + " identifier (such as 'my_database.my_table' or"
                                + " 'my_catalog.my_database.my_table') or set a current database"
                                + " using 'USE my_database'.");
    }

    private static void registerTable(TableEnvironment tEnv, String tableName) {
        final String input1DataId =
                TestValuesTableFactory.registerData(Arrays.asList(Row.of(1, "a"), Row.of(2, "b")));
        tEnv.createTable(
                tableName,
                TableDescriptor.forConnector("values")
                        .option("data-id", input1DataId)
                        .schema(Schema.newBuilder().fromResolvedSchema(EXPECTED_SCHEMA).build())
                        .build());
    }

    private static class NullDefaultDatabaseCatalog extends GenericInMemoryCatalog {

        public NullDefaultDatabaseCatalog(String name) {
            super(name);
        }

        @Override
        public String getDefaultDatabase() {
            return null;
        }
    }
}
