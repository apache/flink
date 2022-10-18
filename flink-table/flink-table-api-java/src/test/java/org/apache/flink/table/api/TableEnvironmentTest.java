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

package org.apache.flink.table.api;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.factories.TestManagedTableFactory.ENRICHED_KEY;
import static org.apache.flink.table.factories.TestManagedTableFactory.ENRICHED_VALUE;
import static org.apache.flink.table.factories.TestManagedTableFactory.MANAGED_TABLES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/** Tests for {@link TableEnvironment}. */
class TableEnvironmentTest {

    @Test
    void testCreateTemporaryTableFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        tEnv.createTemporaryTable(
                "T",
                TableDescriptor.forConnector("fake").schema(schema).option("a", "Test").build());

        assertThat(
                        tEnv.getCatalog(catalog)
                                .orElseThrow(AssertionError::new)
                                .tableExists(new ObjectPath(database, "T")))
                .isFalse();

        final Optional<ContextResolvedTable> lookupResult =
                tEnv.getCatalogManager().getTable(ObjectIdentifier.of(catalog, database, "T"));
        assertThat(lookupResult.isPresent()).isTrue();

        final CatalogBaseTable catalogTable = lookupResult.get().getResolvedTable();
        assertThat(catalogTable instanceof CatalogTable).isTrue();
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions().get("connector")).isEqualTo("fake");
        assertThat(catalogTable.getOptions().get("a")).isEqualTo("Test");
    }

    @Test
    void testCreateTableFromDescriptor() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        tEnv.createTable(
                "T",
                TableDescriptor.forConnector("fake").schema(schema).option("a", "Test").build());

        final ObjectPath objectPath = new ObjectPath(database, "T");
        assertThat(
                        tEnv.getCatalog(catalog)
                                .orElseThrow(AssertionError::new)
                                .tableExists(objectPath))
                .isTrue();

        final CatalogBaseTable catalogTable =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getTable(objectPath);
        assertThat(catalogTable).isInstanceOf(CatalogTable.class);
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions())
                .contains(entry("connector", "fake"), entry("a", "Test"));
    }

    @Test
    void testTableFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("fake").schema(schema).build();

        final Table table = tEnv.from(descriptor);

        assertThat(Schema.newBuilder().fromResolvedSchema(table.getResolvedSchema()).build())
                .isEqualTo(schema);

        assertThat(table.getQueryOperation())
                .asInstanceOf(type(SourceQueryOperation.class))
                .extracting(SourceQueryOperation::getContextResolvedTable)
                .satisfies(
                        crs -> {
                            assertThat(crs.isAnonymous()).isTrue();
                            assertThat(crs.getIdentifier().toList()).hasSize(1);
                            assertThat(crs.getResolvedTable().getOptions())
                                    .containsEntry("connector", "fake");
                        });

        assertThat(tEnv.getCatalogManager().listTables()).isEmpty();
    }

    @Test
    void testManagedTable() {
        innerTestManagedTableFromDescriptor(false, false);
    }

    @Test
    void testManagedTableWithIgnoreExists() {
        innerTestManagedTableFromDescriptor(true, false);
    }

    @Test
    void testTemporaryManagedTableWithIgnoreExists() {
        innerTestManagedTableFromDescriptor(true, true);
    }

    @Test
    void testTemporaryManagedTable() {
        innerTestManagedTableFromDescriptor(true, true);
    }

    private void innerTestManagedTableFromDescriptor(boolean ignoreIfExists, boolean isTemporary) {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        final String tableName = UUID.randomUUID().toString();
        ObjectIdentifier identifier = ObjectIdentifier.of(catalog, database, tableName);

        // create table
        MANAGED_TABLES.put(identifier, new AtomicReference<>());
        CreateTableOperation createOperation =
                new CreateTableOperation(
                        identifier,
                        TableDescriptor.forManaged()
                                .schema(schema)
                                .option("a", "Test")
                                .build()
                                .toCatalogTable(),
                        ignoreIfExists,
                        isTemporary);

        tEnv.executeInternal(createOperation);

        // test ignore: create again
        if (ignoreIfExists) {
            tEnv.executeInternal(createOperation);
        } else {
            assertThatThrownBy(
                    () -> tEnv.executeInternal(createOperation),
                    isTemporary ? "already exists" : "Could not execute CreateTable");
        }

        // lookup table

        boolean isInCatalog =
                tEnv.getCatalog(catalog)
                        .orElseThrow(AssertionError::new)
                        .tableExists(new ObjectPath(database, tableName));
        if (isTemporary) {
            assertThat(isInCatalog).isFalse();
        } else {
            assertThat(isInCatalog).isTrue();
        }

        final Optional<ContextResolvedTable> lookupResult =
                tEnv.getCatalogManager().getTable(identifier);
        assertThat(lookupResult.isPresent()).isTrue();

        final CatalogBaseTable catalogTable = lookupResult.get().getResolvedTable();
        assertThat(catalogTable instanceof CatalogTable).isTrue();
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions().get("a")).isEqualTo("Test");
        assertThat(catalogTable.getOptions().get(ENRICHED_KEY)).isEqualTo(ENRICHED_VALUE);

        AtomicReference<Map<String, String>> reference = MANAGED_TABLES.get(identifier);
        assertThat(reference.get()).isNotNull();
        assertThat(reference.get().get("a")).isEqualTo("Test");
        assertThat(reference.get().get(ENRICHED_KEY)).isEqualTo(ENRICHED_VALUE);

        DropTableOperation dropOperation =
                new DropTableOperation(identifier, ignoreIfExists, isTemporary);
        tEnv.executeInternal(dropOperation);
        assertThat(MANAGED_TABLES.get(identifier).get()).isNull();

        // test ignore: drop again
        if (ignoreIfExists) {
            tEnv.executeInternal(dropOperation);
        } else {
            assertThatThrownBy(() -> tEnv.executeInternal(dropOperation), "does not exist");
        }
        MANAGED_TABLES.remove(identifier);
    }
}
