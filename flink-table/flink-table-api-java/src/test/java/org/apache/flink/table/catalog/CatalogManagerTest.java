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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.listener.AlterDatabaseEvent;
import org.apache.flink.table.catalog.listener.AlterTableEvent;
import org.apache.flink.table.catalog.listener.CatalogModificationEvent;
import org.apache.flink.table.catalog.listener.CatalogModificationListener;
import org.apache.flink.table.catalog.listener.CreateDatabaseEvent;
import org.apache.flink.table.catalog.listener.CreateTableEvent;
import org.apache.flink.table.catalog.listener.DropDatabaseEvent;
import org.apache.flink.table.catalog.listener.DropTableEvent;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CatalogManager}. */
class CatalogManagerTest {
    @Test
    void testDatabaseModificationEvent() throws Exception {
        CompletableFuture<CreateDatabaseEvent> createFuture = new CompletableFuture<>();
        CompletableFuture<AlterDatabaseEvent> alterFuture = new CompletableFuture<>();
        CompletableFuture<DropDatabaseEvent> dropFuture = new CompletableFuture<>();
        CatalogManager catalogManager =
                createCatalogManager(
                        new TestingDatabaseModificationListener(
                                createFuture, alterFuture, dropFuture));

        // Validate create a database
        catalogManager.createDatabase(
                catalogManager.getCurrentCatalog(),
                "database1",
                new CatalogDatabaseImpl(
                        Collections.singletonMap("key1", "val1"), "database1 comment"),
                true);
        CreateDatabaseEvent createDatabaseEvent = createFuture.get(10, TimeUnit.SECONDS);
        assertThat(createDatabaseEvent.context().getCatalogName())
                .isEqualTo(catalogManager.getCurrentCatalog());
        assertThat(createDatabaseEvent.ignoreIfExists()).isTrue();
        assertThat(createDatabaseEvent.databaseName()).isEqualTo("database1");
        assertThat(createDatabaseEvent.database().getComment()).isEqualTo("database1 comment");
        assertThat(createDatabaseEvent.database().getProperties())
                .isEqualTo(Collections.singletonMap("key1", "val1"));
        assertThat(alterFuture.isDone()).isFalse();
        assertThat(dropFuture.isDone()).isFalse();

        // Validate alter a database
        catalogManager.alterDatabase(
                catalogManager.getCurrentCatalog(),
                "database1",
                new CatalogDatabaseImpl(
                        Collections.singletonMap("key1", "val_val1"), "database1 comment modified"),
                false);
        AlterDatabaseEvent alterDatabaseEvent = alterFuture.get(10, TimeUnit.SECONDS);
        assertThat(alterDatabaseEvent.context().getCatalogName())
                .isEqualTo(catalogManager.getCurrentCatalog());
        assertThat(alterDatabaseEvent.ignoreIfNotExists()).isFalse();
        assertThat(alterDatabaseEvent.databaseName()).isEqualTo("database1");
        assertThatThrownBy(alterDatabaseEvent::database)
                .hasMessage(
                        "There is no database in AlterDatabaseEvent, use database name instead.");
        assertThat(alterDatabaseEvent.newDatabase().getComment())
                .isEqualTo("database1 comment modified");
        assertThat(alterDatabaseEvent.newDatabase().getProperties())
                .isEqualTo(Collections.singletonMap("key1", "val_val1"));

        // Validate drop a database
        catalogManager.dropDatabase(catalogManager.getCurrentCatalog(), "database1", true, true);
        DropDatabaseEvent dropDatabaseEvent = dropFuture.get(10, TimeUnit.SECONDS);
        assertThat(dropDatabaseEvent.context().getCatalogName())
                .isEqualTo(catalogManager.getCurrentCatalog());
        assertThat(dropDatabaseEvent.ignoreIfNotExists()).isTrue();
        assertThat(dropDatabaseEvent.databaseName()).isEqualTo("database1");
        assertThatThrownBy(dropDatabaseEvent::database)
                .hasMessage(
                        "There is no database in DropDatabaseEvent, use database name instead.");
        assertThat(dropDatabaseEvent.cascade()).isTrue();
    }

    @Test
    void testTableModificationListener() throws Exception {
        CompletableFuture<CreateTableEvent> createFuture = new CompletableFuture<>();
        CompletableFuture<CreateTableEvent> createTemporaryFuture = new CompletableFuture<>();
        CompletableFuture<AlterTableEvent> alterFuture = new CompletableFuture<>();
        CompletableFuture<DropTableEvent> dropFuture = new CompletableFuture<>();
        CompletableFuture<DropTableEvent> dropTemporaryFuture = new CompletableFuture<>();
        CatalogManager catalogManager =
                createCatalogManager(
                        new TestingTableModificationListener(
                                createFuture,
                                createTemporaryFuture,
                                alterFuture,
                                dropFuture,
                                dropTemporaryFuture));

        catalogManager.initSchemaResolver(true, ExpressionResolverMocks.dummyResolver());
        // Create a view
        catalogManager.createTable(
                CatalogView.of(Schema.newBuilder().build(), null, "", "", Collections.emptyMap()),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "view1"),
                true);
        assertThat(createFuture.isDone()).isFalse();

        // Create a table
        catalogManager.createTable(
                CatalogTable.of(
                        Schema.newBuilder().build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap()),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "table1"),
                true);
        CreateTableEvent createEvent = createFuture.get(10, TimeUnit.SECONDS);
        assertThat(createEvent.isTemporary()).isFalse();
        assertThat(createEvent.identifier().getObjectName()).isEqualTo("table1");
        assertThat(createEvent.ignoreIfExists()).isTrue();

        // Create a temporary table
        catalogManager.createTemporaryTable(
                CatalogTable.of(
                        Schema.newBuilder().build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap()),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "table2"),
                false);
        CreateTableEvent createTemporaryEvent = createTemporaryFuture.get(10, TimeUnit.SECONDS);
        assertThat(createTemporaryEvent.isTemporary()).isTrue();
        assertThat(createTemporaryEvent.identifier().getObjectName()).isEqualTo("table2");
        assertThat(createTemporaryEvent.ignoreIfExists()).isFalse();

        // Alter a table
        catalogManager.alterTable(
                CatalogTable.of(
                        Schema.newBuilder().build(),
                        "table1 comment",
                        Collections.emptyList(),
                        Collections.emptyMap()),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "table1"),
                false);
        AlterTableEvent alterEvent = alterFuture.get(10, TimeUnit.SECONDS);
        assertThat(alterEvent.isTemporary()).isFalse();
        assertThat(alterEvent.identifier().getObjectName()).isEqualTo("table1");
        assertThat(alterEvent.newTable().getComment()).isEqualTo("table1 comment");
        assertThat(alterEvent.ignoreIfNotExists()).isFalse();

        // Drop a view
        catalogManager.dropView(
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "table1"),
                true);
        assertThat(dropFuture.isDone()).isFalse();

        // Drop a table
        catalogManager.dropTable(
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "table1"),
                true);
        DropTableEvent dropEvent = dropFuture.get(10, TimeUnit.SECONDS);
        assertThat(dropEvent.isTemporary()).isFalse();
        assertThat(dropEvent.ignoreIfNotExists()).isTrue();
        assertThat(dropEvent.identifier().getObjectName()).isEqualTo("table1");

        // Create a temporary view with the same table name `table2`
        catalogManager.createTemporaryTable(
                CatalogView.of(Schema.newBuilder().build(), null, "", "", Collections.emptyMap()),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "view2"),
                false);
        // Drop a temporary view
        catalogManager.dropTemporaryView(
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "view2"),
                true);
        assertThat(dropTemporaryFuture.isDone()).isFalse();

        // Drop a temporary table
        catalogManager.dropTemporaryTable(
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        "table2"),
                false);
        DropTableEvent dropTemporaryEvent = dropTemporaryFuture.get(10, TimeUnit.SECONDS);
        assertThat(dropTemporaryEvent.isTemporary()).isTrue();
        assertThat(dropTemporaryEvent.ignoreIfNotExists()).isFalse();
        assertThat(dropTemporaryEvent.identifier().getObjectName()).isEqualTo("table2");
    }

    @Test
    public void testDropCurrentDatabase() throws Exception {
        CatalogManager catalogManager = createCatalogManager(null);

        catalogManager.createDatabase(
                "default", "dummy", new CatalogDatabaseImpl(new HashMap<>(), null), false);
        catalogManager.setCurrentDatabase("dummy");

        assertThatThrownBy(() -> catalogManager.dropDatabase("default", "dummy", false, false))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Cannot drop a database which is currently in use.");
    }

    private CatalogManager createCatalogManager(@Nullable CatalogModificationListener listener) {
        CatalogManager.Builder builder =
                CatalogManager.newBuilder()
                        .classLoader(CatalogManagerTest.class.getClassLoader())
                        .config(new Configuration())
                        .defaultCatalog("default", new GenericInMemoryCatalog("default"))
                        .catalogStoreHolder(
                                CatalogStoreHolder.newBuilder()
                                        .catalogStore(new GenericInMemoryCatalogStore())
                                        .config(new Configuration())
                                        .classloader(CatalogManagerTest.class.getClassLoader())
                                        .build());

        if (listener != null) {
            builder.catalogModificationListeners(Collections.singletonList(listener));
        }

        return builder.build();
    }

    /** Testing database modification listener. */
    static class TestingDatabaseModificationListener implements CatalogModificationListener {
        private final CompletableFuture<CreateDatabaseEvent> createFuture;
        private final CompletableFuture<AlterDatabaseEvent> alterFuture;
        private final CompletableFuture<DropDatabaseEvent> dropFuture;

        TestingDatabaseModificationListener(
                CompletableFuture<CreateDatabaseEvent> createFuture,
                CompletableFuture<AlterDatabaseEvent> alterFuture,
                CompletableFuture<DropDatabaseEvent> dropFuture) {
            this.createFuture = createFuture;
            this.alterFuture = alterFuture;
            this.dropFuture = dropFuture;
        }

        @Override
        public void onEvent(CatalogModificationEvent event) {
            if (event instanceof CreateDatabaseEvent) {
                createFuture.complete((CreateDatabaseEvent) event);
            } else if (event instanceof AlterDatabaseEvent) {
                alterFuture.complete((AlterDatabaseEvent) event);
            } else if (event instanceof DropDatabaseEvent) {
                dropFuture.complete((DropDatabaseEvent) event);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    /** Testing table modification listener. */
    static class TestingTableModificationListener implements CatalogModificationListener {
        private final CompletableFuture<CreateTableEvent> createFuture;
        private final CompletableFuture<CreateTableEvent> createTemporaryFuture;
        private final CompletableFuture<AlterTableEvent> alterFuture;
        private final CompletableFuture<DropTableEvent> dropFuture;
        private final CompletableFuture<DropTableEvent> dropTemporaryFuture;

        TestingTableModificationListener(
                CompletableFuture<CreateTableEvent> createFuture,
                CompletableFuture<CreateTableEvent> createTemporaryFuture,
                CompletableFuture<AlterTableEvent> alterFuture,
                CompletableFuture<DropTableEvent> dropFuture,
                CompletableFuture<DropTableEvent> dropTemporaryFuture) {
            this.createFuture = createFuture;
            this.createTemporaryFuture = createTemporaryFuture;
            this.alterFuture = alterFuture;
            this.dropFuture = dropFuture;
            this.dropTemporaryFuture = dropTemporaryFuture;
        }

        @Override
        public void onEvent(CatalogModificationEvent event) {
            if (event instanceof CreateTableEvent) {
                if (((CreateTableEvent) event).isTemporary()) {
                    createTemporaryFuture.complete((CreateTableEvent) event);
                } else {
                    createFuture.complete((CreateTableEvent) event);
                }
            } else if (event instanceof AlterTableEvent) {
                alterFuture.complete((AlterTableEvent) event);
            } else if (event instanceof DropTableEvent) {
                if (((DropTableEvent) event).isTemporary()) {
                    dropTemporaryFuture.complete((DropTableEvent) event);
                } else {
                    dropFuture.complete((DropTableEvent) event);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Test
    void testCatalogStore() throws Exception {
        CatalogStore catalogStore = new GenericInMemoryCatalogStore();

        Configuration configuration = new Configuration();
        configuration.setString("type", "generic_in_memory");

        assertThatThrownBy(
                        () ->
                                catalogStore.storeCatalog(
                                        "cat1", CatalogDescriptor.of("cat1", configuration)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");

        CatalogManager catalogManager = CatalogManagerMocks.createCatalogManager(catalogStore);
        catalogStore.storeCatalog("exist_cat", CatalogDescriptor.of("exist_cat", configuration));

        catalogManager.createCatalog("cat1", CatalogDescriptor.of("cat1", configuration));
        catalogManager.createCatalog("cat2", CatalogDescriptor.of("cat2", configuration));
        catalogManager.createCatalog("cat3", CatalogDescriptor.of("cat3", configuration));

        assertTrue(catalogManager.getCatalog("cat1").isPresent());
        assertTrue(catalogManager.getCatalog("cat2").isPresent());
        assertTrue(catalogManager.getCatalog("cat3").isPresent());

        assertTrue(catalogManager.listCatalogs().contains("cat1"));
        assertTrue(catalogManager.listCatalogs().contains("cat2"));
        assertTrue(catalogManager.listCatalogs().contains("cat3"));

        catalogManager.registerCatalog("cat4", new GenericInMemoryCatalog("cat4"));

        assertThatThrownBy(
                        () ->
                                catalogManager.createCatalog(
                                        "cat1", CatalogDescriptor.of("cat1", configuration)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Catalog cat1 already exists in catalog store.");

        assertThatThrownBy(
                        () ->
                                catalogManager.createCatalog(
                                        "cat4", CatalogDescriptor.of("cat4", configuration)))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Catalog cat4 already exists in initialized catalogs.");

        catalogManager.createDatabase(
                "exist_cat",
                "cat_db",
                new CatalogDatabaseImpl(Collections.emptyMap(), "database for exist_cat"),
                false);
        catalogManager.createTable(
                CatalogTable.of(
                        Schema.newBuilder().build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap()),
                ObjectIdentifier.of("exist_cat", "cat_db", "test_table"),
                false);
        assertThat(catalogManager.listSchemas("exist_cat"))
                .isEqualTo(new HashSet<>(Arrays.asList("default", "cat_db")));
        assertThat(catalogManager.listTables("exist_cat", "cat_db"))
                .isEqualTo(Collections.singleton("test_table"));
        catalogManager.setCurrentCatalog("exist_cat");
        assertThat(catalogManager.listSchemas())
                .isEqualTo(
                        new HashSet<>(
                                Arrays.asList(
                                        "cat1",
                                        "cat2",
                                        "cat3",
                                        "cat4",
                                        "default_catalog",
                                        "exist_cat")));
        catalogManager.setCurrentDatabase("cat_db");
        assertThat(catalogManager.listTables()).isEqualTo(Collections.singleton("test_table"));

        catalogManager.unregisterCatalog("cat1", false);
        catalogManager.unregisterCatalog("cat2", false);
        catalogManager.unregisterCatalog("cat3", false);

        assertFalse(catalogManager.listCatalogs().contains("cat1"));
        assertFalse(catalogManager.listCatalogs().contains("cat2"));
        assertFalse(catalogManager.listCatalogs().contains("cat3"));

        catalogManager.close();

        assertThatThrownBy(() -> catalogManager.listCatalogs())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CatalogStore is not opened yet.");
    }
}
