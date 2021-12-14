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
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.api.TestManagedTableFactory.ENRICHED_KEY;
import static org.apache.flink.table.api.TestManagedTableFactory.ENRICHED_VALUE;
import static org.apache.flink.table.api.TestManagedTableFactory.MANAGED_TABLES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link TableEnvironment}. */
public class TableEnvironmentTest {

    @Test
    public void testCreateTemporaryTableFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        tEnv.createTemporaryTable(
                "T",
                TableDescriptor.forConnector("fake").schema(schema).option("a", "Test").build());

        assertFalse(
                tEnv.getCatalog(catalog)
                        .orElseThrow(AssertionError::new)
                        .tableExists(new ObjectPath(database, "T")));

        final Optional<CatalogManager.TableLookupResult> lookupResult =
                tEnv.getCatalogManager().getTable(ObjectIdentifier.of(catalog, database, "T"));
        assertTrue(lookupResult.isPresent());

        final CatalogBaseTable catalogTable = lookupResult.get().getTable();
        assertTrue(catalogTable instanceof CatalogTable);
        assertEquals(schema, catalogTable.getUnresolvedSchema());
        assertEquals("fake", catalogTable.getOptions().get("connector"));
        assertEquals("Test", catalogTable.getOptions().get("a"));
    }

    @Test
    public void testCreateTableFromDescriptor() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        tEnv.createTable(
                "T",
                TableDescriptor.forConnector("fake").schema(schema).option("a", "Test").build());

        final ObjectPath objectPath = new ObjectPath(database, "T");
        assertTrue(
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).tableExists(objectPath));

        final CatalogBaseTable catalogTable =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getTable(objectPath);
        assertTrue(catalogTable instanceof CatalogTable);
        assertEquals(schema, catalogTable.getUnresolvedSchema());
        assertEquals("fake", catalogTable.getOptions().get("connector"));
        assertEquals("Test", catalogTable.getOptions().get("a"));
    }

    @Test
    public void testTableFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        final Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        final TableDescriptor descriptor =
                TableDescriptor.forConnector("fake").schema(schema).build();

        final Table table = tEnv.from(descriptor);

        assertEquals(
                schema, Schema.newBuilder().fromResolvedSchema(table.getResolvedSchema()).build());

        assertTrue(table.getQueryOperation() instanceof CatalogQueryOperation);
        final ObjectIdentifier tableIdentifier =
                ((CatalogQueryOperation) table.getQueryOperation()).getTableIdentifier();

        final Optional<CatalogManager.TableLookupResult> lookupResult =
                tEnv.getCatalogManager().getTable(tableIdentifier);
        assertTrue(lookupResult.isPresent());

        assertEquals("fake", lookupResult.get().getTable().getOptions().get("connector"));
    }

    @Test
    public void testManagedTable() {
        innerTestManagedTableFromDescriptor(false, false);
    }

    @Test
    public void testManagedTableWithIgnoreExists() {
        innerTestManagedTableFromDescriptor(true, false);
    }

    @Test
    public void testTemporaryManagedTableWithIgnoreExists() {
        innerTestManagedTableFromDescriptor(true, true);
    }

    @Test
    public void testTemporaryManagedTable() {
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
            Assertions.assertThrows(
                    Exception.class,
                    () -> tEnv.executeInternal(createOperation),
                    isTemporary ? "already exists" : "Could not execute CreateTable");
        }

        // lookup table

        boolean isInCatalog =
                tEnv.getCatalog(catalog)
                        .orElseThrow(AssertionError::new)
                        .tableExists(new ObjectPath(database, tableName));
        if (isTemporary) {
            assertFalse(isInCatalog);
        } else {
            assertTrue(isInCatalog);
        }

        final Optional<CatalogManager.TableLookupResult> lookupResult =
                tEnv.getCatalogManager().getTable(identifier);
        assertTrue(lookupResult.isPresent());

        final CatalogBaseTable catalogTable = lookupResult.get().getTable();
        assertTrue(catalogTable instanceof CatalogTable);
        assertEquals(schema, catalogTable.getUnresolvedSchema());
        assertEquals(CatalogBaseTable.TableKind.MANAGED, catalogTable.getTableKind());
        assertEquals("Test", catalogTable.getOptions().get("a"));
        assertEquals(ENRICHED_VALUE, catalogTable.getOptions().get(ENRICHED_KEY));

        AtomicReference<Map<String, String>> reference = MANAGED_TABLES.get(identifier);
        assertNotNull(reference.get());
        assertEquals("Test", reference.get().get("a"));
        assertEquals(ENRICHED_VALUE, reference.get().get(ENRICHED_KEY));

        DropTableOperation dropOperation =
                new DropTableOperation(identifier, ignoreIfExists, isTemporary);
        tEnv.executeInternal(dropOperation);
        assertNull(MANAGED_TABLES.get(identifier).get());

        // test ignore: drop again
        if (ignoreIfExists) {
            tEnv.executeInternal(dropOperation);
        } else {
            Assertions.assertThrows(
                    ValidationException.class,
                    () -> tEnv.executeInternal(dropOperation),
                    "does not exist");
        }
        MANAGED_TABLES.remove(identifier);
    }
}
