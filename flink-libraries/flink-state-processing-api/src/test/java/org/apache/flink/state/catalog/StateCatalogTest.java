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

package org.apache.flink.state.catalog;

import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit and functional tests for {@link StateCatalog} that are specific to the catalog layer
 * (CatalogView/CatalogTable semantics, unsupported write operations). Directory scanning, db-name
 * derivation, and dynamic re-discovery are {@link StateCatalog}'s delegation to {@link
 * SnapshotDiscovery} and are covered exhaustively by {@link SnapshotDiscoveryTest} instead of being
 * re-verified here.
 */
class StateCatalogTest {

    // Every metadata file created by createMetadataFile() gets this exact modification time, so
    // the creationTs segment in derived database names is deterministic across all tests.
    private static final Instant FIXED_TS = Instant.parse("2024-03-15T10:30:45Z");
    private static final String TS = "2024-03-15T10:30:45Z";
    private static final String METADATA_TABLE = StateCatalog.METADATA_TABLE;

    @TempDir Path tempDir;

    @Test
    void testCatalogOperations() throws Exception {
        createMetadataFile(tempDir.resolve("savepoint-abc"));
        StateCatalog catalog = openCatalog("app1", tempDir);
        String dbName = "app1/" + TS + "/savepoint-abc";

        // databaseExists
        assertThat(catalog.databaseExists(dbName)).isTrue();
        assertThat(catalog.databaseExists("app1/" + TS + "/savepoint-nonexistent")).isFalse();
        assertThat(catalog.databaseExists("unknown/" + TS + "/savepoint-abc")).isFalse();

        // tableExists
        assertThat(catalog.tableExists(new ObjectPath(dbName, METADATA_TABLE))).isTrue();
        assertThat(catalog.tableExists(new ObjectPath(dbName, "nonexistent"))).isFalse();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath("app1/" + TS + "/nonexistent", METADATA_TABLE)))
                .isFalse();

        // getTable returns CatalogView with correct query
        Path savepointDir = tempDir.resolve("savepoint-abc");
        CatalogView view = (CatalogView) catalog.getTable(new ObjectPath(dbName, METADATA_TABLE));
        assertThat(view.getOriginalQuery())
                .contains("savepoint_metadata")
                .contains(savepointDir.toAbsolutePath().toString());

        // getDatabase throws for unknown
        assertThatThrownBy(() -> catalog.getDatabase("app1/" + TS + "/savepoint-nonexistent"))
                .isInstanceOf(DatabaseNotExistException.class);

        // getTable throws for unknown snapshot
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                "app1/" + TS + "/savepoint-nonexistent",
                                                METADATA_TABLE)))
                .isInstanceOf(TableNotExistException.class);

        catalog.close();
    }

    @Test
    void testListFunctionsAlwaysReturnsEmpty() throws Exception {
        StateCatalog catalog = openCatalog("app1", tempDir);
        assertThat(catalog.listFunctions("nonexistent")).isEmpty();
        catalog.close();
    }

    @Test
    void testWriteOperationsThrow() throws Exception {
        StateCatalog catalog = openCatalog("app1", tempDir);

        assertThatThrownBy(
                        () ->
                                catalog.createDatabase(
                                        "db",
                                        new CatalogDatabaseImpl(Collections.emptyMap(), ""),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.dropDatabase("db", true, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.createTable(new ObjectPath("db", "t"), null, false))
                .isInstanceOf(UnsupportedOperationException.class);

        catalog.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void createMetadataFile(Path snapshotDir) throws IOException {
        Files.createDirectories(snapshotDir);
        Path file = Files.createFile(snapshotDir.resolve("_metadata"));
        Files.setLastModifiedTime(file, FileTime.from(FIXED_TS));
    }

    private static StateCatalog openCatalog(String label, Path directory) throws Exception {
        StateCatalog catalog =
                new StateCatalog(
                        "state",
                        Collections.singletonMap(label, directory.toAbsolutePath().toString()));
        catalog.open();
        return catalog;
    }
}
