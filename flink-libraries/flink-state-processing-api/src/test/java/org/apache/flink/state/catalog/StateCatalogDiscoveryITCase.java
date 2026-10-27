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

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.table.module.StateModule;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for basic {@link StateCatalog} functionality driven through {@code CREATE
 * CATALOG} DDL and SQL: multi-label discovery and the {@code metadata} view. Checkpoint metadata is
 * written directly via {@link Checkpoints#storeCheckpointMetadataWithoutExclusiveDir} — no
 * minicluster or real state backend is involved, so these tests are backend-agnostic by
 * construction.
 *
 * <p>For reads of real (generated) keyed-state savepoints, see {@code
 * StateCatalogGeneratedSavepointITCase} (HashMap-only, checked-in fixtures) and {@code
 * KeyedStateReadingITCase} (parameterized across backends, savepoints taken at runtime).
 */
class StateCatalogDiscoveryITCase {

    @Test
    void testMetadataQueryReturnsOperators(@TempDir Path tempDir) throws Exception {
        OperatorID opId1 = new OperatorID(1, 2);
        OperatorState op1 = new OperatorState("source", "source-uid", opId1, 2, 128);
        OperatorID opId2 = new OperatorID(3, 4);
        OperatorState op2 = new OperatorState("sink", null, opId2, 1, 128);

        Path savepointDir = Files.createDirectories(tempDir.resolve("savepoint-test"));
        writeMetadata(savepointDir, 42L, Arrays.asList(op1, op2));

        TableEnvironment tableEnv = newTableEnv();
        createCatalog(tableEnv, "state", directoryOption("app", tempDir));
        tableEnv.executeSql("USE CATALOG state");

        StateCatalog catalog = getCatalog(tableEnv, "state");
        String dbName = catalog.listDatabases().get(0);
        tableEnv.executeSql("USE `" + dbName + "`");

        List<Row> rows = collectWithSql(tableEnv, "SELECT * FROM metadata");

        assertThat(rows).hasSize(2);
        rows.forEach(row -> assertThat(row.getField("checkpoint-id")).isEqualTo(42L));
        assertThat(rows.stream().map(r -> r.getField("operator-name")).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("source", "sink");

        catalog.close();
    }

    @Test
    void testMultipleLabelsDiscovered(@TempDir Path tempDir) throws Exception {
        Path checkpointsDir = Files.createDirectories(tempDir.resolve("checkpoints"));
        Path savepointsDir = Files.createDirectories(tempDir.resolve("savepoints"));
        touchMetadata(checkpointsDir.resolve("savepoint-a"));
        touchMetadata(savepointsDir.resolve("savepoint-b"));

        String directoryOptions =
                directoryOption("ckpts", checkpointsDir)
                        + ", "
                        + directoryOption("svpts", savepointsDir);

        TableEnvironment tableEnv = newTableEnv();
        createCatalog(tableEnv, "with_ts", directoryOptions);
        createCatalog(
                tableEnv, "without_ts", directoryOptions + ", 'db-name.include-ts' = 'false'");

        StateCatalog withTs = getCatalog(tableEnv, "with_ts");
        assertThat(withTs.listDatabases())
                .hasSize(2)
                .allSatisfy(
                        dbName ->
                                assertThat(dbName)
                                        .matches(
                                                "(ckpts|svpts)/\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z/savepoint-[ab]"));
        withTs.close();

        StateCatalog withoutTs = getCatalog(tableEnv, "without_ts");
        assertThat(withoutTs.listDatabases())
                .containsExactlyInAnyOrder("ckpts/savepoint-a", "svpts/savepoint-b");
        withoutTs.close();
    }

    @Test
    void testCatalogOperations(@TempDir Path tempDir) throws Exception {
        Path savepointDir = Files.createDirectories(tempDir.resolve("savepoint-abc"));
        writeMetadata(savepointDir, 7L, Collections.emptyList());

        TableEnvironment tableEnv = newTableEnv();
        createCatalog(tableEnv, "state", directoryOption("app", tempDir));

        StateCatalog catalog = getCatalog(tableEnv, "state");
        List<String> dbs = catalog.listDatabases();
        assertThat(dbs).hasSize(1);
        String dbName = dbs.get(0);
        assertThat(dbName).matches("app/\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z/savepoint-abc");

        assertThat(catalog.databaseExists(dbName)).isTrue();
        assertThat(catalog.databaseExists("app/nonexistent")).isFalse();

        // listTables includes views (the "metadata" view), per the Catalog contract.
        assertThat(catalog.listTables(dbName)).containsExactly(StateCatalog.METADATA_TABLE);
        assertThat(catalog.listViews(dbName)).containsExactly(StateCatalog.METADATA_TABLE);

        assertThat(catalog.tableExists(new ObjectPath(dbName, StateCatalog.METADATA_TABLE)))
                .isTrue();
        assertThat(catalog.tableExists(new ObjectPath(dbName, "other"))).isFalse();

        CatalogView view =
                (CatalogView) catalog.getTable(new ObjectPath(dbName, StateCatalog.METADATA_TABLE));
        assertThat(view.getOriginalQuery())
                .contains("savepoint_metadata")
                .contains(savepointDir.toAbsolutePath().toString());

        // Verify querying the metadata view via SQL works and returns expected rows
        tableEnv.executeSql("USE CATALOG state");
        tableEnv.executeSql("USE `" + dbName + "`");
        assertThat(collectWithSql(tableEnv, "SELECT * FROM metadata")).isEmpty();

        catalog.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static TableEnvironment newTableEnv() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.loadModule("state", StateModule.INSTANCE);
        return env;
    }

    private static void createCatalog(
            TableEnvironment tableEnv, String catalogName, String withOptions) {
        tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = '%s', %s)",
                        catalogName, StateCatalogFactory.IDENTIFIER, withOptions));
    }

    private static String directoryOption(String label, Path dir) {
        return String.format(
                "'directory.%s' = '%s'", label, dir.toAbsolutePath().toString().replace("'", "''"));
    }

    private static StateCatalog getCatalog(TableEnvironment tableEnv, String name) {
        return (StateCatalog) tableEnv.getCatalog(name).get();
    }

    private static void touchMetadata(Path snapshotDir) throws Exception {
        Files.createDirectories(snapshotDir);
        Files.createFile(snapshotDir.resolve("_metadata"));
    }

    private static void writeMetadata(
            Path snapshotDir, long checkpointId, List<OperatorState> operators) throws Exception {
        CheckpointMetadata metadata =
                new CheckpointMetadata(checkpointId, operators, Collections.emptyList());
        try (OutputStream out = Files.newOutputStream(snapshotDir.resolve("_metadata"))) {
            Checkpoints.storeCheckpointMetadataWithoutExclusiveDir(metadata, out);
        }
    }

    private static List<Row> collectWithSql(TableEnvironment tEnv, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> it = result.collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }
}
